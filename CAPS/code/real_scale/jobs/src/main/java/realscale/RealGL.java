package realscale;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.function.Supplier;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogAccumulator;
import io.palyvos.provenance.genealog.GenealogContributionQuery;
import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogGraphClone;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.util.BaseTuple;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import temporalindex.StampDerivedTime;

/**
 * GeneaLog on real data: same dataflow, no channels (the job does not depend on M). Every source record
 * is a GeneaLog SOURCE tuple, the aggregate keeps the pointer chain of its window.
 * Source -> keyBy(key) -> tumbling window -> Agg(count, sum) -> Stamp -> Sink
 *
 * Usage: RealGL dataset inputFile outputFile M [file [sampleDir period] | discard dir]
 * file row: key,timestamp,count,sum,traversal (source tuples reached by the backward traversal)
 * with sampleDir: also the provenance graphs of the sampled outputs, cloned, as the query fixture
 */
public class RealGL {

  public static void main(String[] args) throws Exception {
    final String dataset = args[0];
    final String mode = args.length > 4 ? args[4] : "file";
    String statDir = new File(mode.equals("file") ? args[2] : args[5]).getAbsoluteFile().getParent();
    new File(statDir).mkdirs();
    ExperimentSettings settings = ExperimentSettings.newInstance(new String[] {
        "--statisticsFolder", statDir, "--outputFile", "sink", "--inputFile", "unused",
        "--sourcesNumber", "1", "--sourceRepetitions", "1", "--autoFlush",
        "--provenanceActivator", "GENEALOG", "--aggregateStrategy", "sortedPtr", "--sinkParallelism", "1"});

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setMaxParallelism(settings.maxParallelism());
    env.setParallelism(Integer.parseInt(System.getenv().getOrDefault("CAPS_PARALLELISM", "1")));
    FlinkSerializerActivator.PROVENANCE_OPTIMIZED.activate(env, settings);

    SingleOutputStreamOperator<Tuple> source = env
        .addSource(new Source(dataset, args[1]), TypeInformation.of(Tuple.class)).name("SOURCE")
        .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple>() {
          @Override
          public long extractTimestamp(Tuple t, long prev) {
            return t.getTimestamp();
          }

          @Override
          public Watermark checkAndGetNextWatermark(Tuple t, long ts) {
            return new Watermark(ts - 1);   // see RealData.Assigner
          }
        })
        .setParallelism(1)
        .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
        .returns(Tuple.class)
        .setParallelism(env.getParallelism());

    DataStream<Tuple> aggregated = source
        .keyBy(t -> t.getKey())
        .window(TumblingEventTimeWindows.of(Time.milliseconds(RealData.windowMs(dataset))))
        .aggregate(new CountSumAggregate(settings.aggregateStrategySupplier()))
        .name("AGG")
        .process(new StampDerivedTime<Tuple>((t, ts) -> t.setTimestamp(ts)))
        .returns(Tuple.class)
        .name("STAMP-AGG");

    if (mode.equals("file"))
      aggregated.addSink(new TraversalSink(settings, args[2], args.length > 5 ? args[5] : null,
          args.length > 6 ? Long.parseLong(args[6]) : 0)).setParallelism(1).name("SINK");
    else if (mode.equals("discard")) aggregated.addSink(new DiscardSink(args[5])).setParallelism(1).name("SINK");
    else throw new IllegalArgumentException("unknown mode " + mode);

    JobExecutionResult result = env.execute("RealScale-GeneaLog");
    System.out.println("Job Runtime: " + result.getNetRuntime() + " ms");
  }

  /** A source record (key, value) and the output of the window (key, count, sum). */
  public static class Tuple extends BaseTuple implements GenealogTuple {
    private GenealogData gdata = new GenealogData();
    public long value;
    public long count;
    public long sum;

    public Tuple() {}

    public Tuple(long timestamp, String key, long value) {
      super(timestamp, key, timestamp);
      this.value = value;
    }

    @Override
    public GenealogData getGenealogData() {
      return gdata;
    }

    @Override
    public void initGenealog(GenealogTupleType tupleType) {
      gdata = new GenealogData();
      gdata.init(tupleType);
    }

    @Override
    public long getUID() {
      return gdata.getUID();
    }

    @Override
    public void setUID(long uid) {
      gdata.setUID(uid);
    }

    // Identity, not field equality: the aggregate strategy takes equal tuples for one tuple inserted twice.
    @Override
    public boolean equals(Object o) {
      return this == o;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }

    public String head() {
      return getKey() + "," + getTimestamp() + "," + count + "," + sum;
    }
  }

  public static class Source extends RichSourceFunction<Tuple> {
    private final String dataset;
    private final String path;
    private volatile boolean running = true;

    public Source(String dataset, String path) {
      this.dataset = dataset;
      this.path = path;
    }

    @Override
    public void run(final SourceContext<Tuple> ctx) throws Exception {
      RealData.Emit emit = (ts, key, channel, value) -> {
        Tuple t = new Tuple(ts, key, value);
        t.initGenealog(GenealogTupleType.SOURCE);
        ctx.collect(t);
      };
      try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.InputStreamReader(new java.io.FileInputStream(path), java.nio.charset.StandardCharsets.UTF_8), 1 << 16)) {
        String line;
        while (running && (line = br.readLine()) != null)
          if (!line.isEmpty()) RealData.parse(dataset, line, 1, emit);
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  /** Windowed count and sum; the GeneaLog accumulator keeps the pointer chain of the window. */
  public static class CountSumAggregate implements AggregateFunction<Tuple, CountSumAggregate.Acc, Tuple> {
    private final Supplier<ProvenanceAggregateStrategy> strategySupplier;

    public CountSumAggregate(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
      this.strategySupplier = strategySupplier;
    }

    @Override
    public Acc createAccumulator() {
      return new Acc(strategySupplier);
    }

    @Override
    public Acc add(Tuple value, Acc acc) {
      acc.add(value);
      return acc;
    }

    @Override
    public Tuple getResult(Acc acc) {
      return acc.getAggregatedResult();
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      throw new UnsupportedOperationException("Merge not supported for provenance windows");
    }

    public static class Acc extends GenealogAccumulator<Tuple, Tuple, Acc> {
      private String key;
      private long count;
      private long sum;
      private long maxStimulus;

      public Acc(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
        super(strategySupplier);
      }

      @Override
      protected void doAdd(Tuple t) {
        key = t.getKey();
        count++;
        sum += t.value;
        maxStimulus = Math.max(maxStimulus, t.getStimulus());
      }

      @Override
      protected Tuple doGetAggregatedResult() {
        Tuple out = new Tuple(0, key, 0);   // timestamp: StampDerivedTime sets the window end after the operator
        out.count = count;
        out.sum = sum;
        out.setStimulus(maxStimulus);
        return out;
      }
    }
  }

  /** Timing sink: counts the outputs. No traversal. */
  public static class DiscardSink extends RichSinkFunction<Tuple> {
    private final String dir;
    private transient long outputs;

    public DiscardSink(String dir) {
      this.dir = dir;
    }

    @Override
    public void invoke(Tuple value, Context context) {
      outputs++;
    }

    @Override
    public void close() throws Exception {
      new File(dir).mkdirs();
      try (PrintWriter pw = new PrintWriter(new java.io.FileWriter(dir + "/outputs.txt"))) {
        pw.println(outputs);
      }
    }
  }

  /**
   * Every output with the size of its backward traversal (GenealogContributionQuery.count) as one row; with a
   * sample directory also the outputs whose identity hash is 0 mod period, as a clone of their provenance graph.
   */
  public static class TraversalSink extends RichSinkFunction<Tuple> {
    private final ExperimentSettings settings;
    private final String path;
    private final String sampleDir;
    private final long period;
    private transient ProvenanceAggregateStrategy strategy;
    private transient PrintWriter pw;
    private transient ArrayList<Object> kept;

    public TraversalSink(ExperimentSettings settings, String path, String sampleDir, long period) {
      this.settings = settings;
      this.path = path;
      this.sampleDir = sampleDir;
      this.period = period;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      strategy = settings.aggregateStrategySupplier().get();
      pw = new PrintWriter(new BufferedWriter(new FileWriter(path), 1 << 20));
      kept = new ArrayList<Object>();
    }

    @Override
    public void invoke(Tuple t, Context context) {
      long answer = GenealogContributionQuery.count(t, strategy);
      pw.println(t.head() + "," + answer);
      if (sampleDir != null && RealData.sampled(t.getKey(), t.getTimestamp(), t.count, t.sum, period)) {
        Object clone = GenealogGraphClone.cloneReachable(t, strategy);
        if (GenealogContributionQuery.count((GenealogTuple) clone, strategy) != answer)
          throw new IllegalStateException("cloned traversal differs from the live one");
        kept.add(clone);
      }
    }

    @Override
    public void close() throws Exception {
      if (pw == null) return;
      pw.print("--- OUTPUT END ---");
      pw.close();
      if (sampleDir != null) RealData.writeSample(sampleDir, kept);
    }
  }
}
