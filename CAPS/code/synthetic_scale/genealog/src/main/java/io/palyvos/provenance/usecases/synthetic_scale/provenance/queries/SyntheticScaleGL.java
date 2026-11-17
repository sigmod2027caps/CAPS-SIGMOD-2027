package io.palyvos.provenance.usecases.synthetic_scale.provenance.queries;

import io.palyvos.provenance.genealog.GenealogTraversalSink;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.usecases.synthetic_scale.provenance.SyntheticScaleAggregateFunction;
import io.palyvos.provenance.usecases.synthetic_scale.provenance.SyntheticScaleSourceGL;
import io.palyvos.provenance.usecases.synthetic_scale.provenance.SyntheticScaleTupleGL;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import temporalindex.SampleKeyFn;
import temporalindex.StampDerivedTime;

/**
 * Channel-cardinality dataflow with GeneaLog: the same computation as the
 * noprov, CAPS and Green twins. GeneaLog has no channels, so M only changes the dataflow in the
 * path sweep (M exclusive branches Filter(id % M == i) -> Map, then Union); in the source sweep
 * the job does not depend on M.
 *
 * <pre>
 *   Source -> [M x Filter -> Map]+Union (path sweep) -> keyBy(key) -> TumblingWindow(60 s)
 *          -> Agg(count, sum) -> Stamp(window end) -> Sink
 * </pre>
 *
 * Own options (stripped before the ExperimentSettings see the arguments):
 *   --sweep source|path   --channels M   --cardinalityDiscard dir (timing sink, counts the outputs)
 * Without --cardinalityDiscard the GeneaLog traversal sink is used, which needs --traversalBenchDir
 * and either --traversalDumpFile (every output with the size of its graph: verification) or
 * --traversalQueryIds (the shared 1,000 query ids, writes the query fixture).
 */
public class SyntheticScaleGL {

  // Must match data/synthetic_scale/generate_synthetic_scale.py.
  public static final long WINDOW_MS = 60_000L;

  // Identifies an output across the methods: key, window end, count and sum, no annotation.
  static final SampleKeyFn<SyntheticScaleTupleGL> SAMPLE_KEY = (t, out) -> {
    out.writeInt(t.keyId);
    out.writeLong(t.getTimestamp());
    out.writeLong(t.count);
    out.writeLong(t.sum);
  };

  public static void main(String[] args) throws Exception {
    List<String> forwarded = new ArrayList<String>();
    boolean pathSweep = false;
    int channels = 1;
    String discardDir = null;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--sweep")) {
        String sweep = args[++i];
        if (!sweep.equals("source") && !sweep.equals("path"))
          throw new IllegalArgumentException("--sweep must be source or path: " + sweep);
        pathSweep = sweep.equals("path");
      } else if (args[i].equals("--channels"))
        channels = Integer.parseInt(args[++i]);
      else if (args[i].equals("--cardinalityDiscard"))
        discardDir = args[++i];
      else
        forwarded.add(args[i]);
    }
    if (channels < 1) throw new IllegalArgumentException("--channels must be >= 1");
    ExperimentSettings settings = ExperimentSettings.newInstance(forwarded.toArray(new String[0]));

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setMaxParallelism(settings.maxParallelism());
    // Parallelism of every method comes from CAPS_PARALLELISM when it is set (else from `flink run`).
    String parallelism = System.getenv("CAPS_PARALLELISM");
    if (parallelism != null) env.setParallelism(Integer.parseInt(parallelism));
    FlinkSerializerActivator.PROVENANCE_OPTIMIZED.activate(env, settings);

    SingleOutputStreamOperator<SyntheticScaleTupleGL> sourceStream =
        env.addSource(new SyntheticScaleSourceGL(settings))
            .name("SOURCE")
            .assignTimestampsAndWatermarks(
                new AssignerWithPunctuatedWatermarks<SyntheticScaleTupleGL>() {
                  @Override
                  public long extractTimestamp(SyntheticScaleTupleGL t, long prev) {
                    return t.getTimestamp();
                  }
                  @Override
                  public Watermark checkAndGetNextWatermark(SyntheticScaleTupleGL t, long ts) {
                    return new Watermark(ts);
                  }
                })
            .setParallelism(1)
            .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
            .returns(SyntheticScaleTupleGL.class)
            .setParallelism(env.getParallelism());

    DataStream<SyntheticScaleTupleGL> tagged = sourceStream;
    if (pathSweep) {
      DataStream<SyntheticScaleTupleGL>[] branches = new DataStream[channels];
      for (int i = 0; i < channels; i++)
        branches[i] = sourceStream
            .filter(new ExclusiveIdFilter(i, channels))
            .name("PATH-FILTER-" + i)
            .map(new PathMap())
            .name("PATH-MAP-" + i);
      DataStream<SyntheticScaleTupleGL>[] others = new DataStream[channels - 1];
      System.arraycopy(branches, 1, others, 0, channels - 1);
      tagged = branches[0].union(others);
    }

    DataStream<SyntheticScaleTupleGL> aggregated = tagged
        .keyBy(t -> t.keyId)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_MS)))
        .aggregate(new SyntheticScaleAggregateFunction(settings.aggregateStrategySupplier()))
        .name("AGG")
        .process(new StampDerivedTime<SyntheticScaleTupleGL>((t, ts) -> t.setTimestamp(ts)))
        .returns(SyntheticScaleTupleGL.class)
        .name("STAMP-AGG");

    if (discardDir != null)
      aggregated.addSink(new DiscardSink(discardDir)).setParallelism(1).name("SINK");
    else if (settings.traversalBenchDir() != null)
      aggregated.addSink(new GenealogTraversalSink<>(settings, SAMPLE_KEY))
          .setParallelism(1).name("SINK");
    else
      throw new IllegalArgumentException(
          "give --cardinalityDiscard dir or --traversalBenchDir (with --traversalDumpFile or --traversalQueryIds)");

    // The client of a cluster prints this line too; in-process runs only have this one.
    JobExecutionResult result = env.execute(pathSweep ? "SyntheticPathScale-GeneaLog" : "SyntheticSourceScale-GeneaLog");
    System.out.println("Job Runtime: " + result.getNetRuntime() + " ms");
  }

  public static class ExclusiveIdFilter implements FilterFunction<SyntheticScaleTupleGL> {
    private final int branch;
    private final int paths;

    public ExclusiveIdFilter(int branch, int paths) {
      this.branch = branch;
      this.paths = paths;
    }

    @Override
    public boolean filter(SyntheticScaleTupleGL t) {
      return t.id % paths == branch;
    }
  }

  /** The branch's map: a new tuple whose provenance is the source tuple it came from. */
  public static class PathMap implements MapFunction<SyntheticScaleTupleGL, SyntheticScaleTupleGL> {
    @Override
    public SyntheticScaleTupleGL map(SyntheticScaleTupleGL in) {
      SyntheticScaleTupleGL out = in.copyPayload();
      out.initGenealog(GenealogTupleType.MAP);
      out.setU1(in);
      return out;
    }
  }

  /** Timing sink: counts the outputs and writes the count at the end. No traversal. */
  public static class DiscardSink extends RichSinkFunction<SyntheticScaleTupleGL> {
    private final String dir;
    private transient long outputs;

    public DiscardSink(String dir) {
      this.dir = dir;
    }

    @Override
    public void open(Configuration parameters) {
      outputs = 0;
    }

    @Override
    public void invoke(SyntheticScaleTupleGL value, Context context) {
      outputs++;
    }

    @Override
    public void close() throws Exception {
      new File(dir).mkdirs();
      try (PrintWriter pw = new PrintWriter(new FileWriter(dir + "/outputs.txt"))) {
        pw.println(outputs);
      }
    }
  }
}
