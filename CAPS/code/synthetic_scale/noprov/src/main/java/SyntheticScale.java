import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import temporalindex.StampDerivedTime;

/**
 * Channel-cardinality dataflow without provenance: the baseline of the CAPS,
 * Green and GeneaLog twins, which run the same computation with M channels.
 *
 * <pre>
 *   source sweep:  Source ---------------------------------------------------+
 *   path sweep:    Source -> M x [Filter(id % M == i) -> Map] -> Union
 *                                                                          |
 *        keyBy(key) -> TumblingWindow(60 s) -> Agg(count, sum) -> Stamp -> Sink
 * </pre>
 *
 * Source sweep: M changes nothing here (no annotation), the dataflow has one path.
 * Path sweep: M real exclusive branches, so the graph grows with M: this isolates the cost of
 * the extra operators from the cost of the annotation.
 * The output timestamp is the window end (StampDerivedTime), like in every workflow.
 *
 * Usage (parallelism from CAPS_PARALLELISM, default 1):
 *   SyntheticScale inputFile outputFile source|path M [mode benchDir params...]
 * without a mode the outputs are written to outputFile, one row per output: key,timestamp,count,sum
 * mode discard counts the outputs into benchDir/outputs.txt (the timed runs).
 */
public class SyntheticScale {

  // Must match data/synthetic_scale/generate_synthetic_scale.py.
  public static final long WINDOW_MS = 60_000L;

  public static void main(String[] args) throws Exception {
    if (args.length < 4) {
      System.err.println("Usage: SyntheticScale <inputFile> <outputFile> source|path <M>"
          + " [discard benchDir]");
      System.exit(1);
    }
    final String inputFile = args[0];
    final String outputFile = args[1];
    final boolean pathSweep = parseSweep(args[2]);
    final int channels = Integer.parseInt(args[3]);
    if (channels < 1) throw new IllegalArgumentException("M must be >= 1");
    final String mode = args.length > 4 ? args[4] : "file";

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    final int parallelism = Integer.parseInt(
        System.getenv().getOrDefault("CAPS_PARALLELISM", "1"));
    env.setParallelism(parallelism);

    DataStream<Record> sourceStream = env
        .addSource(new RecordSource(inputFile))
        .name("SOURCE")
        .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Record>() {
          @Override
          public long extractTimestamp(Record r, long prev) {
            return r.timestamp;
          }
          @Override
          public Watermark checkAndGetNextWatermark(Record r, long ts) {
            return new Watermark(ts);
          }
        });

    DataStream<Record> tagged = sourceStream;
    if (pathSweep) {
      // M real exclusive branches: every record takes the one whose index is id % M.
      DataStream<Record>[] branches = new DataStream[channels];
      for (int i = 0; i < channels; i++)
        branches[i] = sourceStream
            .filter(new ExclusiveIdFilter(i, channels))
            .name("PATH-FILTER-" + i)
            .map(new PathTagMap(i, channels))
            .name("PATH-MAP-" + i);
      DataStream<Record>[] others = new DataStream[channels - 1];
      System.arraycopy(branches, 1, others, 0, channels - 1);
      tagged = branches[0].union(others);
    }

    DataStream<AggOut> aggregated = tagged
        .keyBy(r -> r.key)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_MS)))
        .aggregate(new CountSumAggregate())
        .name("AGG")
        .process(new StampDerivedTime<AggOut>((t, ts) -> t.timestamp = ts))
        .returns(AggOut.class)
        .name("STAMP-AGG");

    if (mode.equals("file"))
      aggregated.addSink(new FileSink(outputFile)).setParallelism(1).name("SINK");
    else if (mode.equals("discard"))
      aggregated.addSink(new DiscardSink(args[5])).setParallelism(1).name("SINK");
    else
      throw new IllegalArgumentException("unknown mode: " + mode);

    // The client of a cluster prints this line too; in-process runs only have this one.
    JobExecutionResult result = env.execute(pathSweep ? "SyntheticPathScale-No-provenance" : "SyntheticSourceScale-No-provenance");
    System.out.println("Job Runtime: " + result.getNetRuntime() + " ms");
  }

  static boolean parseSweep(String sweep) {
    if (sweep.equals("path")) return true;
    if (sweep.equals("source")) return false;
    throw new IllegalArgumentException("sweep must be source or path: " + sweep);
  }

  // Data types
  public static class Record implements Serializable {
    public long id;
    public long timestamp;
    public int sourceId;
    public int key;
    public long value;
  }

  public static class AggOut implements Serializable {
    public int key;
    public long timestamp;
    public long count;
    public long sum;

    public AggOut() {}

    public AggOut(int key, long timestamp, long count, long sum) {
      this.key = key;
      this.timestamp = timestamp;
      this.count = count;
      this.sum = sum;
    }

    @Override
    public String toString() {
      return key + "," + timestamp + "," + count + "," + sum;
    }
  }

  // Source
  /** Reads synthetic_scale.txt: id,timestamp,sourceId,key,value. */
  public static class RecordSource extends RichSourceFunction<Record> {
    private final String path;
    private volatile boolean running = true;

    public RecordSource(String path) {
      this.path = path;
    }

    @Override
    public void run(SourceContext<Record> ctx) throws Exception {
      try (BufferedReader br = new BufferedReader(new FileReader(path))) {
        String line;
        while (running && (line = br.readLine()) != null) {
          if (line.isEmpty()) continue;
          String[] parts = line.split(",", 5);
          Record r = new Record();
          r.id = Long.parseLong(parts[0]);
          r.timestamp = Long.parseLong(parts[1]);
          r.sourceId = Integer.parseInt(parts[2]);
          r.key = Integer.parseInt(parts[3]);
          r.value = Long.parseLong(parts[4]);
          ctx.collect(r);
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  // Operators
  public static class ExclusiveIdFilter implements FilterFunction<Record> {
    private final int branch;
    private final int paths;

    public ExclusiveIdFilter(int branch, int paths) {
      this.branch = branch;
      this.paths = paths;
    }

    @Override
    public boolean filter(Record r) {
      return r.id % paths == branch;
    }
  }

  /** The branch's map: copies the record (the instrumented methods tag it here). */
  public static class PathTagMap implements MapFunction<Record, Record> {
    private final int branch;
    private final int paths;

    public PathTagMap(int branch, int paths) {
      this.branch = branch;
      this.paths = paths;
    }

    @Override
    public Record map(Record r) {
      Record o = new Record();
      o.id = r.id;
      o.timestamp = r.timestamp;
      o.sourceId = r.sourceId;
      o.key = r.key;
      o.value = r.value;
      return o;
    }
  }

  /** Windowed count and sum. */
  public static class CountSumAggregate
      implements AggregateFunction<Record, CountSumAggregate.Acc, AggOut> {

    @Override
    public Acc createAccumulator() {
      return new Acc();
    }

    @Override
    public Acc add(Record value, Acc acc) {
      acc.key = value.key;
      acc.count++;
      acc.sum += value.value;
      return acc;
    }

    @Override
    public AggOut getResult(Acc acc) {
      // Placeholder: StampDerivedTime sets the window end after the operator.
      return new AggOut(acc.key, 0, acc.count, acc.sum);
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.key = b.key;
      a.count += b.count;
      a.sum += b.sum;
      return a;
    }

    public static class Acc implements Serializable {
      int key;
      long count;
      long sum;
    }
  }

  // Sinks
  /** Verification sink: every output as one row, in arrival order. */
  public static class FileSink extends RichSinkFunction<AggOut> {
    private final String path;
    private transient PrintWriter pw;

    public FileSink(String path) {
      this.path = path;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      pw = new PrintWriter(new BufferedWriter(new FileWriter(path), 1 << 20));
    }

    @Override
    public void invoke(AggOut value, Context context) {
      pw.println(value);
    }

    @Override
    public void close() throws Exception {
      if (pw != null) {
        pw.print("--- OUTPUT END ---");
        pw.close();
      }
    }
  }

  /** Timing sink: counts the outputs and writes the count at the end. No per-output work. */
  public static class DiscardSink extends RichSinkFunction<AggOut> {
    private final String dir;
    private transient long outputs;

    public DiscardSink(String dir) {
      this.dir = dir;
    }

    @Override
    public void invoke(AggOut value, Context context) {
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
