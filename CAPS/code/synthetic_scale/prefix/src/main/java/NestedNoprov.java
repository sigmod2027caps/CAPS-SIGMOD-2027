import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
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
 * Nested-path test without provenance: three levels of 8-way multiplexes
 * (8 + 64 + 512 Filters, 512 leaf paths p = id mod 512) recombined by one windowed join.
 * Level 1 splits on bits 0-2 of the id, level 2 on bits 3-5, level 3 on bits 6-8. A leaf copies the record (M is ignored).
 * The leaves whose top branch (p mod 8) is 0-3 are united into the left input, 4-7 into the right input.
 * Each side: keyBy key -> 60 s tumbling window -> count/sum . Join on key in the
 * same window; no annotation.
 *
 * <pre>
 *   Source -> Filter(L1) -> Filter(L2) -> Filter(L3) -> Map(copy) -> Union(left | right)
 *          -> keyBy -> Window -> Agg -> Stamp  (left, right)  -> Join(key, 60 s window) -> Stamp -> Sink
 * </pre>
 *
 * Usage: NestedNoprov inputFile outputFile M [file | discard dir]
 *   file:    one row per output: key,timestamp,count,sum
 *   discard: count the outputs into dir/outputs.txt (the timed runs)
 */
public class NestedNoprov {

  public static final long WINDOW_MS = 60_000L;

  public static void main(String[] args) throws Exception {
    final String inputFile = args[0];
    final String outputFile = args[1];
    final String mode = args.length > 3 ? args[3] : "file";

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setParallelism(Integer.parseInt(System.getenv().getOrDefault("CAPS_PARALLELISM", "1")));

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

    List<DataStream<Record>> left = new ArrayList<DataStream<Record>>();
    List<DataStream<Record>> right = new ArrayList<DataStream<Record>>();
    buildTree(sourceStream, 0, 0, left, right);

    DataStream<AggOut> leftAgg = sideAggregate(left, "L");
    DataStream<AggOut> rightAgg = sideAggregate(right, "R");

    DataStream<AggOut> joined = leftAgg.join(rightAgg)
        .where(a -> a.key).equalTo(b -> b.key)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_MS)))
        .apply(new MergeJoin())
        .process(new StampDerivedTime<AggOut>((t, ts) -> t.timestamp = ts))
        .returns(AggOut.class)
        .name("STAMP-JOIN");

    if (mode.equals("file"))
      joined.addSink(new FileSink(outputFile)).setParallelism(1).name("SINK");
    else if (mode.equals("discard"))
      joined.addSink(new DiscardSink(args[4])).setParallelism(1).name("SINK");
    else
      throw new IllegalArgumentException("unknown mode " + mode);

    JobExecutionResult result = env.execute("Nested-noprov");
    System.out.println("Job Runtime: " + result.getNetRuntime() + " ms");
  }

  // Level k (0..2) splits on bits 3k..3k+2 of the id; the leaf path is p = id mod 512.
  static void buildTree(DataStream<Record> s, int level, int prefix,
      List<DataStream<Record>> left, List<DataStream<Record>> right) {
    if (level == 3) {
      DataStream<Record> leaf = s.map(new LeafTagMap(prefix)).name("LEAF-MAP-" + prefix);
      if ((prefix & 7) < 4) left.add(leaf);
      else right.add(leaf);
      return;
    }
    for (int b = 0; b < 8; b++)
      buildTree(s.filter(new MaskFilter(3 * level, b)).name("FILTER-L" + level + "-" + (prefix | (b << (3 * level)))),
          level + 1, prefix | (b << (3 * level)), left, right);
  }

  static DataStream<AggOut> sideAggregate(List<DataStream<Record>> leaves, String name) {
    DataStream<Record> u = leaves.get(0).union(leaves.subList(1, leaves.size()).toArray(new DataStream[0]));
    return u
        .keyBy(r -> r.key)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_MS)))
        .aggregate(new CountSumAggregate()).name("AGG-" + name)
        .process(new StampDerivedTime<AggOut>((t, ts) -> t.timestamp = ts))
        .returns(AggOut.class)
        .name("STAMP-" + name);
  }

  /** Join: counts and sums add up. */
  public static class MergeJoin implements JoinFunction<AggOut, AggOut, AggOut> {
    @Override
    public AggOut join(AggOut l, AggOut r) {
      return new AggOut(l.key, 0, l.count + r.count, l.sum + r.sum);
    }
  }

  // ── Data types ──────────────────────────────────────────────

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

  // ── Source ──────────────────────────────────────────────────

  /** Reads synthetic_scale.txt: id,timestamp,sourceId,key,value (no annotation yet). */
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

  // ── Operators ──────────────────────────────────────────────

  /** One branch of a multiplex: keeps the records whose 3 bits of the id at `shift` equal `b`. */
  public static class MaskFilter implements FilterFunction<Record> {
    private final int shift;
    private final int b;

    public MaskFilter(int shift, int b) {
      this.shift = shift;
      this.b = b;
    }

    @Override
    public boolean filter(Record r) {
      return ((r.id >> shift) & 7) == b;
    }
  }

  /** The leaf path's operator: copies the tuple (no annotation). */
  public static class LeafTagMap implements MapFunction<Record, Record> {
    private final int leaf;

    public LeafTagMap(int leaf) {
      this.leaf = leaf;
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

  /** Windowed count and sum that also adds up the per-channel counters: O(M) per input. */
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

  // ── Sinks ───────────────────────────────────────────────────

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
