package caps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import temporalindex.StampDerivedTime;

// End to end in a local Flink runtime: keyed tumbling windows -> StampDerivedTime -> the index sink
// attached through BenchSinks.attach (WatermarkTriggeringSink) at parallelism 1, 2 and 4. The index
// answers must equal a brute-force sum over the window outputs, nothing may be late, and the sink
// must have one shard per subtask.
public class TieredIndexSinkFlinkTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  static final long WINDOW = 100;

  public static class In implements Serializable {
    public String key;
    public long ts;
    public long a;
    public long b;

    public In() {}

    In(String key, long ts, long a, long b) {
      this.key = key;
      this.ts = ts;
      this.a = a;
      this.b = b;
    }
  }

  public static class Out implements Serializable {
    public String key;
    public long ts;
    public HowMuch hm;

    public Out() {}
  }

  public static class Acc implements Serializable {
    public String key;
    public long a;
    public long b;
  }

  static class Sum implements AggregateFunction<In, Acc, Out> {
    @Override
    public Acc createAccumulator() {
      return new Acc();
    }

    @Override
    public Acc add(In v, Acc acc) {
      acc.key = v.key;
      acc.a += v.a;
      acc.b += v.b;
      return acc;
    }

    @Override
    public Out getResult(Acc acc) {
      Out o = new Out();
      o.key = acc.key;
      o.ts = 0; // placeholder, StampDerivedTime sets the window boundary
      o.hm = HowMuch.of(acc.a, acc.b);
      return o;
    }

    @Override
    public Acc merge(Acc x, Acc y) {
      x.a += y.a;
      x.b += y.b;
      return x;
    }
  }

  static class Source implements SourceFunction<In> {
    final List<In> records;

    Source(List<In> records) {
      this.records = records;
    }

    @Override
    public void run(SourceContext<In> ctx) {
      for (In r : records)
        ctx.collect(r);
    }

    @Override
    public void cancel() {}
  }

  static List<In> input(int n) {
    Random rnd = new Random(5);
    List<In> out = new ArrayList<>();
    long ts = 1000;
    for (int i = 0; i < n; i++) {
      ts += rnd.nextInt(3);
      out.add(new In("k" + rnd.nextInt(50), ts, 1, rnd.nextInt(3)));
    }
    return out;
  }

  /** The window outputs the dataflow must produce: (key, window) -> {ts, a, b}. */
  static List<long[]> expectedOutputs(List<In> in) {
    Map<String, long[]> m = new HashMap<>();
    for (In r : in) {
      long start = Math.floorDiv(r.ts, WINDOW) * WINDOW;
      long[] x = m.computeIfAbsent(r.key + "@" + start, k -> new long[] {start + WINDOW - 1, 0, 0});
      x[1] += r.a;
      x[2] += r.b;
    }
    return new ArrayList<>(m.values());
  }

  void check(int p) throws Exception {
    List<In> in = input(30000);
    List<long[]> expect = expectedOutputs(in);
    File dir = tmp.newFolder("p" + p);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setParallelism(p);
    DataStream<In> src = env.addSource(new Source(in)).assignTimestampsAndWatermarks(
        new AssignerWithPunctuatedWatermarks<In>() {
          @Override
          public long extractTimestamp(In r, long prev) {
            return r.ts;
          }

          @Override
          public Watermark checkAndGetNextWatermark(In r, long ts) {
            return new Watermark(ts - 1); // "no more records <= ts - 1"; Watermark(ts) would drop a record at window end - 1 that follows an equal one
          }
        });
    DataStream<Out> out = src.keyBy(r -> r.key)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW)))
        .aggregate(new Sum())
        .process(new StampDerivedTime<Out>((t, ts) -> t.ts = ts))
        .returns(Out.class);
    @SuppressWarnings("unchecked")
    IndexingSink<Out> sink = (IndexingSink<Out>) BenchSinks.create(
        new String[] {"queries", dir.getAbsolutePath(), "300", "400"}, 0,
        (IndexingSink.TsFn<Out>) o -> o.ts, (IndexingSink.HmFn<Out>) o -> o.hm, 2);
    sink.expectedOutputs = expect.size(); // a provisional batch every tenth of the outputs
    BenchSinks.attach(out, sink, "SINK-INDEX");
    env.execute("two-tier-p" + p);

    // every window output is indexed exactly once, none is late, one shard per subtask
    List<String> stats = Files.readAllLines(new File(dir, "tiered_stats.csv").toPath());
    assertEquals(p + 1, stats.size());
    long indexed = 0;
    for (int i = 1; i < stats.size(); i++) {
      String[] f = stats.get(i).split(",");
      indexed += Long.parseLong(f[4]);
      assertEquals("late", "0", f[5]);
      assertEquals("tier 1 drained", "0", f[8]);
      assertTrue("every arrival precedes its watermark", f[14].equals("NA") || Long.parseLong(f[14]) < 0);
    }
    assertEquals(expect.size(), indexed);

    List<String> rows = Files.readAllLines(new File(dir, "query_answers.csv").toPath());
    assertTrue(rows.size() > 900);
    for (int i = 1; i < rows.size(); i++) {
      String[] c = rows.get(i).split(",");
      long from = Long.parseLong(c[2]);
      long to = Long.parseLong(c[3]);
      long a = 0, b = 0;
      int n = 0;
      for (long[] x : expect)
        if (x[0] >= from && x[0] <= to) {
          n++;
          a += x[1];
          b += x[2];
        }
      assertEquals(rows.get(i), a, Long.parseLong(c[4]));
      assertEquals(rows.get(i), b, Long.parseLong(c[5]));
      assertEquals(rows.get(i), n > 0 ? "1" : "0", c[6]);
    }
    List<String> prov = Files.readAllLines(new File(dir, "provisional_queries.csv").toPath());
    assertTrue("provisional batches ran", prov.size() > 1);
  }

  @Test
  public void parallelismOne() throws Exception {
    check(1);
  }

  @Test
  public void parallelismTwo() throws Exception {
    check(2);
  }

  @Test
  public void parallelismFour() throws Exception {
    check(4);
  }
}
