package temporalindex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

// The disorder machinery of experiment E3: the parallel replayable source (which lines each reader gets, the bound
// on the disorder, the arrival order by timestamp + delay, the slow reader), the Zipf remapping and the counting
// trigger.
public class DisorderSourceTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  /** What the sink saw, per sink subtask (= reader: source, assigner and sink are chained), in arrival order. */
  static final Map<Integer, List<long[]>> SEEN = new ConcurrentHashMap<>();

  public static class Rec implements Serializable {
    public long line;
    public long ts;
  }

  public static class P extends DisorderSource.Parser<Rec> {
    @Override
    public Rec parse(String line, long lineNo) {
      String[] f = line.split(",");
      Rec r = new Rec();
      r.line = Long.parseLong(f[0]);
      r.ts = Long.parseLong(f[1]);
      if (r.line != lineNo)
        throw new IllegalStateException("line " + lineNo + " parsed as " + r.line);
      return r;
    }
  }

  public static class Collect extends RichSinkFunction<Rec> {
    @Override
    public void invoke(Rec r, Context context) {
      SEEN.computeIfAbsent(getRuntimeContext().getIndexOfThisSubtask(), k -> new ArrayList<>()).add(new long[] {r.line, r.ts});
    }
  }

  /** n lines "i,ts" with ts non-decreasing (pairs of equal timestamps, 10 ms apart). */
  private String file(int n) throws Exception {
    File f = tmp.newFile();
    try (PrintWriter pw = new PrintWriter(f)) {
      for (int i = 0; i < n; i++)
        pw.println(i + "," + (1000 + 10 * (i / 2)));
      pw.println(); // a blank line: counted as a line, skipped
    }
    return f.getPath();
  }

  private void run(String path, Map<String, String> env) throws Exception {
    SEEN.clear();
    DisorderConfig cfg = DisorderConfig.of(env);
    StreamExecutionEnvironment e = Disorder.environment(cfg);
    DataStream<Rec> s = Disorder.source(e, cfg, "SOURCE", path, new P(), r -> r.ts, Rec.class);
    s.addSink(new Collect()).setParallelism(cfg.sourceParallelism);
    e.execute("disorder-test");
  }

  private static Map<String, String> env(int p, long d, long delta) {
    Map<String, String> m = new HashMap<>();
    m.put("CAPS_PARALLELISM", Integer.toString(p));
    m.put("CAPS_DISORDER_MS", Long.toString(d));
    m.put("CAPS_DELTA_MS", Long.toString(delta));
    return m;
  }

  @Test
  public void withoutDisorderEveryReaderGetsItsLinesInFileOrder() throws Exception {
    run(file(3000), env(3, 0, 0));
    assertEquals(3, SEEN.size());
    for (int j = 0; j < 3; j++) {
      List<long[]> got = SEEN.get(j);
      assertEquals(1000, got.size());
      for (int k = 0; k < got.size(); k++)
        assertEquals(3L * k + j, got.get(k)[0]);
    }
  }

  @Test
  public void disorderIsBoundedAndArrivalFollowsTimestampPlusDelay() throws Exception {
    long d = 500;
    run(file(3000), env(2, d, d));
    Set<Long> all = new HashSet<>();
    boolean reordered = false;
    for (int j = 0; j < 2; j++) {
      long maxTs = Long.MIN_VALUE, prevKey = Long.MIN_VALUE;
      for (long[] rec : SEEN.get(j)) {
        assertTrue(all.add(rec[0]));
        assertEquals(j, rec[0] % 2);
        assertTrue("line " + rec[0] + " is more than D behind", maxTs == Long.MIN_VALUE || maxTs - rec[1] <= d);
        reordered |= rec[1] < maxTs;
        maxTs = Math.max(maxTs, rec[1]);
        long delay = (Zipf.mix(7L ^ (rec[0] * 0x9E3779B97F4A7C15L)) >>> 1) % (d + 1);
        assertTrue("arrival order is not by ts + delay", rec[1] + delay >= prevKey);
        prevKey = rec[1] + delay;
      }
    }
    assertEquals(3000, all.size());
    assertTrue("no record was reordered", reordered);
  }

  @Test
  public void theSameRecordsAtEveryParallelismAndTheDelayDoesNotDependOnIt() throws Exception {
    String path = file(2000);
    Map<Long, Long> one = new HashMap<>();
    run(path, env(1, 300, 300));
    for (long[] r : SEEN.get(0))
      one.put(r[0], r[1]);
    run(path, env(4, 300, 300));
    Map<Long, Long> four = new HashMap<>();
    for (List<long[]> l : SEEN.values())
      for (long[] r : l)
        four.put(r[0], r[1]);
    assertEquals(2000, one.size());
    assertEquals(one, four);
  }

  @Test
  public void aSlowReaderStillDeliversEverything() throws Exception {
    Map<String, String> m = env(2, 100, 100);
    m.put("CAPS_SLOW_FACTOR", "3");
    m.put("CAPS_SLOW_BATCH", "100");
    run(file(2000), m);
    assertEquals(1000, SEEN.get(0).size());
    assertEquals(1000, SEEN.get(1).size());
  }

  @Test
  public void anIdleTimeoutMarksTheSlowReaderIdleAndLosesNothing() throws Exception {
    Map<String, String> m = env(2, 0, 0);
    m.put("CAPS_SLOW_FACTOR", "20");
    m.put("CAPS_SLOW_BATCH", "50");
    m.put("CAPS_IDLE_MS", "1");
    m.put("CAPS_TRACE_DIR", tmp.getRoot().getPath());
    run(file(1000), m);
    assertEquals(500, SEEN.get(0).size());
    assertEquals(500, SEEN.get(1).size());
    long marks = 0;
    for (int j = 0; j < 2; j++)
      try (java.util.Scanner sc = new java.util.Scanner(new File(tmp.getRoot(), "src_stats_SOURCE_s" + j + ".csv"))) {
        sc.nextLine();
        marks += Long.parseLong(sc.nextLine().split(",")[6]);
      }
    assertTrue("the slow reader never went idle", marks > 0);
  }

  @Test
  public void zipfIsDeterministicSkewedAndInRange() {
    Zipf z = new Zipf(1000, 1.0, 12345);
    int[] count = new int[1000];
    for (long line = 0; line < 200000; line++) {
      int r = z.rank(line, 0);
      assertTrue(r >= 0 && r < 1000);
      assertEquals(r, z.rank(line, 0));
      count[r]++;
    }
    double h = 0;
    for (int r = 1; r <= 1000; r++)
      h += 1.0 / r;
    assertEquals(200000 / h, count[0], 0.05 * 200000 / h); // rank 0 has probability 1 / H_n
    assertTrue(count[0] > 4 * count[9]); // 1/1 against 1/10: well apart
    assertFalse("different salts must not give the same stream", z.rank(5, 0) == z.rank(5, 1) && z.rank(6, 0) == z.rank(6, 1)
        && z.rank(7, 0) == z.rank(7, 1) && z.rank(8, 0) == z.rank(8, 1));
  }

  // MissedWindowTrigger
  private static final class Stub {
    final Counter counter = new SimpleCounter();
    long watermark;
    final List<Long> timers = new ArrayList<>();
    Trigger.TriggerContext ctx;

    Stub() {
      final MetricGroup group = new UnregisteredMetricsGroup() {
        @Override
        public Counter counter(String name) {
          return counter;
        }
      };
      ctx = (Trigger.TriggerContext) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {Trigger.TriggerContext.class},
          (proxy, method, args) -> {
            switch (method.getName()) {
              case "getCurrentWatermark":
                return watermark;
              case "getMetricGroup":
                return group;
              case "registerEventTimeTimer":
                timers.add((Long) args[0]);
                return null;
              default:
                throw new UnsupportedOperationException(method.getName());
            }
          });
    }
  }

  @Test
  public void theTriggerCountsARecordThatMissedItsFirstWindowOnce() throws Exception {
    long size = 3600, slide = 600;
    MissedWindowTrigger t = new MissedWindowTrigger(slide);
    Stub s = new Stub();
    long ts = 1000; // windows [-2400,1200) ... [600,4200): last start 600, first window ends at 1199
    List<TimeWindow> windows = new ArrayList<>();
    for (long start = 600; start > 600 - size; start -= slide)
      windows.add(new TimeWindow(start, start + size));
    // watermark below the record: nothing is late
    s.watermark = 500;
    for (TimeWindow w : windows)
      assertEquals(TriggerResult.CONTINUE, t.onElement(null, ts, w, s.ctx));
    assertEquals(0, s.counter.getCount());
    // behind the watermark but the first window has not fired: not missed
    s.watermark = 1100;
    for (TimeWindow w : windows)
      t.onElement(null, ts, w, s.ctx);
    assertEquals(0, s.counter.getCount());
    // the first window has fired (its right boundary 1199 <= watermark): missed once, however many windows still take it
    s.watermark = 1199;
    for (TimeWindow w : windows.subList(0, 5)) // the windows that are not late (the sixth, [-2400,1200), is skipped by Flink)
      t.onElement(null, ts, w, s.ctx);
    assertEquals(1, s.counter.getCount());
    // a tumbling window: the record's window is its first and last one; behind the watermark but not yet fired it is
    // not missed, and once it has fired Flink never offers the record to it
    MissedWindowTrigger tumbling = new MissedWindowTrigger(100);
    s.watermark = 160;
    tumbling.onElement(null, 150, new TimeWindow(100, 200), s.ctx);
    assertEquals(1, s.counter.getCount());
  }

  @Test
  public void theTriggerFiresLikeTheEventTimeTrigger() throws Exception {
    MissedWindowTrigger t = new MissedWindowTrigger(100);
    Stub s = new Stub();
    TimeWindow w = new TimeWindow(100, 200);
    s.watermark = 150;
    assertEquals(TriggerResult.CONTINUE, t.onElement(null, 120, w, s.ctx));
    assertEquals(java.util.Collections.singletonList(199L), s.timers);
    assertEquals(TriggerResult.CONTINUE, t.onEventTime(150, w, s.ctx));
    assertEquals(TriggerResult.FIRE, t.onEventTime(199, w, s.ctx));
    s.watermark = 250;
    assertEquals(TriggerResult.FIRE, t.onElement(null, 120, w, s.ctx));
  }
}
