package temporalindex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

// Checkpoint and restore of the parallel file source (E4). The source is driven by hand the way
// Flink drives it: run() in one thread, snapshotState() under the checkpoint lock from another, between two
// steps. A source restored from any such snapshot must emit exactly the records the first run emitted after it.
public class DisorderSourceRecoveryTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  public static class Rec implements java.io.Serializable {
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
      return r;
    }
  }

  /** A ListState in memory. */
  static final class MemState implements ListState<byte[]> {
    final List<byte[]> items = new ArrayList<>();

    @Override public void update(List<byte[]> values) { items.clear(); items.addAll(values); }
    @Override public void addAll(List<byte[]> values) { items.addAll(values); }
    @Override public Iterable<byte[]> get() { return items; }
    @Override public void add(byte[] value) { items.add(value); }
    @Override public void clear() { items.clear(); }
  }

  @SuppressWarnings("unchecked")
  static <S> S proxy(Class<S> type, java.lang.reflect.InvocationHandler h) {
    return (S) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, h);
  }

  /** One reader driven by hand. */
  static final class Harness {
    final DisorderSource<Rec> src;
    final MemState state = new MemState();
    final Object lock = new Object();
    final List<long[]> out = new ArrayList<>(); // (line, ts), in emission order
    final List<Long> nanos = new ArrayList<>(); // System.nanoTime() of every emission
    int idleMarks;
    final long sleepEvery;
    final FunctionInitializationContext init;
    final SourceFunction.SourceContext<Rec> ctx;

    Harness(String path, DisorderConfig cfg, int p, int me, int attempt, byte[] restored, long sleepEvery) {
      this.sleepEvery = sleepEvery;
      src = new DisorderSource<>("SOURCE", path, new P(), r -> r.ts, cfg,
          TypeInformation.of(Rec.class).createSerializer(new ExecutionConfig()));
      src.setRuntimeContext(proxy(RuntimeContext.class, (o, m, a) -> {
        switch (m.getName()) {
          case "getNumberOfParallelSubtasks": return p;
          case "getIndexOfThisSubtask": return me;
          case "getAttemptNumber": return attempt;
          default: throw new UnsupportedOperationException(m.getName());
        }
      }));
      if (restored != null)
        state.add(restored);
      boolean isRestored = restored != null;
      init = proxy(FunctionInitializationContext.class, (o, m, a) -> {
        switch (m.getName()) {
          case "isRestored": return isRestored;
          case "getOperatorStateStore":
            return proxy(org.apache.flink.api.common.state.OperatorStateStore.class, (o2, m2, a2) -> state);
          default: throw new UnsupportedOperationException(m.getName());
        }
      });
      ctx = proxy(SourceFunction.SourceContext.class, (o, m, a) -> {
        switch (m.getName()) {
          case "collect": {
            Rec r = (Rec) a[0];
            out.add(new long[] {r.line, r.ts});
            nanos.add(System.nanoTime());
            if (sleepEvery > 0 && out.size() % sleepEvery == 0)
              Thread.sleep(1);
            return null;
          }
          case "getCheckpointLock": return lock;
          case "markAsTemporarilyIdle": idleMarks++; return null;
          case "close": return null;
          default: throw new UnsupportedOperationException(m.getName());
        }
      });
    }

    void run() throws Exception {
      src.initializeState(init);
      src.run(ctx);
    }

    /** Snapshot under the lock, between two steps. Returns {records emitted so far} and the bytes. */
    Object[] snapshot() throws Exception {
      synchronized (lock) {
        src.snapshotState(proxy(FunctionSnapshotContext.class, (o, m, a) -> { throw new UnsupportedOperationException(); }));
        return new Object[] {out.size(), state.items.get(0)};
      }
    }
  }

  private String file(int n) throws Exception {
    File f = tmp.newFile();
    try (PrintWriter pw = new PrintWriter(f)) {
      for (int i = 0; i < n; i++)
        pw.println(i + "," + (1000 + 10 * (i / 2)));
    }
    return f.getPath();
  }

  private static DisorderConfig cfg(long d, long delta, Map<String, String> extra) {
    Map<String, String> m = new HashMap<>();
    m.put("CAPS_DISORDER_MS", Long.toString(d));
    m.put("CAPS_DELTA_MS", Long.toString(delta));
    m.put("CAPS_CHECKPOINT_MS", "100"); // a checkpointed reader: its lines and position are atomic steps
    m.putAll(extra);
    return DisorderConfig.of(m);
  }

  /** Runs the reader to the end while another thread snapshots it at about the given fractions of its output. */
  private void checkRestoredReadersContinueExactly(long d, int p, int me) throws Exception {
    String path = file(6000);
    DisorderConfig cfg = cfg(d, d, new HashMap<String, String>());
    Harness a = new Harness(path, cfg, p, me, 0, null, 25);
    List<Object[]> snaps = new ArrayList<>();
    int mine = 6000 / p;
    Thread t = new Thread(() -> {
      try {
        for (double frac : new double[] {0.1, 0.45, 0.8}) {
          while (true) {
            Object[] s;
            synchronized (a.lock) {
              if (a.out.size() < frac * mine)
                s = null;
              else
                s = a.snapshot();
            }
            if (s != null) {
              snaps.add(s);
              break;
            }
            Thread.yield();
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    t.start();
    a.run();
    t.join();
    assertEquals(mine, a.out.size());
    assertEquals(3, snaps.size());
    boolean bufferWasNonEmpty = false;
    for (Object[] s : snaps) {
      int c = (Integer) s[0];
      Harness b = new Harness(path, cfg, p, me, 1, (byte[]) s[1], 0);
      b.run();
      List<long[]> expected = a.out.subList(c, a.out.size());
      assertEquals("restored from after " + c + " records", expected.size(), b.out.size());
      for (int i = 0; i < expected.size(); i++)
        assertTrue("record " + (c + i), Arrays.equals(expected.get(i), b.out.get(i)));
      bufferWasNonEmpty |= d > 0 && ((byte[]) s[1]).length > 48; // 4 + 5 * 8 + 4 bytes without a buffer
    }
    if (d > 0)
      assertTrue("the cut should have records in the disorder buffer", bufferWasNonEmpty);
  }

  @Test
  public void orderedReaderResumesAtTheStoredOffset() throws Exception {
    checkRestoredReadersContinueExactly(0, 1, 0);
  }

  @Test
  public void disorderedReaderResumesWithItsBuffer() throws Exception {
    checkRestoredReadersContinueExactly(300, 1, 0);
  }

  @Test
  public void parallelReaderResumesOnItsOwnLines() throws Exception {
    checkRestoredReadersContinueExactly(300, 3, 1);
  }

  /** The cuts of a checkpointed reader fall between batches: the checkpoint lock is taken once per batch, not once per line. */
  @Test
  public void checkpointLockIsTakenPerBatchNotPerLine() throws Exception {
    int n = 9500;
    String path = file(n);
    DisorderConfig cfg = cfg(0, 0, new HashMap<String, String>());
    Harness a = new Harness(path, cfg, 1, 0, 0, null, 100);
    List<Object[]> snaps = new ArrayList<>();
    boolean[] done = {false};
    Thread t = new Thread(() -> { // a checkpoint coordinator that triggers as often as it can
      try {
        int last = -1;
        while (!done[0]) {
          synchronized (a.lock) {
            if (a.out.size() != last && a.out.size() > 0) {
              snaps.add(a.snapshot());
              last = a.out.size();
            }
          }
          Thread.yield();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    t.start();
    a.run();
    done[0] = true;
    t.join();
    assertEquals(n, a.out.size());
    assertTrue("the coordinator found several cuts", snaps.size() >= 3);
    for (Object[] sn : snaps) {
      int c = (Integer) sn[0];
      assertTrue("a cut after " + c + " records is between batches", c % DisorderSource.CHECKPOINT_BATCH == 0 || c == n);
    }
    // restore from some of the cuts: exactly the remaining records, in the same order
    for (int k = 0; k < snaps.size(); k += Math.max(1, snaps.size() / 4)) {
      int c = (Integer) snaps.get(k)[0];
      Harness b = new Harness(path, cfg, 1, 0, 1, (byte[]) snaps.get(k)[1], 0);
      b.run();
      assertEquals(n - c, b.out.size());
      for (int i = 0; i < b.out.size(); i++)
        assertTrue("record " + (c + i), Arrays.equals(a.out.get(c + i), b.out.get(i)));
    }
  }

  /** Pacing follows the schedule line by line: no burst of a batch of lines, and the whole file takes the paced time. */
  private void checkPacedReaderIsSmooth(String checkpointMs) throws Exception {
    int n = 3000;
    String path = file(n);
    Map<String, String> extra = new HashMap<>();
    extra.put("CAPS_PACE_SECONDS", "1");
    extra.put("CAPS_CHECKPOINT_MS", checkpointMs);
    DisorderConfig cfg = cfg(0, 0, extra);
    Harness a = new Harness(path, cfg, 1, 0, 0, null, 0);
    long t0 = System.nanoTime();
    a.run();
    double seconds = (System.nanoTime() - t0) / 1e9;
    assertEquals(n, a.out.size());
    assertTrue("paced to 1 s, took " + seconds, seconds > 0.9 && seconds < 1.6);
    // lines per 50 ms bucket: the schedule gives 150; a reader that sleeps once per 256 or 1000 lines gives bursts of more
    int[] bucket = new int[40];
    for (long ns : a.nanos)
      bucket[Math.min(39, (int) ((ns - a.nanos.get(0)) / 50_000_000L))]++;
    int max = 0;
    for (int b : bucket)
      max = Math.max(max, b);
    assertTrue("no bucket of 50 ms holds more than 300 lines, the largest has " + max, max <= 300);
  }

  @Test
  public void pacedReaderFollowsTheScheduleLineByLine() throws Exception {
    checkPacedReaderIsSmooth("0");
  }

  @Test
  public void pacedCheckpointedReaderFollowsTheScheduleToo() throws Exception {
    checkPacedReaderIsSmooth("100");
  }

  @Test
  public void faultInjectionFiresOnceInTheFirstAttemptOnly() throws Exception {
    String path = file(3000);
    Map<String, String> extra = new HashMap<>();
    extra.put("CAPS_FAIL_AT", "1000");
    extra.put("CAPS_FAIL_SOURCE", "SOURCE");
    DisorderConfig cfg = cfg(0, 0, extra);
    Harness first = new Harness(path, cfg, 1, 0, 0, null, 0);
    try {
      first.run();
      fail("expected the injected failure");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("fault injection"));
    }
    assertEquals(1000, first.out.size());
    Harness second = new Harness(path, cfg, 1, 0, 1, null, 0);
    second.run();
    assertEquals(3000, second.out.size());
    // another reader name is not affected
    extra.put("CAPS_FAIL_SOURCE", "OTHER");
    Harness other = new Harness(path, cfg(0, 0, extra), 1, 0, 0, null, 0);
    other.run();
    assertEquals(3000, other.out.size());
  }

  @Test
  public void idlenessIsOffWhileReplaying() throws Exception {
    // a slow reader (subtask 0 of 2) that would mark itself idle after 1 ms without a record
    String path = file(4000);
    Map<String, String> extra = new HashMap<>();
    extra.put("CAPS_SLOW_FACTOR", "20");
    extra.put("CAPS_SLOW_BATCH", "50");
    extra.put("CAPS_IDLE_MS", "1");
    DisorderConfig cfg = cfg(0, 0, extra);
    Harness a = new Harness(path, cfg, 2, 0, 0, null, 10);
    Object[][] snap = new Object[1][];
    Thread t = new Thread(() -> {
      try {
        while (snap[0] == null) {
          synchronized (a.lock) {
            if (a.out.size() >= 500)
              snap[0] = a.snapshot();
          }
          Thread.yield();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    t.start();
    a.run();
    t.join();
    assertTrue("the slow reader marks itself idle when it is not replaying", a.idleMarks > 0);
    Harness b = new Harness(path, cfg, 2, 0, 1, (byte[]) snap[0][1], 10);
    b.run();
    assertEquals(0, b.idleMarks);
    assertEquals(a.out.size() - (Integer) snap[0][0], b.out.size());
  }

  /** A ListState of Longs in memory. */
  static final class LongState implements ListState<Long> {
    final List<Long> items = new ArrayList<>();

    @Override public void update(List<Long> values) { items.clear(); items.addAll(values); }
    @Override public void addAll(List<Long> values) { items.addAll(values); }
    @Override public Iterable<Long> get() { return items; }
    @Override public void add(Long value) { items.add(value); }
    @Override public void clear() { items.clear(); }
  }

  private static FunctionInitializationContext initContext(ListState<Long> state, boolean restored) {
    return proxy(FunctionInitializationContext.class, (o, m, a) -> {
      switch (m.getName()) {
        case "isRestored": return restored;
        case "getOperatorStateStore":
          return proxy(org.apache.flink.api.common.state.OperatorStateStore.class, (o2, m2, a2) -> state);
        default: throw new UnsupportedOperationException(m.getName());
      }
    });
  }

  @Test
  public void watermarkAssignerContinuesFromTheStoredMaximum() throws Exception {
    Disorder.Assigner<Rec> a = new Disorder.Assigner<>(100, r -> r.ts);
    LongState st = new LongState();
    a.initializeState(initContext(st, false));
    assertEquals(Long.MIN_VALUE, a.getCurrentWatermark().getTimestamp()); // nothing seen: the minimum
    Rec r = new Rec();
    r.ts = 5000;
    a.extractTimestamp(r, 0);
    r.ts = 4900; // behind: the maximum stays
    a.extractTimestamp(r, 0);
    assertEquals(5000 - 101, a.getCurrentWatermark().getTimestamp()); // newest - (delta + 1)
    a.snapshotState(null);
    // a restored assigner continues from the watermark position of the checkpoint, not from the minimum
    Disorder.Assigner<Rec> b = new Disorder.Assigner<>(100, x -> x.ts);
    b.initializeState(initContext(st, true));
    assertEquals(5000 - 101, b.getCurrentWatermark().getTimestamp());
    r.ts = 4950; // a replayed record that is behind the maximum does not move it backwards
    b.extractTimestamp(r, 0);
    assertEquals(5000 - 101, b.getCurrentWatermark().getTimestamp());
    r.ts = 5200;
    b.extractTimestamp(r, 0);
    assertEquals(5200 - 101, b.getCurrentWatermark().getTimestamp());
  }
}
