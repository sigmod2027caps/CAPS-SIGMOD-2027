package caps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

// Expiry, summarize and query-length sinks over the two-tier index, at one and at three subtasks.
public class TieredSinksTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  static final long MIN = 60_000L;

  static List<TieredIndexSinkTest.Rec> stream(long seed, int n, long base) {
    Random rnd = new Random(seed);
    List<TieredIndexSinkTest.Rec> out = new ArrayList<>();
    long clock = base;
    for (int i = 0; i < n; i++) {
      clock += MIN * (1 + rnd.nextInt(4));
      out.add(new TieredIndexSinkTest.Rec(clock - MIN * rnd.nextInt(20), rnd.nextInt(3), 1 + rnd.nextInt(2)));
    }
    return out;
  }

  static <T> void run(TieredIndexSink<TieredIndexSinkTest.Rec, ?> sink, List<TieredIndexSinkTest.Rec> recs)
      throws Exception {
    sink.open(new Configuration());
    long clock = Long.MIN_VALUE;
    for (TieredIndexSinkTest.Rec r : recs) {
      sink.invoke(r, null);
      clock = Math.max(clock, r.ts);
      sink.accept(new Watermark(clock - 20 * MIN - 1));
    }
    sink.accept(new Watermark(Long.MAX_VALUE));
    sink.close();
  }

  interface Factory {
    TieredIndexSink<TieredIndexSinkTest.Rec, ?> make(String dir);
  }

  /** Runs the sink at parallelism 1 and 3 on the same outputs; returns the two output dirs. */
  File[] bothWays(String name, Factory f) throws Exception {
    List<List<TieredIndexSinkTest.Rec>> parts = new ArrayList<>();
    List<TieredIndexSinkTest.Rec> all = new ArrayList<>();
    for (int s = 0; s < 3; s++) {
      List<TieredIndexSinkTest.Rec> part = stream(40 + s, 4000, 10_000 * MIN + 7 * MIN * s);
      parts.add(part);
      all.addAll(part);
    }
    all.sort((x, y) -> Long.compare(x.ts, y.ts));
    File one = tmp.newFolder(name + "1");
    run(f.make(one.getAbsolutePath()), all);
    File three = tmp.newFolder(name + "3");
    TieredIndexSink<TieredIndexSinkTest.Rec, ?> proto = f.make(three.getAbsolutePath());
    Thread[] ts = new Thread[3];
    final Throwable[] failure = new Throwable[1];
    for (int s = 0; s < 3; s++) {
      final TieredIndexSink<TieredIndexSinkTest.Rec, ?> sink = TieredIndexSinkTest.copy(proto);
      sink.subtask = s;
      sink.parallelism = 3;
      final List<TieredIndexSinkTest.Rec> part = parts.get(s);
      ts[s] = new Thread(() -> {
        try {
          run(sink, part);
        } catch (Throwable t) {
          failure[0] = t;
        }
      });
    }
    for (Thread t : ts)
      t.start();
    for (Thread t : ts)
      t.join();
    if (failure[0] != null)
      throw new AssertionError(failure[0]);
    return new File[] {one, three};
  }

  static String[] row(File f, int i) throws Exception {
    return Files.readAllLines(f.toPath()).get(i).split(",");
  }

  @Test
  public void expiryIsExactInsideTheHorizonAtAnyParallelism() throws Exception {
    File[] d = bothWays("exp", dir -> new ExpirySink<>(TieredIndexSinkTest.TS, TieredIndexSinkTest.HM, 2,
        dir, 2 * 24 * 60 * MIN, 300 * MIN));
    for (File x : d) {
      String[] r = row(new File(x, "expiry_stats.csv"), 1);
      assertEquals("12000", r[1]); // records_total
      assertEquals("0", r[6]); // mismatches
      assertTrue(Long.parseLong(r[2]) < 12000); // something was expired
    }
    assertEquals(row(new File(d[0], "expiry_stats.csv"), 1)[1], row(new File(d[1], "expiry_stats.csv"), 1)[1]);
  }

  @Test
  public void summarizeBaselineIsExactAndErrorsAreBounded() throws Exception {
    File[] d = bothWays("sum", dir -> new SummarizeSink<>(TieredIndexSinkTest.TS, TieredIndexSinkTest.HM, 2,
        dir, 2 * 24 * 60 * MIN, new long[] {60 * MIN, 6 * 60 * MIN}));
    for (File x : d) {
      List<String> rows = Files.readAllLines(new File(x, "summarize.csv").toPath());
      assertEquals(4, rows.size());
      assertEquals("0", rows.get(1).split(",")[0]);
      assertEquals("12000", rows.get(1).split(",")[1]);
      assertEquals("0.000", rows.get(1).split(",")[12]); // baseline error
      for (int i = 2; i < 4; i++)
        assertTrue(Double.parseDouble(rows.get(i).split(",")[11]) < 20.0);
    }
  }

  @Test
  public void queryLengthSweepAgreesAcrossParallelism() throws Exception {
    File[] d = bothWays("len", dir -> new QueryLengthSink<>(TieredIndexSinkTest.TS, TieredIndexSinkTest.HM, 2,
        dir, 100, new long[] {60 * MIN, 600 * MIN}));
    assertEquals(Files.readAllLines(new File(d[0], "query_length_answers.csv").toPath()),
        Files.readAllLines(new File(d[1], "query_length_answers.csv").toPath()));
  }
}
