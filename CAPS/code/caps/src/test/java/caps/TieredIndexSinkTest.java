package caps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

// The index sinks driven by hand, the way Flink drives them: records, then watermarks, then close; one
// serialized copy per subtask; shards meet in ShardGroup.
public class TieredIndexSinkTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  static final class Rec {
    final long ts;
    final long[] v;

    Rec(long ts, long... v) {
      this.ts = ts;
      this.v = v;
    }
  }

  static final IndexingSink.TsFn<Rec> TS = r -> r.ts;
  static final IndexingSink.HmFn<Rec> HM = r -> HowMuch.of(r.v);

  @SuppressWarnings("unchecked")
  static <S> S copy(S sink) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(sink);
    }
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (S) in.readObject();
    }
  }

  /** Records of one subtask, in the arrival order Flink would produce: each preceded by a watermark that lags it. */
  static List<Rec> stream(long seed, int n, int channels, long base) {
    Random rnd = new Random(seed);
    List<Rec> out = new ArrayList<>();
    long clock = base;
    for (int i = 0; i < n; i++) {
      clock += 1 + rnd.nextInt(5);
      long[] v = new long[channels];
      for (int c = 0; c < channels; c++)
        v[c] = rnd.nextInt(4);
      out.add(new Rec(clock - rnd.nextInt(20), v)); // bounded disorder, ts > watermark below
    }
    return out;
  }

  /** Feeds a subtask: watermark(clock - 21) after each record (so every record precedes its watermark). */
  static void run(IndexingSink<Rec> sink, List<Rec> recs) throws Exception {
    sink.open(new Configuration());
    long clock = Long.MIN_VALUE;
    for (Rec r : recs) {
      sink.invoke(r, null);
      clock = Math.max(clock, r.ts);
      if (clock > Long.MIN_VALUE + 100)
        sink.accept(new Watermark(clock - 20 - 1 + 0));
    }
    sink.accept(new Watermark(Long.MAX_VALUE));
    sink.close();
  }

  static List<String> lines(File f) throws Exception {
    return Files.readAllLines(f.toPath());
  }

  @Test
  public void singleSubtaskMatchesBruteForceAndWritesStats() throws Exception {
    File dir = tmp.newFolder("one");
    List<Rec> recs = stream(1, 20000, 3, 1000);
    IndexingSink<Rec> sink = new IndexingSink<>(TS, HM, 3, dir.getAbsolutePath(), 200, 500);
    sink.expectedOutputs = 20000;
    run(sink, recs);

    checkAnswers(new File(dir, "query_answers.csv"), recs, 3);
    List<String> stats = lines(new File(dir, "tiered_stats.csv"));
    assertEquals(2, stats.size());
    String[] f = stats.get(1).split(",");
    assertEquals("20000", f[3]); // records
    assertEquals("0", f[5]); // late
    assertEquals("20000", f[6]); // migrated
    assertEquals("0", f[8]); // tier 1 empty at the end
    assertTrue(Long.parseLong(f[14]) < 0); // every arrival precedes its watermark
    List<String> prov = lines(new File(dir, "provisional_queries.csv"));
    assertTrue("provisional batches ran: " + prov, prov.size() > 5);
    for (int i = 1; i < prov.size(); i++) {
      String[] g = prov.get(i).split(",");
      assertEquals(g[6], g[8]); // all queries provisional
      assertEquals("0", g[10]); // none exceeded its final answer
    }
    List<String> q = lines(new File(dir, "tiered_queries.csv"));
    assertTrue(q.get(1).startsWith("howmuch,200,200,0,"));
    assertFalse(new File(dir, "late_index_s0.csv").exists());
  }

  @Test
  public void fourSubtasksGiveTheAnswersOfOne() throws Exception {
    File one = tmp.newFolder("p1");
    File four = tmp.newFolder("p4");
    int channels = 2;
    // the same outputs, split across four subtasks with different clocks (skewed watermarks)
    List<List<Rec>> parts = new ArrayList<>();
    List<Rec> all = new ArrayList<>();
    for (int s = 0; s < 4; s++) {
      List<Rec> part = stream(10 + s, 5000, channels, 1000 + 300L * s);
      parts.add(part);
      all.addAll(part);
    }
    IndexingSink<Rec> ref = new IndexingSink<>(TS, HM, channels, one.getAbsolutePath(), 300, 800);
    List<Rec> sorted = new ArrayList<>(all);
    sorted.sort((x, y) -> Long.compare(x.ts, y.ts));
    run(ref, sorted);

    IndexingSink<Rec> proto = new IndexingSink<>(TS, HM, channels, four.getAbsolutePath(), 300, 800);
    Thread[] threads = new Thread[4];
    final Throwable[] failure = new Throwable[1];
    for (int s = 0; s < 4; s++) {
      final IndexingSink<Rec> sink = copy(proto);
      sink.subtask = s;
      sink.parallelism = 4;
      final List<Rec> part = parts.get(s);
      threads[s] = new Thread(() -> {
        try {
          run(sink, part);
        } catch (Throwable t) {
          failure[0] = t;
        }
      });
    }
    for (Thread t : threads)
      t.start();
    for (Thread t : threads)
      t.join();
    if (failure[0] != null)
      throw new AssertionError(failure[0]);

    // min/max over the union differ from the single-sink run only if a shard lost a record; the query
    // ranges are derived from them, so the answer files are identical.
    assertEquals(lines(new File(one, "query_answers.csv")), lines(new File(four, "query_answers.csv")));
    checkAnswers(new File(four, "query_answers.csv"), all, channels);
    assertEquals(5, lines(new File(four, "tiered_stats.csv")).size()); // header + 4 shards
  }

  @Test
  public void lateRecordsAreCountedAndWrittenNotIndexed() throws Exception {
    File dir = tmp.newFolder("late");
    IndexingSink<Rec> sink = new IndexingSink<>(TS, HM, 1, dir.getAbsolutePath(), 5, 10);
    sink.open(new Configuration());
    sink.invoke(new Rec(100, 1), null);
    sink.invoke(new Rec(120, 1), null);
    sink.accept(new Watermark(110));
    sink.invoke(new Rec(105, 7), null); // late: 105 <= 110
    sink.invoke(new Rec(110, 7), null); // late: ts <= W
    sink.invoke(new Rec(111, 1), null);
    sink.accept(new Watermark(Long.MAX_VALUE));
    sink.close();
    String[] f = lines(new File(dir, "tiered_stats.csv")).get(1).split(",");
    assertEquals("5", f[3]);
    assertEquals("3", f[4]); // indexed: 100, 120, 111 ... 
    assertEquals("2", f[5]); // late
    assertEquals(3, Long.parseLong(f[6]));
    List<String> late = lines(new File(dir, "late_index_s0.csv"));
    assertEquals(3, late.size());
    assertEquals("105,110,7", late.get(1));
  }

  @Test
  public void windowsInAGapAreUndefinedNotZero() throws Exception {
    File dir = tmp.newFolder("gap");
    List<Rec> recs = new ArrayList<>();
    for (int i = 0; i < 300; i++)
      recs.add(new Rec(1000 + i, 1, 2)); // an early burst
    for (int i = 0; i < 300; i++)
      recs.add(new Rec(100_000 + i, 3, 0)); // a late burst, after a gap of 99 s
    IndexingSink<Rec> sink = new IndexingSink<>(TS, HM, 2, dir.getAbsolutePath(), 400, 150);
    run(sink, recs);
    List<String> rows = lines(new File(dir, "query_answers.csv"));
    int undefined = 0;
    for (int i = 1; i < rows.size(); i++) {
      String[] c = rows.get(i).split(",");
      if (c[6].equals("0")) {
        undefined++;
        assertEquals("0", c[4]); // the counts of an undefined answer are zero, but the answer is bottom
        assertEquals("0", c[5]);
      }
    }
    assertTrue("windows inside the gap exist", undefined > 100);
    String[] q = lines(new File(dir, "tiered_queries.csv")).get(1).split(",");
    assertEquals(Integer.toString(undefined >= 0 ? Integer.parseInt(q[5]) : 0), q[5]);
    assertTrue(Integer.parseInt(q[5]) > 0);
    checkAnswers(new File(dir, "query_answers.csv"), recs, 2);
  }

  @Test
  public void emptyShardsAreFineIfSomeOtherShardHasRecords() throws Exception {
    File dir = tmp.newFolder("empty");
    IndexingSink<Rec> proto = new IndexingSink<>(TS, HM, 1, dir.getAbsolutePath(), 10, 100);
    IndexingSink<Rec> a = copy(proto);
    IndexingSink<Rec> b = copy(proto);
    a.subtask = 0; a.parallelism = 2;
    b.subtask = 1; b.parallelism = 2;
    run(a, stream(3, 500, 1, 0));
    run(b, new ArrayList<Rec>()); // closes last, with nothing: the merge must still run
    assertTrue(new File(dir, "query_answers.csv").isFile());
  }

  @Test
  public void allShardsEmptyFails() throws Exception {
    File dir = tmp.newFolder("allempty");
    IndexingSink<Rec> sink = new IndexingSink<>(TS, HM, 1, dir.getAbsolutePath(), 10, 100);
    try {
      run(sink, new ArrayList<Rec>());
      org.junit.Assert.fail("expected failure");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("no records"));
    }
  }

  /** Every row of the answer file against a brute-force sum over the records. */
  static void checkAnswers(File f, List<Rec> recs, int channels) throws Exception {
    List<String> rows = lines(f);
    assertTrue(rows.size() > 1);
    for (int i = 1; i < rows.size(); i++) {
      String[] c = rows.get(i).split(",");
      long from = Long.parseLong(c[2]);
      long to = Long.parseLong(c[3]);
      long[] sum = new long[channels];
      int n = 0;
      for (Rec r : recs)
        if (r.ts >= from && r.ts <= to) {
          n++;
          for (int k = 0; k < channels; k++)
            sum[k] += r.v[k];
        }
      for (int k = 0; k < channels; k++)
        assertEquals(rows.get(i), sum[k], Long.parseLong(c[4 + k]));
      assertEquals(rows.get(i), n > 0 ? "1" : "0", c[4 + channels]);
      assertEquals("1", c[5 + channels]);
    }
  }
}
