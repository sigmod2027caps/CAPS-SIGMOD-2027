package ink;

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

// The Green index sink driven by hand, the way Flink drives it: records (bounded disorder), watermarks, close.
// The polynomial of an output is the product of its source variables (source seq * channels + channel); the
// expected answer of a query is counted from the sources directly, independent of Polynomial and InkMeta.
public class GreenIndexingSinkTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  static final int CHANNELS = 3;

  static final class Rec {
    final long ts;
    final long[] counts; // expected how-much vector (sources per channel, with multiplicity)
    final InkMeta meta;

    Rec(long ts, long[] counts, InkMeta meta) {
      this.ts = ts;
      this.counts = counts;
      this.meta = meta;
    }
  }

  static final InkQuerySink.TsFn<Rec> TS = r -> r.ts;
  static final InkQuerySink.MetaFn<Rec> META = r -> r.meta;

  /** Outputs of one subtask: 0..4 sources each (a source can repeat: exponent 2), arriving up to 20 ms out of order. */
  static List<Rec> stream(long seed, int n, long base, int[] nextSeq) {
    Random rnd = new Random(seed);
    List<Rec> out = new ArrayList<>();
    long clock = base;
    for (int i = 0; i < n; i++) {
      clock += 1 + rnd.nextInt(5);
      long[] counts = new long[CHANNELS];
      InkMeta meta = InkMeta.one();
      int k = rnd.nextInt(5);
      InkMeta previous = null;
      for (int j = 0; j < k; j++) {
        InkMeta src;
        if (previous != null && rnd.nextInt(4) == 0)
          src = previous; // the same source tuple twice in the product
        else
          src = InkMeta.source(rnd.nextInt(CHANNELS), CHANNELS, nextSeq[0]++);
        previous = src;
        meta = meta.times(src);
        counts[(src.poly.monomials[0].vars[0]) % CHANNELS]++;
      }
      out.add(new Rec(clock - rnd.nextInt(20), counts, meta));
    }
    return out;
  }

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

  static void run(GreenIndexingSink<Rec> sink, List<Rec> recs) throws Exception {
    sink.open(new Configuration());
    long clock = Long.MIN_VALUE;
    for (Rec r : recs) {
      sink.invoke(r, null);
      clock = Math.max(clock, r.ts);
      sink.accept(new Watermark(clock - 21)); // every record precedes the watermark that certifies it
    }
    sink.accept(new Watermark(Long.MAX_VALUE));
    sink.close();
  }

  static List<String> lines(File f) throws Exception {
    return Files.readAllLines(f.toPath());
  }

  /** Every row of an answer file against a brute-force sum of the expected vectors. */
  static int checkAnswers(File file, List<Rec> all) throws Exception {
    List<String> rows = lines(file);
    String[] header = rows.get(0).split(",");
    assertEquals("i,from,to", header[1] + "," + header[2] + "," + header[3]);
    int definedRows = 0;
    for (int r = 1; r < rows.size(); r++) {
      String[] f = rows.get(r).split(",");
      long from = Long.parseLong(f[2]), to = Long.parseLong(f[3]);
      long[] expected = new long[CHANNELS];
      int n = 0;
      for (Rec x : all)
        if (x.ts >= from && x.ts <= to) {
          n++;
          for (int c = 0; c < CHANNELS; c++)
            expected[c] += x.counts[c];
        }
      for (int c = 0; c < CHANNELS; c++)
        assertEquals("row " + r + " channel " + c, expected[c], Long.parseLong(f[4 + c]));
      assertEquals(n > 0 ? "1" : "0", f[4 + CHANNELS]);
      assertEquals("1", f[5 + CHANNELS]);
      definedRows += n > 0 ? 1 : 0;
    }
    return definedRows;
  }

  @Test
  public void fixedWindowMatchesBruteForce() throws Exception {
    File dir = tmp.newFolder("fixed");
    List<Rec> recs = stream(1, 20000, 1000, new int[1]);
    GreenIndexingSink<Rec> sink = new GreenIndexingSink<>(TS, META, CHANNELS, dir.getAbsolutePath(), 200, 500);
    run(sink, recs);
    int defined = checkAnswers(new File(dir, "query_answers_green.csv"), recs);
    assertTrue("non-trivial windows: " + defined, defined > 300);
    assertEquals(601, lines(new File(dir, "query_answers_green.csv")).size());
    List<String> lat = lines(new File(dir, "query_latency_green.csv"));
    assertTrue(lat.get(1).startsWith("green,howmuch,20000,200,"));
    assertTrue(lat.get(2).startsWith("green,versioning,20000,200,"));
    String[] stats = lines(new File(dir, "tiered_stats_green.csv")).get(1).split(",");
    assertEquals("20000", stats[3]); // records
    assertEquals("0", stats[5]); // late
    assertEquals("20000", stats[6]); // migrated
    assertEquals("0", stats[8]); // Tier 1 empty at the end
    long bytes = 0;
    for (Rec r : recs)
      bytes += r.meta.metadataBytes();
    assertEquals("20000," + recs.stream().mapToLong(r -> r.meta.totalVars()).sum() + "," + bytes,
        lines(new File(dir, "index_stats_green.csv")).get(1));
    assertFalse(new File(dir, "late_green_s0.csv").exists());
  }

  @Test
  public void lengthSweepMatchesBruteForce() throws Exception {
    File dir = tmp.newFolder("sweep");
    List<Rec> recs = stream(2, 10000, 1000, new int[1]);
    GreenIndexingSink<Rec> sink = new GreenIndexingSink<>(TS, META, CHANNELS, dir.getAbsolutePath(), 100,
        new long[] {50, 500, 5000});
    run(sink, recs);
    checkAnswers(new File(dir, "query_length_answers_green.csv"), recs);
    List<String> lat = lines(new File(dir, "query_length_latency_green.csv"));
    assertEquals(4, lat.size());
    assertTrue(lat.get(3).startsWith("green,5000,10000,100,"));
  }

  @Test
  public void fourSubtasksGiveTheAnswersOfAll() throws Exception {
    File dir = tmp.newFolder("four");
    int[] seq = new int[1];
    List<List<Rec>> parts = new ArrayList<>();
    List<Rec> all = new ArrayList<>();
    for (int s = 0; s < 4; s++) {
      List<Rec> part = stream(10 + s, 5000, 1000 + 300L * s, seq); // skewed clocks per subtask
      parts.add(part);
      all.addAll(part);
    }
    GreenIndexingSink<Rec> proto = new GreenIndexingSink<>(TS, META, CHANNELS, dir.getAbsolutePath(), 300, 800);
    Thread[] threads = new Thread[4];
    final Throwable[] failure = new Throwable[1];
    for (int s = 0; s < 4; s++) {
      final GreenIndexingSink<Rec> sink = copy(proto);
      final int subtask = s;
      threads[s] = new Thread(() -> {
        try {
          sink.subtask = subtask;
          sink.parallelism = 4;
          run(sink, parts.get(subtask));
        } catch (Throwable t) {
          failure[0] = t;
        }
      });
      threads[s].start();
    }
    for (Thread t : threads)
      t.join();
    if (failure[0] != null)
      throw new AssertionError(failure[0]);
    checkAnswers(new File(dir, "query_answers_green.csv"), all);
    assertEquals(5, lines(new File(dir, "tiered_stats_green.csv")).size());
  }

  @Test
  public void lateRecordsAreCountedAndNotIndexed() throws Exception {
    File dir = tmp.newFolder("late");
    GreenIndexingSink<Rec> sink = new GreenIndexingSink<>(TS, META, CHANNELS, dir.getAbsolutePath(), 10, 100);
    sink.open(new Configuration());
    int[] seq = new int[1];
    List<Rec> recs = stream(3, 500, 1000, seq);
    for (Rec r : recs)
      sink.invoke(r, null);
    sink.accept(new Watermark(recs.get(0).ts + 50));
    sink.invoke(new Rec(recs.get(0).ts, new long[CHANNELS], InkMeta.one()), null); // at or below the watermark
    sink.accept(new Watermark(Long.MAX_VALUE));
    sink.close();
    assertTrue(new File(dir, "late_green_s0.csv").exists());
    String[] stats = lines(new File(dir, "tiered_stats_green.csv")).get(1).split(",");
    assertEquals("501", stats[3]);
    assertEquals("500", stats[4]);
    assertEquals("1", stats[5]);
  }
}
