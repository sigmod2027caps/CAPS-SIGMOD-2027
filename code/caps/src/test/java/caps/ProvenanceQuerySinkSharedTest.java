package caps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import org.apache.flink.configuration.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import temporalindex.LogicalSampler;
import temporalindex.QueryFixture;
import temporalindex.QueryIdManifest;
import temporalindex.SampleKeyFn;

public class ProvenanceQuerySinkSharedTest {

  private static final int SHARED_COUNT = 1000;

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void generateIdsWritesManifestAndDoesNotQuery() throws Exception {
    File bench = tmp.newFolder("gen-bench");
    File manifest = new File(tmp.getRoot(), "generated.txt");
    ProvenanceQuerySink<Rec> sink = (ProvenanceQuerySink<Rec>) BenchSinks.create(
        new String[] {"provenance_ids", bench.getAbsolutePath(), manifest.getAbsolutePath(), "8"},
        0, Rec.TS, Rec.HM, Rec.KEY, 2, 0L);
    sink.open(new Configuration());
    LogicalSampler sampler = new LogicalSampler();
    QueryIdManifest.Generator expected = new QueryIdManifest.Generator(8, 42L);
    for (int i = 0; i < 20; i++) {
      Rec rec = Rec.of(i % 12, HowMuch.of(i + 1, 0));
      expected.observe(sampler.hashKey(Rec.KEY, rec));
      sink.invoke(rec, null);
    }
    sink.close();

    assertTrue(manifest.isFile());
    assertFalse(new File(bench, "provenance_query.csv").exists());
    assertFalse(new File(bench, "query_answers_caps.csv").exists());
    assertFalse(new File(bench, "query_fixture_caps.ser").exists());

    File oracle = new File(tmp.getRoot(), "oracle.txt");
    expected.write(oracle.getAbsolutePath());
    assertEquals(readLines(oracle), readLines(manifest));
  }

  @Test
  public void sharedCapturesFirstOccurrenceAndWritesFixture() throws Exception {
    File bench = tmp.newFolder("shared-bench");
    File manifest = new File(tmp.getRoot(), "shared.txt");
    writeGeneratedManifest(manifest, SHARED_COUNT, SHARED_COUNT);

    ProvenanceQuerySink<Rec> sink = openShared(bench, manifest);
    Rec first = Rec.of(0, HowMuch.of(3, 4));
    sink.invoke(first, null);
    first.hm.counts[0] = 90L;
    sink.invoke(Rec.of(0, HowMuch.of(90, 9)), null);
    for (int i = 1; i < SHARED_COUNT; i++) {
      sink.invoke(Rec.of(i, HowMuch.of(1, 0)), null);
    }
    sink.invoke(Rec.of(SHARED_COUNT + 5, HowMuch.of(8, 8)), null);
    sink.close();

    List<String> answers = readLines(new File(bench, "query_answers_caps.csv"));
    assertEquals("query_id,total_contributions", answers.get(0));
    assertEquals(SHARED_COUNT + 1, answers.size());
    HashMap<Long, Long> rows = parseAnswers(answers);
    assertEquals(SHARED_COUNT, rows.size());
    assertUnsignedSorted(answerIds(answers));
    long firstId = hash(first);
    assertEquals(Long.valueOf(7L), rows.get(Long.valueOf(firstId)));
    assertFalse(rows.containsKey(Long.valueOf(hash(Rec.of(SHARED_COUNT + 5, HowMuch.of(1))))));

    assertFalse(new File(bench, "provenance_query.csv").exists());
    assertFalse(new File(bench, "query_timings_caps.csv").exists());

    QueryFixture fixture = QueryFixture.load(
        new File(bench, "query_fixture_caps.ser").getAbsolutePath(), QueryFixture.METHOD_CAPS);
    assertEquals(2, fixture.channels());
    assertEquals(SHARED_COUNT, fixture.ids().length);
    assertUnsignedSorted(fixture.ids());
    long checksum = 0L;
    for (int i = 0; i < fixture.queries().length; i++) {
      HowMuch hm = (HowMuch) fixture.queries()[i];
      long total = hm.total();
      checksum += total;
      assertEquals(rows.get(Long.valueOf(fixture.ids()[i])).longValue(), total);
    }
    assertEquals(7L + (SHARED_COUNT - 1), checksum);
  }

  @Test
  public void sharedFailsCloseWhenManifestIdIsMissing() throws Exception {
    File bench = tmp.newFolder("missing-bench");
    File manifest = new File(tmp.getRoot(), "missing.txt");
    writeGeneratedManifest(manifest, SHARED_COUNT, SHARED_COUNT);
    ProvenanceQuerySink<Rec> sink = openShared(bench, manifest);
    for (int i = 0; i < SHARED_COUNT - 1; i++) {
      sink.invoke(Rec.of(i, HowMuch.of(1)), null);
    }
    try {
      sink.close();
      fail("expected unmatched manifest id");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage(), e.getMessage().toLowerCase(Locale.ROOT).contains("unmatch")
          || e.getMessage().toLowerCase(Locale.ROOT).contains("remaining"));
    }
    assertFalse(new File(bench, "query_answers_caps.csv").exists());
    assertFalse(new File(bench, "query_fixture_caps.ser").exists());
  }

  @Test
  public void sharedFailsCloseUnlessManifestTotalIsExactly1000() throws Exception {
    File bench = tmp.newFolder("count-bench");
    File manifest = new File(tmp.getRoot(), "ten.txt");
    writeGeneratedManifest(manifest, 10, 10);
    ProvenanceQuerySink<Rec> sink = openShared(bench, manifest);
    for (int i = 0; i < 10; i++) {
      sink.invoke(Rec.of(i, HowMuch.of(1)), null);
    }
    try {
      sink.close();
      fail("expected exact-1000 enforcement");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("1000"));
    }
  }

  @Test
  public void oldProvenanceModeKeepsOldCsvSchema() throws Exception {
    File bench = tmp.newFolder("old-bench");
    ProvenanceQuerySink<Rec> sink = (ProvenanceQuerySink<Rec>) BenchSinks.create(
        new String[] {"provenance", bench.getAbsolutePath(), "1.0"},
        0, Rec.TS, Rec.HM, Rec.KEY, 2, 0L);
    sink.open(new Configuration());
    sink.invoke(Rec.of(1, HowMuch.of(2, 3)), null);
    sink.close();
    List<String> query = readLines(new File(bench, "provenance_query.csv"));
    assertEquals("method,outputs,samples,avg_ns,total_ms,checksum,"
        + "query_semantics,predicate_checksum,unique_selected", query.get(0));
    assertFalse(new File(bench, "query_answers_caps.csv").exists());
    assertFalse(new File(bench, "query_fixture_caps.ser").exists());
  }

  private ProvenanceQuerySink<Rec> openShared(File bench, File manifest) throws Exception {
    ProvenanceQuerySink<Rec> sink = (ProvenanceQuerySink<Rec>) BenchSinks.create(
        new String[] {"provenance_shared", bench.getAbsolutePath(), manifest.getAbsolutePath()},
        0, Rec.TS, Rec.HM, Rec.KEY, 2, 0L);
    sink.open(new Configuration());
    return sink;
  }

  private static void writeGeneratedManifest(File path, int observe, int keep) throws Exception {
    QueryIdManifest.Generator gen = new QueryIdManifest.Generator(keep, 42L);
    LogicalSampler sampler = new LogicalSampler();
    for (int i = 0; i < observe; i++) {
      gen.observe(sampler.hashKey(Rec.KEY, Rec.of(i, HowMuch.of(1))));
    }
    gen.write(path.getAbsolutePath());
  }

  private static long hash(Rec rec) {
    return new LogicalSampler().hashKey(Rec.KEY, rec);
  }

  private static HashMap<Long, Long> parseAnswers(List<String> lines) {
    HashMap<Long, Long> rows = new HashMap<Long, Long>();
    for (int i = 1; i < lines.size(); i++) {
      String[] parts = lines.get(i).split(",", -1);
      assertEquals(2, parts.length);
      Long id = Long.valueOf(Long.parseLong(parts[0]));
      assertTrue("duplicate answer id " + id, rows.put(id, Long.valueOf(Long.parseLong(parts[1]))) == null);
    }
    return rows;
  }

  private static long[] answerIds(List<String> lines) {
    long[] ids = new long[lines.size() - 1];
    for (int i = 1; i < lines.size(); i++) {
      ids[i - 1] = Long.parseLong(lines.get(i).split(",", -1)[0]);
    }
    return ids;
  }

  private static void assertUnsignedSorted(long[] ids) {
    HashSet<Long> seen = new HashSet<Long>();
    for (int i = 0; i < ids.length; i++) {
      assertTrue(seen.add(Long.valueOf(ids[i])));
      if (i > 0) {
        assertTrue(Long.compareUnsigned(ids[i - 1], ids[i]) < 0);
      }
    }
  }

  private static List<String> readLines(File path) throws Exception {
    ArrayList<String> lines = new ArrayList<String>();
    BufferedReader in = new BufferedReader(
        new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8));
    try {
      String line;
      while ((line = in.readLine()) != null) {
        lines.add(line);
      }
    } finally {
      in.close();
    }
    return lines;
  }

  static final class Rec {
    final long id;
    final long ts;
    final HowMuch hm;

    Rec(long id, long ts, HowMuch hm) {
      this.id = id;
      this.ts = ts;
      this.hm = hm;
    }

    static Rec of(long id, HowMuch hm) {
      return new Rec(id, id, hm);
    }

    static final IndexingSink.TsFn<Rec> TS = new IndexingSink.TsFn<Rec>() {
      @Override
      public long ts(Rec tuple) {
        return tuple.ts;
      }
    };
    static final IndexingSink.HmFn<Rec> HM = new IndexingSink.HmFn<Rec>() {
      @Override
      public HowMuch hm(Rec tuple) {
        return tuple.hm;
      }
    };
    static final SampleKeyFn<Rec> KEY = new SampleKeyFn<Rec>() {
      @Override
      public void writeKey(Rec value, java.io.DataOutput out) throws java.io.IOException {
        out.writeLong(value.id);
      }
    };
  }
}
