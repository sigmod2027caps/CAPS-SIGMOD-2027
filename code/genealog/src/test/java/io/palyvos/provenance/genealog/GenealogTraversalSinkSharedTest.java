package io.palyvos.provenance.genealog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.palyvos.provenance.ananke.aggregate.SortedPointersAggregateStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
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

public class GenealogTraversalSinkSharedTest {

  private static final int SHARED_COUNT = 1000;

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void sharedCapturesFirstOccurrenceAndWritesFixture() throws Exception {
    File bench = tmp.newFolder("shared-bench");
    File manifest = new File(tmp.getRoot(), "shared.txt");
    writeGeneratedManifest(manifest, SHARED_COUNT, SHARED_COUNT);

    GenealogTraversalSink<StubTuple> sink = openShared(bench, manifest);
    StubTuple first = contributions(0L, 7);
    sink.invoke(first, null);
    sink.invoke(contributions(0L, 99), null);
    for (int i = 1; i < SHARED_COUNT; i++) {
      sink.invoke(contributions(i, 1), null);
    }
    sink.invoke(contributions(SHARED_COUNT + 5, 8), null);
    sink.close();

    List<String> answers = readLines(new File(bench, "query_answers_genealog.csv"));
    assertEquals("query_id,total_contributions", answers.get(0));
    assertEquals(SHARED_COUNT + 1, answers.size());
    HashMap<Long, Long> rows = parseAnswers(answers);
    assertEquals(SHARED_COUNT, rows.size());
    assertUnsignedSorted(answerIds(answers));
    long firstId = hash(first);
    assertEquals(Long.valueOf(7L), rows.get(Long.valueOf(firstId)));
    assertFalse(rows.containsKey(Long.valueOf(hash(contributions(SHARED_COUNT + 5, 1)))));

    assertFalse(new File(bench, "provenance_query_genealog.csv").exists());
    assertFalse(new File(bench, "query_timings_genealog.csv").exists());

    QueryFixture fixture = QueryFixture.load(
        new File(bench, "query_fixture_genealog.ser").getAbsolutePath(),
        QueryFixture.METHOD_GENEALOG);
    assertEquals(SHARED_COUNT, fixture.ids().length);
    assertUnsignedSorted(fixture.ids());
    SortedPointersAggregateStrategy strategy = new SortedPointersAggregateStrategy();
    long checksum = 0L;
    for (int i = 0; i < fixture.queries().length; i++) {
      GenealogBenchmarkTuple root = (GenealogBenchmarkTuple) fixture.queries()[i];
      long total = GenealogContributionQuery.count(root, strategy);
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
    GenealogTraversalSink<StubTuple> sink = openShared(bench, manifest);
    for (int i = 0; i < SHARED_COUNT - 1; i++) {
      sink.invoke(contributions(i, 1), null);
    }
    try {
      sink.close();
      fail("expected unmatched manifest id");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage(), e.getMessage().toLowerCase(Locale.ROOT).contains("unmatch")
          || e.getMessage().toLowerCase(Locale.ROOT).contains("remaining"));
    }
    assertFalse(new File(bench, "query_answers_genealog.csv").exists());
    assertFalse(new File(bench, "query_fixture_genealog.ser").exists());
  }

  @Test
  public void sharedFailsCloseUnlessManifestTotalIsExactly1000() throws Exception {
    File bench = tmp.newFolder("count-bench");
    File manifest = new File(tmp.getRoot(), "ten.txt");
    writeGeneratedManifest(manifest, 10, 10);
    GenealogTraversalSink<StubTuple> sink = openShared(bench, manifest);
    for (int i = 0; i < 10; i++) {
      sink.invoke(contributions(i, 1), null);
    }
    try {
      sink.close();
      fail("expected exact-1000 enforcement");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("1000"));
    }
  }

  @Test
  public void sharedIgnoresTraversalCalibrateAndDoesNotSample() throws Exception {
    File bench = tmp.newFolder("calibrate-shared");
    File manifest = new File(tmp.getRoot(), "calibrate-shared.txt");
    writeGeneratedManifest(manifest, SHARED_COUNT, SHARED_COUNT);
    ExperimentSettings settings = ExperimentSettings.newInstance(new String[] {
        "--statisticsFolder", tmp.getRoot().getAbsolutePath(),
        "--outputFile", "out",
        "--sourcesNumber", "1",
        "--traversalBenchDir", bench.getAbsolutePath(),
        "--traversalQueryIds", manifest.getAbsolutePath(),
        "--traversalCalibrate",
        "--traversalSampleProb", "0.0"
    });
    GenealogTraversalSink<StubTuple> sink =
        new GenealogTraversalSink<StubTuple>(settings, StubTuple.KEY);
    sink.open(new Configuration());
    for (int i = 0; i < SHARED_COUNT; i++) {
      sink.invoke(contributions(i, 1), null);
    }
    sink.close();
    assertTrue(new File(bench, "query_answers_genealog.csv").isFile());
    assertTrue(new File(bench, "query_fixture_genealog.ser").isFile());
    assertFalse(new File(bench, "provenance_query_genealog.csv").exists());
  }

  @Test
  public void oldTimingModeKeepsOldCsvSchema() throws Exception {
    File bench = tmp.newFolder("old-bench");
    ExperimentSettings settings = ExperimentSettings.newInstance(new String[] {
        "--statisticsFolder", tmp.getRoot().getAbsolutePath(),
        "--outputFile", "out",
        "--sourcesNumber", "1",
        "--traversalBenchDir", bench.getAbsolutePath(),
        "--traversalSampleProb", "1.0"
    });
    GenealogTraversalSink<StubTuple> sink =
        new GenealogTraversalSink<StubTuple>(settings, StubTuple.KEY);
    sink.open(new Configuration());
    sink.invoke(contributions(1L, 2), null);
    sink.close();
    List<String> query = readLines(new File(bench, "provenance_query_genealog.csv"));
    assertEquals("method,outputs,samples,avg_ns,total_ms,checksum,"
        + "query_semantics,predicate_checksum,unique_selected", query.get(0));
    assertEquals("genealog", query.get(1).split(",", -1)[0]);
    assertFalse(new File(bench, "query_answers_genealog.csv").exists());
    assertFalse(new File(bench, "query_fixture_genealog.ser").exists());
  }

  @Test
  public void oldCalibrationModeKeepsTimerMethodName() throws Exception {
    File bench = tmp.newFolder("cal-bench");
    ExperimentSettings settings = ExperimentSettings.newInstance(new String[] {
        "--statisticsFolder", tmp.getRoot().getAbsolutePath(),
        "--outputFile", "out",
        "--sourcesNumber", "1",
        "--traversalBenchDir", bench.getAbsolutePath(),
        "--traversalSampleProb", "1.0",
        "--traversalCalibrate"
    });
    GenealogTraversalSink<StubTuple> sink =
        new GenealogTraversalSink<StubTuple>(settings, StubTuple.KEY);
    sink.open(new Configuration());
    sink.invoke(contributions(1L, 1), null);
    sink.close();
    List<String> query = readLines(new File(bench, "provenance_query_genealog.csv"));
    assertEquals("method,outputs,samples,avg_ns,total_ms,checksum,"
        + "query_semantics,predicate_checksum,unique_selected", query.get(0));
    assertEquals("genealog_timer", query.get(1).split(",", -1)[0]);
    assertFalse(new File(bench, "query_answers_genealog.csv").exists());
    assertFalse(new File(bench, "query_fixture_genealog.ser").exists());
  }

  @Test
  public void oldVerificationModeWritesAnswersNotSharedCsv() throws Exception {
    File bench = tmp.newFolder("verify-bench");
    ExperimentSettings settings = ExperimentSettings.newInstance(new String[] {
        "--statisticsFolder", tmp.getRoot().getAbsolutePath(),
        "--outputFile", "out",
        "--sourcesNumber", "1",
        "--traversalBenchDir", bench.getAbsolutePath(),
        "--traversalVerify"
    });
    GenealogTraversalSink<StubTuple> sink =
        new GenealogTraversalSink<StubTuple>(settings, StubTuple.KEY);
    sink.open(new Configuration());
    sink.invoke(contributions(3L, 1), null);
    sink.close();
    assertTrue(new File(bench, "provenance_answers_genealog.csv").isFile());
    assertFalse(new File(bench, "query_answers_genealog.csv").exists());
    assertFalse(new File(bench, "query_fixture_genealog.ser").exists());
    List<String> answers = readLines(new File(bench, "provenance_answers_genealog.csv"));
    assertEquals("ts,group_count,vector_hi,vector_lo,payload_hi,payload_lo,total_hi,total_lo,semantics",
        answers.get(0));
  }

  private GenealogTraversalSink<StubTuple> openShared(File bench, File manifest) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(new String[] {
        "--statisticsFolder", tmp.getRoot().getAbsolutePath(),
        "--outputFile", "out",
        "--sourcesNumber", "1",
        "--traversalBenchDir", bench.getAbsolutePath(),
        "--traversalQueryIds", manifest.getAbsolutePath()
    });
    GenealogTraversalSink<StubTuple> sink =
        new GenealogTraversalSink<StubTuple>(settings, StubTuple.KEY);
    sink.open(new Configuration());
    return sink;
  }

  static void writeGeneratedManifest(File path, int observe, int keep) throws Exception {
    QueryIdManifest.Generator gen = new QueryIdManifest.Generator(keep, 42L);
    LogicalSampler sampler = new LogicalSampler();
    for (int i = 0; i < observe; i++) {
      gen.observe(sampler.hashKey(StubTuple.KEY, contributions(i, 1)));
    }
    gen.write(path.getAbsolutePath());
  }

  static long hash(StubTuple tuple) {
    return new LogicalSampler().hashKey(StubTuple.KEY, tuple);
  }

  static StubTuple contributions(long id, int n) {
    if (n <= 1) {
      return source(id, id);
    }
    StubTuple acc = source(id, id);
    for (int i = 1; i < n; i++) {
      acc = join(id, id, acc, source(id, id + i));
    }
    return acc;
  }

  private static StubTuple source(long id, long ts) {
    StubTuple t = new StubTuple(id, ts);
    t.initGenealog(GenealogTupleType.SOURCE);
    return t;
  }

  private static StubTuple join(long id, long ts, StubTuple left, StubTuple right) {
    StubTuple t = new StubTuple(id, ts);
    t.initGenealog(GenealogTupleType.JOIN);
    t.setU1(left);
    t.setU2(right);
    return t;
  }

  private static HashMap<Long, Long> parseAnswers(List<String> lines) {
    HashMap<Long, Long> rows = new HashMap<Long, Long>();
    for (int i = 1; i < lines.size(); i++) {
      String[] parts = lines.get(i).split(",", -1);
      assertEquals(2, parts.length);
      Long id = Long.valueOf(Long.parseLong(parts[0]));
      assertTrue("duplicate answer id " + id,
          rows.put(id, Long.valueOf(Long.parseLong(parts[1]))) == null);
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

  static List<String> readLines(File path) throws Exception {
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

  static final class StubTuple implements GenealogTuple {
    final long id;
    private final long timestamp;
    private GenealogData gdata;

    StubTuple(long id, long timestamp) {
      this.id = id;
      this.timestamp = timestamp;
    }

    static final SampleKeyFn<StubTuple> KEY = new SampleKeyFn<StubTuple>() {
      @Override
      public void writeKey(StubTuple value, java.io.DataOutput out) throws java.io.IOException {
        out.writeLong(value.id);
      }
    };

    @Override
    public void initGenealog(GenealogTupleType tupleType) {
      gdata = new GenealogData();
      gdata.init(tupleType);
    }

    @Override
    public GenealogData getGenealogData() {
      return gdata;
    }

    @Override
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public void setTimestamp(long timestamp) {
    }

    @Override
    public long getStimulus() {
      return 0;
    }

    @Override
    public void setStimulus(long stimulus) {
    }
  }
}
