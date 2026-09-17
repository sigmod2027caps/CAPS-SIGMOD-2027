package queryjmh;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import caps.HowMuch;
import ink.InkMeta;
import io.palyvos.provenance.ananke.aggregate.SortedPointersAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogBenchmarkTuple;
import io.palyvos.provenance.genealog.GenealogContributionQuery;
import io.palyvos.provenance.genealog.GenealogTupleType;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import temporalindex.QueryFixture;

public class QueryWorkloadTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void capsWorkloadChecksumMatchesHowMuchTotals() throws Exception {
    long[] ids = sequentialIds();
    Object[] queries = new Object[QueryFixture.REQUIRED_COUNT];
    long expected = 0L;
    for (int i = 0; i < queries.length; i++) {
      HowMuch hm = HowMuch.of(i + 1L, 2L);
      queries[i] = hm;
      expected += hm.total();
    }
    File path = tmp.newFile("caps.ser");
    QueryFixture.of(QueryFixture.METHOD_CAPS, 2, ids, queries).write(path.getAbsolutePath());
    QueryWorkload workload = QueryWorkload.load(QueryFixture.METHOD_CAPS, path.getAbsolutePath());
    assertEquals(QueryFixture.REQUIRED_COUNT, workload.queryCount());
    assertEquals(expected, workload.runAll());
    assertEquals(idChecksum(ids), workload.idChecksum());
  }

  @Test
  public void greenWorkloadChecksumMatchesHowMuchTotal() throws Exception {
    long[] ids = sequentialIds();
    Object[] queries = new Object[QueryFixture.REQUIRED_COUNT];
    long expected = 0L;
    for (int i = 0; i < queries.length; i++) {
      InkMeta meta = InkMeta.source(0, 2, i);
      queries[i] = meta;
      expected += meta.howMuchTotal(2);
    }
    File path = tmp.newFile("green.ser");
    QueryFixture.of(QueryFixture.METHOD_GREEN, 2, ids, queries).write(path.getAbsolutePath());
    QueryWorkload workload = QueryWorkload.load(QueryFixture.METHOD_GREEN, path.getAbsolutePath());
    assertEquals(QueryFixture.REQUIRED_COUNT, workload.queryCount());
    assertEquals(expected, workload.runAll());
    assertEquals(idChecksum(ids), workload.idChecksum());
  }

  @Test
  public void genealogWorkloadChecksumMatchesExtractedBfs() throws Exception {
    long[] ids = sequentialIds();
    Object[] queries = new Object[QueryFixture.REQUIRED_COUNT];
    SortedPointersAggregateStrategy strategy = new SortedPointersAggregateStrategy();
    long expected = 0L;
    for (int i = 0; i < queries.length; i++) {
      GenealogBenchmarkTuple root = new GenealogBenchmarkTuple(i, 0L, GenealogTupleType.SOURCE);
      queries[i] = root;
      expected += GenealogContributionQuery.count(root, strategy);
    }
    File path = tmp.newFile("genealog.ser");
    QueryFixture.of(QueryFixture.METHOD_GENEALOG, 0, ids, queries).write(path.getAbsolutePath());
    QueryWorkload workload = QueryWorkload.load(QueryFixture.METHOD_GENEALOG, path.getAbsolutePath());
    assertEquals(QueryFixture.REQUIRED_COUNT, workload.queryCount());
    assertEquals(expected, workload.runAll());
    assertEquals(idChecksum(ids), workload.idChecksum());
  }

  @Test
  public void methodDispatchIsOutsideQueryLoop() throws Exception {
    String src = readSource(QueryWorkload.class);
    String runAll = src.substring(src.indexOf("public long runAll()"));
    int firstLoop = runAll.indexOf("for (int i = 0; i < queries.length; i++)");
    assertTrue(firstLoop > 0);
    String beforeLoop = runAll.substring(0, firstLoop);
    assertTrue(beforeLoop.contains("QueryFixture.METHOD_CAPS.equals(method)"));
    assertTrue(beforeLoop.contains("QueryFixture.METHOD_GREEN.equals(method)"));
    assertTrue(beforeLoop.contains("QueryFixture.METHOD_GENEALOG.equals(method)"));
    String loopRegion = runAll.substring(firstLoop);
    assertFalse(loopRegion.contains("METHOD_CAPS.equals"));
    assertFalse(loopRegion.contains("METHOD_GREEN.equals"));
    assertFalse(loopRegion.contains("METHOD_GENEALOG.equals"));
    assertFalse(loopRegion.contains(".equals(method)"));
  }

  @Test
  public void idChecksumUsesJavaLongWraparoundSum() throws Exception {
    long[] ids = sequentialIds();
    ids[ids.length - 1] = Long.MAX_VALUE;
    Object[] queries = new Object[QueryFixture.REQUIRED_COUNT];
    for (int i = 0; i < queries.length; i++) {
      queries[i] = HowMuch.of(1L);
    }
    File path = tmp.newFile("wrap.ser");
    QueryFixture.of(QueryFixture.METHOD_CAPS, 1, ids, queries).write(path.getAbsolutePath());
    QueryWorkload workload = QueryWorkload.load(QueryFixture.METHOD_CAPS, path.getAbsolutePath());
    assertEquals(idChecksum(ids), workload.idChecksum());
  }

  @Test
  public void loadRejectsMalformedAndWrongMethodFixtures() throws Exception {
    File malformed = tmp.newFile("bad.ser");
    FileOutputStream out = new FileOutputStream(malformed);
    try {
      out.write(new byte[] {0, 1, 2});
    } finally {
      out.close();
    }
    try {
      QueryWorkload.load(QueryFixture.METHOD_CAPS, malformed.getAbsolutePath());
      fail("expected malformed");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().toLowerCase(Locale.ROOT).contains("malformed"));
    }

    long[] ids = sequentialIds();
    Object[] queries = new Object[QueryFixture.REQUIRED_COUNT];
    for (int i = 0; i < queries.length; i++) {
      queries[i] = HowMuch.of(1L);
    }
    File caps = tmp.newFile("caps-wrong.ser");
    QueryFixture.of(QueryFixture.METHOD_CAPS, 1, ids, queries).write(caps.getAbsolutePath());
    try {
      QueryWorkload.load(QueryFixture.METHOD_GREEN, caps.getAbsolutePath());
      fail("expected wrong method");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().toLowerCase(Locale.ROOT).contains("method"));
    }
  }

  private static long[] sequentialIds() {
    long[] ids = new long[QueryFixture.REQUIRED_COUNT];
    for (int i = 0; i < ids.length; i++) {
      ids[i] = i;
    }
    return ids;
  }

  private static long idChecksum(long[] ids) {
    long sum = 0L;
    for (int i = 0; i < ids.length; i++) {
      sum += ids[i];
    }
    return sum;
  }

  private static String readSource(Class<?> type) throws Exception {
    File src = new File("src/main/java/" + type.getName().replace('.', '/') + ".java");
    BufferedReader in = new BufferedReader(
        new InputStreamReader(new FileInputStream(src), StandardCharsets.UTF_8));
    try {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = in.readLine()) != null) {
        sb.append(line).append('\n');
      }
      return sb.toString();
    } finally {
      in.close();
    }
  }
}
