package temporalindex;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class QueryIdManifestTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void selectsExactlyKUniqueIdsBySmallestUnsignedRank() {
    long seed = 42L;
    long[] observed = new long[] {7L, 3L, 9L, 1L, 5L, 8L, 2L, 4L, 6L, 0L};
    QueryIdManifest.Generator gen = new QueryIdManifest.Generator(3, seed);
    for (int i = 0; i < observed.length; i++) {
      gen.observe(observed[i]);
    }
    long[] selected = selectedIds(gen);
    assertEquals(3, selected.length);
    assertArrayEquals(oracleBottomK(observed, 3, seed), selected);
  }

  @Test
  public void selectionIsDeterministicAndOrderIndependent() {
    long seed = 99L;
    long[] a = new long[] {10L, -3L, 4L, Long.MIN_VALUE, 4L, 88L, -1L, 0L};
    long[] b = new long[] {-1L, 88L, 10L, 0L, Long.MIN_VALUE, 4L, -3L, 4L};
    assertArrayEquals(generate(a, 4, seed), generate(b, 4, seed));
    assertArrayEquals(oracleBottomK(a, 4, seed), generate(a, 4, seed));
  }

  @Test
  public void duplicateObservationsDoNotChangeSelection() {
    long seed = 7L;
    QueryIdManifest.Generator gen = new QueryIdManifest.Generator(2, seed);
    gen.observe(11L);
    gen.observe(11L);
    gen.observe(11L);
    gen.observe(22L);
    gen.observe(22L);
    assertArrayEquals(new long[] {11L, 22L}, selectedIds(gen));
    assertEquals(2, gen.retained());
  }

  @Test
  public void generatorDoesNotRetainAllObservedIds() {
    QueryIdManifest.Generator gen = new QueryIdManifest.Generator(5, 123L);
    for (long i = 0; i < 10_000L; i++) {
      gen.observe(i);
    }
    assertEquals(5, gen.retained());
    assertEquals(5, selectedIds(gen).length);
  }

  @Test
  public void unsignedIdBreaksEqualRank() {
    assertTrue(QueryIdManifest.compareUnsignedRankThenId(1L, 9L, 2L, 0L) < 0);
    assertTrue(QueryIdManifest.compareUnsignedRankThenId(5L, 1L, 5L, 2L) < 0);
    assertTrue(QueryIdManifest.compareUnsignedRankThenId(5L, -1L, 5L, 0L) > 0);
    assertEquals(0, QueryIdManifest.compareUnsignedRankThenId(5L, 1L, 5L, 1L));
  }

  @Test
  public void manifestRoundTripPreservesHeaderAndUnsignedOrder() throws Exception {
    File path = tmp.newFile("ids.txt");
    long seed = 42L;
    long[] observed = new long[] {-1L, 0L, 2L, Long.MIN_VALUE, 5L, 1L};
    QueryIdManifest.Generator gen = new QueryIdManifest.Generator(4, seed);
    for (int i = 0; i < observed.length; i++) {
      gen.observe(observed[i]);
    }
    gen.write(path.getAbsolutePath());

    List<String> lines = readLines(path);
    assertEquals("# count=4 seed=42", lines.get(0));
    assertEquals(5, lines.size());
    long[] fileIds = new long[lines.size() - 1];
    for (int i = 1; i < lines.size(); i++) {
      fileIds[i - 1] = Long.parseLong(lines.get(i));
    }
    assertUnsignedSorted(fileIds);
    assertArrayEquals(oracleBottomK(observed, 4, seed), fileIds);

    QueryIdManifest.Matcher matcher = QueryIdManifest.Matcher.load(path.getAbsolutePath());
    assertEquals(4, matcher.total());
    assertEquals(0, matcher.matched());
    assertEquals(4, matcher.remaining());
  }

  @Test
  public void loadRejectsMalformedHeader() throws Exception {
    expectLoadRejected("count=2 seed=1\n1\n2\n");
    expectLoadRejected("# count=x seed=1\n1\n");
    expectLoadRejected("# seed=1 count=1\n1\n");
    expectLoadRejected("");
  }

  @Test
  public void loadRejectsMalformedIdLines() throws Exception {
    expectLoadRejected("# count=1 seed=1\nabc\n");
    expectLoadRejected("# count=1 seed=1\n1 2\n");
    expectLoadRejected("# count=1 seed=1\n\n");
  }

  @Test
  public void loadRejectsDuplicateIds() throws Exception {
    expectLoadRejected("# count=2 seed=1\n5\n5\n");
  }

  @Test
  public void loadRejectsHeaderCountMismatch() throws Exception {
    expectLoadRejected("# count=3 seed=1\n1\n2\n");
    expectLoadRejected("# count=1 seed=1\n1\n2\n");
  }

  @Test
  public void matcherSelectsOnlyFirstOccurrenceAndReportsCounts() throws Exception {
    File path = writeManifest("# count=3 seed=9\n10\n20\n30\n");
    QueryIdManifest.Matcher matcher = QueryIdManifest.Matcher.load(path.getAbsolutePath());
    assertFalse(matcher.selectFirst(99L));
    assertTrue(matcher.selectFirst(20L));
    assertFalse(matcher.selectFirst(20L));
    assertTrue(matcher.selectFirst(10L));
    assertFalse(matcher.selectFirst(10L));
    assertEquals(3, matcher.total());
    assertEquals(2, matcher.matched());
    assertEquals(1, matcher.remaining());

    assertTrue(matcher.selectFirst(30L));
    assertEquals(0, matcher.remaining());
    assertEquals(3, matcher.matched());
  }

  @Test
  public void matcherMatchedRowsAreUnsignedSorted() throws Exception {
    File path = writeManifest("# count=3 seed=1\n-1\n0\n1\n");
    QueryIdManifest.Matcher matcher = QueryIdManifest.Matcher.load(path.getAbsolutePath());
    assertTrue(matcher.selectFirst(-1L));
    assertTrue(matcher.selectFirst(1L));
    ArrayList<Long> rows = matcher.matchedRows();
    assertEquals(2, rows.size());
    assertEquals(Long.valueOf(1L), rows.get(0));
    assertEquals(Long.valueOf(-1L), rows.get(1));
    assertTrue(matcher.selectFirst(0L));
    rows = matcher.matchedRows();
    assertEquals(Long.valueOf(0L), rows.get(0));
    assertEquals(Long.valueOf(1L), rows.get(1));
    assertEquals(Long.valueOf(-1L), rows.get(2));
  }

  private static long[] generate(long[] ids, int k, long seed) {
    QueryIdManifest.Generator gen = new QueryIdManifest.Generator(k, seed);
    for (int i = 0; i < ids.length; i++) {
      gen.observe(ids[i]);
    }
    return selectedIds(gen);
  }

  private static long[] selectedIds(QueryIdManifest.Generator gen) {
    try {
      File path = File.createTempFile("qim-selected", ".txt");
      path.deleteOnExit();
      gen.write(path.getAbsolutePath());
      QueryIdManifest.Matcher matcher = QueryIdManifest.Matcher.load(path.getAbsolutePath());
      long[] ids = new long[matcher.total()];
      int n = 0;
      BufferedReader in = new BufferedReader(new FileReader(path));
      try {
        in.readLine();
        String line;
        while ((line = in.readLine()) != null) {
          ids[n++] = Long.parseLong(line);
        }
      } finally {
        in.close();
      }
      return ids;
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static long[] oracleBottomK(long[] ids, int k, long seed) {
    LinkedHashSet<Long> unique = new LinkedHashSet<Long>();
    for (int i = 0; i < ids.length; i++) {
      unique.add(Long.valueOf(ids[i]));
    }
    ArrayList<Long> all = new ArrayList<Long>(unique);
    Collections.sort(all, new Comparator<Long>() {
      @Override
      public int compare(Long a, Long b) {
        long ra = splitmix64(a.longValue() ^ seed);
        long rb = splitmix64(b.longValue() ^ seed);
        return QueryIdManifest.compareUnsignedRankThenId(ra, a.longValue(), rb, b.longValue());
      }
    });
    int n = Math.min(k, all.size());
    ArrayList<Long> chosen = new ArrayList<Long>(all.subList(0, n));
    Collections.sort(chosen, new Comparator<Long>() {
      @Override
      public int compare(Long a, Long b) {
        return Long.compareUnsigned(a.longValue(), b.longValue());
      }
    });
    long[] out = new long[chosen.size()];
    for (int i = 0; i < chosen.size(); i++) {
      out[i] = chosen.get(i).longValue();
    }
    return out;
  }

  private static long splitmix64(long z) {
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
    return z ^ (z >>> 31);
  }

  private static void assertUnsignedSorted(long[] ids) {
    HashSet<Long> seen = new HashSet<Long>();
    for (int i = 0; i < ids.length; i++) {
      assertTrue("duplicate id " + ids[i], seen.add(Long.valueOf(ids[i])));
      if (i > 0) {
        assertTrue("ids not unsigned-sorted at " + i,
            Long.compareUnsigned(ids[i - 1], ids[i]) < 0);
      }
    }
  }

  private void expectLoadRejected(String contents) throws IOException {
    File path = writeManifest(contents);
    try {
      QueryIdManifest.Matcher.load(path.getAbsolutePath());
      fail("expected malformed manifest to be rejected:\n" + contents);
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage(), expected.getMessage().length() > 0);
    }
  }

  private File writeManifest(String contents) throws IOException {
    File path = tmp.newFile();
    OutputStreamWriter out =
        new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8);
    try {
      out.write(contents);
    } finally {
      out.close();
    }
    return path;
  }

  private static List<String> readLines(File path) throws IOException {
    ArrayList<String> lines = new ArrayList<String>();
    BufferedReader in = new BufferedReader(
        new java.io.InputStreamReader(new java.io.FileInputStream(path), StandardCharsets.UTF_8));
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
}
