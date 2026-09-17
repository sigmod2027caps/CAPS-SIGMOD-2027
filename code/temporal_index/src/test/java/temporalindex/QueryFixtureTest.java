package temporalindex;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.Locale;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class QueryFixtureTest {

  private static final int N = QueryFixture.REQUIRED_COUNT;

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void roundTripPreservesMethodChannelsSortedIdsAndQueries() throws Exception {
    long[] ids = sequentialIds();
    Long[] queries = sequentialQueries();
    long checksum = sum(queries);
    File path = tmp.newFile("caps.ser");

    QueryFixture.of(QueryFixture.METHOD_CAPS, 2, ids, queries).write(path.getAbsolutePath());
    QueryFixture loaded = QueryFixture.load(path.getAbsolutePath(), QueryFixture.METHOD_CAPS);

    assertEquals(QueryFixture.METHOD_CAPS, loaded.method());
    assertEquals(2, loaded.channels());
    assertEquals(N, loaded.ids().length);
    assertEquals(N, loaded.queries().length);
    assertArrayEquals(ids, loaded.ids());
    assertEquals(checksum, sum(loaded.queries()));
    for (int i = 0; i < N; i++) {
      assertEquals(queries[i], loaded.queries()[i]);
    }
  }

  @Test
  public void greenAndGenealogRoundTripChecksums() throws Exception {
    long[] ids = sequentialIds();
    Long[] queries = sequentialQueries();
    File green = tmp.newFile("green.ser");
    QueryFixture.of(QueryFixture.METHOD_GREEN, 3, ids, queries).write(green.getAbsolutePath());
    QueryFixture loadedGreen = QueryFixture.load(green.getAbsolutePath(), QueryFixture.METHOD_GREEN);
    assertEquals(QueryFixture.METHOD_GREEN, loadedGreen.method());
    assertEquals(3, loadedGreen.channels());
    assertEquals(sum(queries), sum(loadedGreen.queries()));

    File genealog = tmp.newFile("genealog.ser");
    QueryFixture.of(QueryFixture.METHOD_GENEALOG, 0, ids, queries).write(genealog.getAbsolutePath());
    QueryFixture loadedGenealog =
        QueryFixture.load(genealog.getAbsolutePath(), QueryFixture.METHOD_GENEALOG);
    assertEquals(QueryFixture.METHOD_GENEALOG, loadedGenealog.method());
    assertEquals(0, loadedGenealog.channels());
    assertEquals(sum(queries), sum(loadedGenealog.queries()));
  }

  @Test
  public void loadRejectsWrongMethod() throws Exception {
    File path = tmp.newFile("wrong-method.ser");
    QueryFixture.of(QueryFixture.METHOD_CAPS, 1, sequentialIds(), sequentialQueries())
        .write(path.getAbsolutePath());
    try {
      QueryFixture.load(path.getAbsolutePath(), QueryFixture.METHOD_GREEN);
      fail("expected wrong-method");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().toLowerCase(Locale.ROOT).contains("method"));
    }
  }

  @Test
  public void loadRejectsWrongCount() throws Exception {
    File path = tmp.newFile("wrong-count.ser");
    long[] ids = new long[] {1L, 2L};
    Object[] queries = new Object[] {Long.valueOf(1L), Long.valueOf(2L)};
    QueryFixture.unchecked(QueryFixture.METHOD_CAPS, 1, ids, queries).write(path.getAbsolutePath());
    try {
      QueryFixture.load(path.getAbsolutePath(), QueryFixture.METHOD_CAPS);
      fail("expected wrong-count");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("1000"));
    }
  }

  @Test
  public void loadRejectsMalformedBytes() throws Exception {
    File path = tmp.newFile("malformed.ser");
    FileOutputStream out = new FileOutputStream(path);
    try {
      out.write(new byte[] {1, 2, 3, 4});
    } finally {
      out.close();
    }
    try {
      QueryFixture.load(path.getAbsolutePath(), QueryFixture.METHOD_CAPS);
      fail("expected malformed");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().toLowerCase(Locale.ROOT).contains("malformed"));
    }
  }

  @Test
  public void loadRejectsSerializedWrongType() throws Exception {
    File path = tmp.newFile("string.ser");
    ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(path));
    try {
      out.writeObject("not a fixture");
    } finally {
      out.close();
    }
    try {
      QueryFixture.load(path.getAbsolutePath(), QueryFixture.METHOD_CAPS);
      fail("expected malformed");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().toLowerCase(Locale.ROOT).contains("malformed"));
    }
  }

  @Test
  public void ofRejectsUnsortedIds() {
    long[] ids = sequentialIds();
    long tmp0 = ids[0];
    ids[0] = ids[1];
    ids[1] = tmp0;
    try {
      QueryFixture.of(QueryFixture.METHOD_CAPS, 1, ids, sequentialQueries());
      fail("expected unsorted ids");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().toLowerCase(Locale.ROOT).contains("sort"));
    }
  }

  private static long[] sequentialIds() {
    long[] ids = new long[N];
    for (int i = 0; i < N; i++) {
      ids[i] = i;
    }
    return ids;
  }

  private static Long[] sequentialQueries() {
    Long[] queries = new Long[N];
    for (int i = 0; i < N; i++) {
      queries[i] = Long.valueOf(i + 1L);
    }
    return queries;
  }

  private static long sum(Object[] queries) {
    long total = 0;
    for (int i = 0; i < queries.length; i++) {
      total += ((Long) queries[i]).longValue();
    }
    return total;
  }
}
