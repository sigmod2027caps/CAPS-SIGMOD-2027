package queryjmh;

import caps.HowMuch;
import java.io.IOException;
import temporalindex.QueryFixture;

/** Writes a 1,000-query CAPS fixture for standalone JMH smoke tests. */
public final class SyntheticFixture {

  private SyntheticFixture() {}

  // Synthetic ids 0..999 — real fixtures inlude captured polynomials from provenance_shared runs.
  public static void writeCaps(String path) throws IOException {
    long[] ids = new long[QueryFixture.REQUIRED_COUNT];
    Object[] queries = new Object[QueryFixture.REQUIRED_COUNT];
    for (int i = 0; i < QueryFixture.REQUIRED_COUNT; i++) {
      ids[i] = i;
      queries[i] = HowMuch.of(i + 1L, 1L);
    }
    QueryFixture.of(QueryFixture.METHOD_CAPS, 2, ids, queries).write(path);
  }

  public static void main(String[] args) throws Exception {
    if (args == null || args.length != 1) {
      throw new IllegalArgumentException("usage: SyntheticFixture FIXTURE_PATH");
    }
    writeCaps(args[0]);
  }
}
