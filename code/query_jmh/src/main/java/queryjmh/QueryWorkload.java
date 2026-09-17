package queryjmh;

import caps.HowMuch;
import ink.InkMeta;
import io.palyvos.provenance.ananke.aggregate.SortedPointersAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogContributionQuery;
import io.palyvos.provenance.genealog.GenealogTuple;
import java.io.IOException;
import temporalindex.QueryFixture;

/**
 * Loads one method's query fixture and evaluates all 1,000 queries.
 */
public final class QueryWorkload {

  private final String method;
  private final int channels;
  private final long[] ids;
  private final Object[] queries;
  private final SortedPointersAggregateStrategy genealogStrategy;

  private QueryWorkload(String method, int channels, long[] ids, Object[] queries) {
    this.method = method;
    this.channels = channels;
    this.ids = ids;
    this.queries = queries;
    this.genealogStrategy = QueryFixture.METHOD_GENEALOG.equals(method)
        ? new SortedPointersAggregateStrategy() : null;
  }

  public static QueryWorkload load(String method, String fixturePath) throws IOException {
    QueryFixture fixture = QueryFixture.load(fixturePath, method);
    return new QueryWorkload(fixture.method(), fixture.channels(), fixture.ids(),
        fixture.queries());
  }

  public int queryCount() {
    return queries.length;
  }

  public long idChecksum() {
    long sum = 0L;
    for (int i = 0; i < ids.length; i++) {
      sum += ids[i];
    }
    return sum;
  }

  public long runAll() {
    if (QueryFixture.METHOD_CAPS.equals(method)) {
      return runCaps();
    }
    if (QueryFixture.METHOD_GREEN.equals(method)) {
      return runGreen();
    }
    if (QueryFixture.METHOD_GENEALOG.equals(method)) {
      return runGenealog();
    }
    throw new IllegalStateException("unknown method " + method);
  }

  // Checksum ties the timed run to the fixture — same ids and answers every time, becasue JMH does not check them.
  private long runCaps() {
    long checksum = 0L;
    for (int i = 0; i < queries.length; i++) {
      checksum += ((HowMuch) queries[i]).total();
    }
    return checksum;
  }

  private long runGreen() {
    long checksum = 0L;
    for (int i = 0; i < queries.length; i++) {
      checksum += ((InkMeta) queries[i]).howMuchTotal(channels);
    }
    return checksum;
  }

  private long runGenealog() {
    long checksum = 0L;
    for (int i = 0; i < queries.length; i++) {
      checksum += GenealogContributionQuery.count((GenealogTuple) queries[i], genealogStrategy);
    }
    return checksum;
  }
}
