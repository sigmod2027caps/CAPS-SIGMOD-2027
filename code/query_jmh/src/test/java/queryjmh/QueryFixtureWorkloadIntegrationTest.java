package queryjmh;

import static org.junit.Assert.assertEquals;

import caps.HowMuch;
import ink.InkMeta;
import io.palyvos.provenance.ananke.aggregate.SortedPointersAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogBenchmarkTuple;
import io.palyvos.provenance.genealog.GenealogContributionQuery;
import io.palyvos.provenance.genealog.GenealogGraphClone;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import java.io.File;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import temporalindex.QueryFixture;

public class QueryFixtureWorkloadIntegrationTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void capsFixtureCopyRoundTripMatchesWorkloadChecksum() throws Exception {
    long[] ids = sequentialIds();
    Object[] queries = new Object[QueryFixture.REQUIRED_COUNT];
    long expected = 0L;
    for (int i = 0; i < queries.length; i++) {
      HowMuch live = HowMuch.of(i + 3L, 1L);
      HowMuch copy = live.copy();
      live.counts[0] = 999L;
      queries[i] = copy;
      expected += copy.total();
    }
    assertChecksum(QueryFixture.METHOD_CAPS, 2, ids, queries, expected);
  }

  @Test
  public void greenFixtureCopyRoundTripMatchesWorkloadChecksum() throws Exception {
    long[] ids = sequentialIds();
    Object[] queries = new Object[QueryFixture.REQUIRED_COUNT];
    long expected = 0L;
    for (int i = 0; i < queries.length; i++) {
      InkMeta live = InkMeta.source(0, 2, i).times(InkMeta.source(1, 2, i));
      InkMeta copy = live.copy();
      live.poly = InkMeta.source(0, 2, 99).poly;
      queries[i] = copy;
      expected += copy.howMuchTotal(2);
    }
    assertChecksum(QueryFixture.METHOD_GREEN, 2, ids, queries, expected);
  }

  @Test
  public void genealogFixtureCloneRoundTripMatchesWorkloadChecksum() throws Exception {
    long[] ids = sequentialIds();
    Object[] queries = new Object[QueryFixture.REQUIRED_COUNT];
    SortedPointersAggregateStrategy strategy = new SortedPointersAggregateStrategy();
    long expected = 0L;
    for (int i = 0; i < queries.length; i++) {
      GenealogBenchmarkTuple left = new GenealogBenchmarkTuple(i, 0L, GenealogTupleType.SOURCE);
      GenealogBenchmarkTuple right = new GenealogBenchmarkTuple(i, 0L, GenealogTupleType.SOURCE);
      GenealogBenchmarkTuple join = new GenealogBenchmarkTuple(i, 0L, GenealogTupleType.JOIN);
      join.setU1(left);
      join.setU2(right);
      GenealogTuple clone = GenealogGraphClone.cloneReachable(join, strategy);
      queries[i] = clone;
      expected += GenealogContributionQuery.count(clone, strategy);
    }
    assertChecksum(QueryFixture.METHOD_GENEALOG, 0, ids, queries, expected);
  }

  private void assertChecksum(String method, int channels, long[] ids, Object[] queries,
      long expected) throws Exception {
    File path = tmp.newFile(method + ".ser");
    QueryFixture.of(method, channels, ids, queries).write(path.getAbsolutePath());
    QueryWorkload workload = QueryWorkload.load(method, path.getAbsolutePath());
    assertEquals(QueryFixture.REQUIRED_COUNT, workload.queryCount());
    assertEquals(expected, workload.runAll());
  }

  private static long[] sequentialIds() {
    long[] ids = new long[QueryFixture.REQUIRED_COUNT];
    for (int i = 0; i < ids.length; i++) {
      ids[i] = i;
    }
    return ids;
  }
}
