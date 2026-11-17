package scalebench;

import caps.HowMuch;
import ink.InkMeta;
import io.palyvos.provenance.ananke.aggregate.SortedPointersAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogContributionQuery;
import io.palyvos.provenance.genealog.GenealogTuple;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import temporalindex.QueryFixture;

/**
 * Per-output provenance query over the 1,000 shared output ids of a fixture (the same ids for
 * every method). Two questions: "how much in total" (a scalar) and "how much per channel" (the
 * M-vector). One JMH invocation answers all 1,000 queries, the score is ns per query.
 *
 *   caps      total: HowMuch.total (O(M) sum)             vector: HowMuch.copyCounts (O(M) copy)
 *   green     total: fold of the monomial (O(vars))        vector: the same fold into long[M]
 *   genealog  total: BFS over the provenance graph (O(graph)), no per-channel answer exists
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = "-XX:+AlwaysPreTouch")
@State(Scope.Benchmark)
@Threads(1)
public class PerOutputBenchmark {

  @Param({"caps"})
  public String method;

  @Param({"total"})
  public String kind;

  @Param({"MISSING"})
  public String fixturePath;

  private int channels;
  private Object[] queries;
  private SortedPointersAggregateStrategy genealogStrategy;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    QueryFixture fixture = QueryFixture.load(fixturePath, method);
    channels = fixture.channels();
    queries = fixture.queries();
    genealogStrategy = new SortedPointersAggregateStrategy();
    if (method.equals("genealog") && !kind.equals("total"))
      throw new IllegalArgumentException("GeneaLog has no per-channel answer");
  }

  /** Checksum of one pass, for the CSV: the same fixture must give the same sum in every run. */
  public long checksum() {
    long sum = 0;
    for (Object q : queries)
      sum += total(q);
    return sum;
  }

  private long total(Object q) {
    if (method.equals("caps")) return ((HowMuch) q).total();
    if (method.equals("green")) return ((InkMeta) q).howMuchTotal(channels);
    return GenealogContributionQuery.count((GenealogTuple) q, genealogStrategy);
  }

  @Benchmark
  @OperationsPerInvocation(QueryFixture.REQUIRED_COUNT)
  public void query(Blackhole bh) {
    if (kind.equals("total"))
      for (int i = 0; i < queries.length; i++)
        bh.consume(total(queries[i]));
    else if (method.equals("caps"))
      for (int i = 0; i < queries.length; i++)
        bh.consume(((HowMuch) queries[i]).copyCounts());
    else
      for (int i = 0; i < queries.length; i++)
        bh.consume(((InkMeta) queries[i]).howMuch(channels));
  }
}
