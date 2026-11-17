package realbench;

import caps.HowMuch;
import ink.InkMeta;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.ananke.aggregate.SortedPointersAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogContributionQuery;
import io.palyvos.provenance.genealog.GenealogTuple;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
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
import prefix.SparseHowMuch;

/**
 * Per-output how-much query over the annotations of the ~1,000 sampled outputs (the same outputs for
 * every method). Score: ns per query. One invocation answers 1,000 queries cycling over the fixture.
 *   total   a scalar: the number of contributing records
 *   vector  the M-long dense answer long[M]   (dense: copy, sparse: expand, green: fold)
 *   pairs   sparse only: the non-zero (channel, count) pairs, i.e. clones of the two arrays
 * genealog has the total only: a backward traversal of the provenance graph (no channel identity).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = "-XX:+AlwaysPreTouch")
@State(Scope.Benchmark)
@Threads(1)
public class RealQueryBenchmark {

  @Param({"dense"})
  public String method;

  @Param({"total"})
  public String kind;

  @Param({"MISSING"})
  public String fixture;

  @Param({"1"})
  public int channels;

  private Object[] q;
  private ProvenanceAggregateStrategy strategy;

  @Setup(Level.Trial)
  public void setup() throws IOException, ClassNotFoundException {
    try (ObjectInputStream is = new ObjectInputStream(new FileInputStream(fixture))) {
      q = (Object[]) is.readObject();
    }
    if (q.length == 0) throw new IllegalStateException("empty fixture");
    strategy = new SortedPointersAggregateStrategy();
    if (method.equals("genealog") && !kind.equals("total"))
      throw new IllegalArgumentException("GeneaLog has no per-channel answer");
  }

  public int size() {
    return q.length;
  }

  /** Checksum of the totals, to compare across methods: the same outputs give the same sum. */
  public long checksum() {
    long s = 0;
    for (Object o : q) s += total(o);
    return s;
  }

  private long total(Object o) {
    if (method.equals("dense")) return ((HowMuch) o).total();
    if (method.equals("sparse")) return ((SparseHowMuch) o).total();
    if (method.equals("genealog")) return GenealogContributionQuery.count((GenealogTuple) o, strategy);
    return ((InkMeta) o).howMuchTotal(channels);
  }

  // Mode code, so that the timed loop has no string compares: 0 dense total, 1 sparse total, 2 green total,
  // 3 dense vector, 4 sparse vector, 5 green vector, 6 sparse pairs, 7 genealog total.
  private int code() {
    if (method.equals("genealog")) return 7;
    int m = method.equals("dense") ? 0 : method.equals("sparse") ? 1 : 2;
    if (kind.equals("total")) return m;
    if (kind.equals("pairs")) return 6;
    return 3 + m;
  }

  @Benchmark
  @OperationsPerInvocation(1000)
  public void query(Blackhole bh) {
    final Object[] a = q;
    final int n = a.length;
    final int c = code();
    final int ch = channels;
    int j = 0;
    for (int i = 0; i < 1000; i++) {
      Object o = a[j];
      if (++j == n) j = 0;
      switch (c) {
        case 0: bh.consume(((HowMuch) o).total()); break;
        case 1: bh.consume(((SparseHowMuch) o).total()); break;
        case 2: bh.consume(((InkMeta) o).howMuchTotal(ch)); break;
        case 3: bh.consume(((HowMuch) o).copyCounts()); break;
        case 4: bh.consume(((SparseHowMuch) o).toDense(ch)); break;
        case 5: bh.consume(((InkMeta) o).howMuch(ch)); break;
        case 7: bh.consume(GenealogContributionQuery.count((GenealogTuple) o, strategy)); break;
        default:
          SparseHowMuch s = (SparseHowMuch) o;
          bh.consume(s.ch.clone());
          bh.consume(s.cnt.clone());
      }
    }
  }
}
