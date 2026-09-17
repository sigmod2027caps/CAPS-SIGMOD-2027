package queryjmh;

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
import temporalindex.QueryFixture;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
// JMH, not a hand loop — it handles warmup, forking, and GC between trials.
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = "-XX:+AlwaysPreTouch")
@State(Scope.Benchmark)
@Threads(1)
public class QueryBenchmark {

  @Param({"caps", "green", "genealog"})
  public String method;

  @Param({"MISSING"})
  public String fixturePath;

  private QueryWorkload workload;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    workload = QueryWorkload.load(method, fixturePath);
  }

  @Benchmark
  @OperationsPerInvocation(QueryFixture.REQUIRED_COUNT)
  // One JMH invocation runs all 1,000 queries; score is ns per query, not per batch.
  public long query() {
    return workload.runAll();
  }
}
