package queryjmh;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import temporalindex.QueryFixture;

public final class QueryJmhMain {

  private QueryJmhMain() {}

  public static final class FixtureStats {
    public final long checksum;
    public final int fixtureCount;
    public final long idChecksum;

    public FixtureStats(long checksum, int fixtureCount, long idChecksum) {
      this.checksum = checksum;
      this.fixtureCount = fixtureCount;
      this.idChecksum = idChecksum;
    }
  }

  public static FixtureStats inspect(String method, String fixturePath) throws IOException {
    QueryWorkload workload = QueryWorkload.load(method, fixturePath);
    return new FixtureStats(workload.runAll(), workload.queryCount(), workload.idChecksum());
  }

  public static void main(String[] args) throws Exception {
    if (args == null || args.length != 3) {
      throw new IllegalArgumentException("usage: METHOD FIXTURE OUTPUT_CSV");
    }
    String method = args[0];
    String fixture = args[1];
    String output = args[2];
    if (!QueryFixture.METHOD_CAPS.equals(method)
        && !QueryFixture.METHOD_GREEN.equals(method)
        && !QueryFixture.METHOD_GENEALOG.equals(method)) {
      throw new IllegalArgumentException("unknown method " + method);
    }
    FixtureStats stats = inspect(method, fixture);
    // Pre-flight checksum before JMH spends a minute on warmup.
    Collection<RunResult> results = new Runner(options(method, fixture)).run();
    QueryJmhCsv.write(new File(output), method, results, stats.checksum, stats.fixtureCount,
        stats.idChecksum);
  }

  public static Options options(String method, String fixturePath) {
    return new OptionsBuilder()
        .include("^" + QueryBenchmark.class.getName() + ".query$")
        .param("method", method)
        .param("fixturePath", fixturePath)
        .mode(org.openjdk.jmh.annotations.Mode.AverageTime)
        .timeUnit(TimeUnit.NANOSECONDS)
        .warmupIterations(10)
        .warmupTime(TimeValue.seconds(1))
        .measurementIterations(5)
        .measurementTime(TimeValue.seconds(1))
        .forks(1)
        .threads(1)
        // AlwaysPreTouch: pages are faulted before measurement, not during it.
        .jvmArgsAppend("-XX:+AlwaysPreTouch")
        .build();
  }
}
