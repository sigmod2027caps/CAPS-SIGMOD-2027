package realbench;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * One JMH run, one CSV row: RealJmhMain METHOD KIND FIXTURE CHANNELS OUT_CSV
 * (METHOD dense|sparse|green|genealog, KIND total|vector|pairs). JMH_WARMUP / JMH_MEASURE: iterations of 1 s.
 */
public final class RealJmhMain {

  private RealJmhMain() {}

  public static void main(String[] args) throws Exception {
    int warmup = Integer.parseInt(System.getenv().getOrDefault("JMH_WARMUP", "3"));
    int measure = Integer.parseInt(System.getenv().getOrDefault("JMH_MEASURE", "3"));
    RealQueryBenchmark probe = new RealQueryBenchmark();
    probe.method = args[0];
    probe.kind = args[1];
    probe.fixture = args[2];
    probe.channels = Integer.parseInt(args[3]);
    probe.setup();
    long checksum = probe.checksum();
    Options options = new OptionsBuilder()
        .include("^" + RealQueryBenchmark.class.getName() + ".query$")
        .param("method", args[0]).param("kind", args[1]).param("fixture", args[2])
        .param("channels", args[3])
        .mode(org.openjdk.jmh.annotations.Mode.AverageTime)
        .timeUnit(TimeUnit.NANOSECONDS)
        .warmupIterations(warmup).warmupTime(TimeValue.seconds(1))
        .measurementIterations(measure).measurementTime(TimeValue.seconds(1))
        .forks(Integer.parseInt(System.getenv().getOrDefault("JMH_FORKS", "1"))).threads(1).jvmArgsAppend("-XX:+AlwaysPreTouch").build();
    Collection<RunResult> results = new Runner(options).run();
    Result<?> primary = results.iterator().next().getPrimaryResult();
    try (PrintWriter pw = new PrintWriter(new FileWriter(new File(args[4])))) {
      pw.println("method,kind,score_ns,error_ns,samples,fixture_size,checksum");
      pw.printf(Locale.ROOT, "%s,%s,%s,%s,%d,%d,%d%n", args[0], args[1],
          Double.toString(primary.getScore()), Double.toString(primary.getScoreError()),
          primary.getSampleCount(), probe.size(), checksum);
    }
  }
}
