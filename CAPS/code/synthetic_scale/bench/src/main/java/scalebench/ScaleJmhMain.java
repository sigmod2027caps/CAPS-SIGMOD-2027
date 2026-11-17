package scalebench;

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
 * One JMH run, one CSV row (method,kind,score_ns,error_ns,samples,checksum).
 *
 *   perOutput METHOD KIND FIXTURE OUT_CSV     KIND = total | vector (vector: not for genealog)
 *   temporal  CAPS_FILE CHANNELS RANGE_MS OUT_CSV
 *
 * Environment: JMH_WARMUP, JMH_MEASURE (iterations of 1 s, default 10 and 5), for quick checks.
 */
public final class ScaleJmhMain {

  private ScaleJmhMain() {}

  public static void main(String[] args) throws Exception {
    int warmup = Integer.parseInt(System.getenv().getOrDefault("JMH_WARMUP", "10"));
    int measure = Integer.parseInt(System.getenv().getOrDefault("JMH_MEASURE", "5"));
    OptionsBuilder b = new OptionsBuilder();
    String label;
    String kind;
    double checksum;
    File out;
    if (args.length == 5 && args[0].equals("perOutput")) {
      label = args[1];
      kind = args[2];
      out = new File(args[4]);
      b.include("^" + PerOutputBenchmark.class.getName() + ".query$")
          .param("method", args[1]).param("kind", args[2]).param("fixturePath", args[3]);
      PerOutputBenchmark probe = new PerOutputBenchmark();
      probe.method = args[1];
      probe.kind = args[2];
      probe.fixturePath = args[3];
      probe.setup();
      checksum = probe.checksum();
    } else if (args.length == 5 && args[0].equals("temporal")) {
      label = "caps";
      kind = "temporal";
      out = new File(args[4]);
      b.include("^" + TemporalBenchmark.class.getName() + ".query$")
          .param("capsFile", args[1]).param("channels", args[2]).param("rangeMs", args[3]);
      TemporalBenchmark probe = new TemporalBenchmark();
      probe.capsFile = args[1];
      probe.channels = Integer.parseInt(args[2]);
      probe.rangeMs = Long.parseLong(args[3]);
      probe.setup();
      checksum = probe.checksum();
    } else
      throw new IllegalArgumentException("usage: perOutput METHOD KIND FIXTURE OUT_CSV"
          + " | temporal CAPS_FILE CHANNELS RANGE_MS OUT_CSV");
    Options options = b.mode(org.openjdk.jmh.annotations.Mode.AverageTime)
        .timeUnit(TimeUnit.NANOSECONDS)
        .warmupIterations(warmup).warmupTime(TimeValue.seconds(1))
        .measurementIterations(measure).measurementTime(TimeValue.seconds(1))
        .forks(1).threads(1)
        .jvmArgsAppend("-XX:+AlwaysPreTouch")
        .build();
    Collection<RunResult> results = new Runner(options).run();
    if (results.size() != 1) throw new IllegalStateException("expected one JMH result");
    Result<?> primary = results.iterator().next().getPrimaryResult();
    try (PrintWriter pw = new PrintWriter(new FileWriter(out))) {
      pw.println("method,kind,score_ns,error_ns,samples,checksum");
      pw.printf(Locale.ROOT, "%s,%s,%s,%s,%d,%s%n", label, kind,
          Double.toString(primary.getScore()), Double.toString(primary.getScoreError()),
          primary.getSampleCount(), Double.toString(checksum));
    }
  }
}
