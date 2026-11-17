package scalebench;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
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
import temporalindex.CapsIndex;

/**
 * Temporal how-much query of CAPS over the prefix-sum index: HP[ts, te] per channel, offline on the
 * outputs the CAPS job wrote (key,timestamp,count,sum,c0;c1;...,bytes), appended in arrival order.
 * The ranges are RANGES fixed pseudo-random windows of `rangeMs` ms (seed 42), the score is ns per
 * query: O(log N + M) per query.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = "-XX:+AlwaysPreTouch")
@State(Scope.Benchmark)
@Threads(1)
public class TemporalBenchmark {

  public static final int RANGES = 1000;

  @Param({"MISSING"})
  public String capsFile;

  @Param({"1"})
  public int channels;

  @Param({"600000"})
  public long rangeMs;

  private CapsIndex index;
  private long[] from;
  private long[] to;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    index = new CapsIndex(channels);
    long first = Long.MAX_VALUE;
    long last = Long.MIN_VALUE;
    long[] counts = new long[channels];
    ArrayList<long[]> rows = new ArrayList<long[]>();
    try (BufferedReader br = new BufferedReader(new FileReader(capsFile))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (line.startsWith("---")) continue;
        String[] f = line.split(",");
        long ts = Long.parseLong(f[1]);
        String[] v = f[4].split(";");
        if (v.length != channels)
          throw new IllegalArgumentException("row with " + v.length + " channels, expected " + channels);
        for (int c = 0; c < channels; c++) counts[c] = Long.parseLong(v[c]);
        index.append(ts, counts);
        first = Math.min(first, ts);
        last = Math.max(last, ts);
        long[] row = new long[channels + 1];
        row[0] = ts;
        System.arraycopy(counts, 0, row, 1, channels);
        rows.add(row);
      }
    }
    Random rng = new Random(42);
    from = new long[RANGES];
    to = new long[RANGES];
    for (int i = 0; i < RANGES; i++) {
      from[i] = first + (long) (rng.nextDouble() * (last - first - rangeMs));
      to[i] = from[i] + rangeMs - 1;
    }
    // The index must agree with a plain scan of the rows (spot check of the first ranges).
    for (int i = 0; i < 20; i++) {
      double[] expected = new double[channels];
      for (long[] row : rows)
        if (row[0] >= from[i] && row[0] <= to[i])
          for (int c = 0; c < channels; c++) expected[c] += row[c + 1];
      double[] got = index.howMuch(from[i], to[i]);
      for (int c = 0; c < channels; c++)
        if (Math.abs(got[c] - expected[c]) > 1e-6 * Math.max(1, Math.abs(expected[c])))
          throw new IllegalStateException("index != scan for range " + i + " channel " + c);
    }
  }

  /** Sum over the ranges of the total count, for the CSV. */
  public double checksum() {
    double sum = 0;
    for (int i = 0; i < RANGES; i++)
      for (double v : index.howMuch(from[i], to[i])) sum += v;
    return sum;
  }

  @Benchmark
  @OperationsPerInvocation(RANGES)
  public void query(Blackhole bh) {
    for (int i = 0; i < RANGES; i++)
      bh.consume(index.howMuch(from[i], to[i]));
  }
}
