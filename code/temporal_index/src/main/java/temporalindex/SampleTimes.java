package temporalindex;

import java.util.Arrays;

/**
 * The durations of the timed in-situ provenance queries of one sink (one per coin-sampled output), kept so that the
 * experiment can report the median next to the mean: the mean of about 1,000 samples is dominated by a few outliers
 * (a GC pause or a deoptimization inside the nanoTime bracket), the median is not.
 * Per-output timing statistics (experiments/how_much).
 */
public final class SampleTimes {
  private long[] v = new long[1024];
  private int n;

  public void add(long nanos) {
    if (n == v.length)
      v = Arrays.copyOf(v, n * 2);
    v[n++] = nanos;
  }

  public void addAll(SampleTimes other) {
    for (int i = 0; i < other.n; i++)
      add(other.v[i]);
  }

  public int size() {
    return n;
  }

  /** The median (mean of the two middle samples for an even count), or 0 if there is no sample. */
  public double median() {
    if (n == 0)
      return 0;
    long[] s = Arrays.copyOf(v, n);
    Arrays.sort(s);
    return n % 2 == 1 ? s[n / 2] : (s[n / 2 - 1] + s[n / 2]) / 2.0;
  }
}
