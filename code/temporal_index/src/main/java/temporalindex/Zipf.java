package temporalindex;

import java.io.Serializable;

// Zipf(s) over the ranks 0 .. n-1 (P(rank r) proportional to 1 / (r + 1)^s), sampled by inverse CDF.
// The draw is a pure function of (seed, line number, salt): the same input line gets the same key in
// every run, at every parallelism and in the ordered reference run, whichever reader reads it.
public final class Zipf implements Serializable {

  private static final long serialVersionUID = 1L;

  private final double[] cdf;
  private final long seed;

  public Zipf(int n, double exponent, long seed) {
    this.seed = seed;
    cdf = new double[n];
    double sum = 0;
    for (int r = 0; r < n; r++) {
      sum += 1.0 / Math.pow(r + 1, exponent);
      cdf[r] = sum;
    }
    for (int r = 0; r < n; r++)
      cdf[r] /= sum;
  }

  public int size() {
    return cdf.length;
  }

  /** Rank drawn for the line (salt distinguishes several draws for one line). */
  public int rank(long line, int salt) {
    double u = (mix(seed ^ (line * 0x9E3779B97F4A7C15L) ^ (salt * 0xC2B2AE3D27D4EB4FL)) >>> 11) * 0x1.0p-53;
    int lo = 0, hi = cdf.length - 1;
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (cdf[mid] >= u)
        hi = mid;
      else
        lo = mid + 1;
    }
    return lo;
  }

  /** SplitMix64 finaliser. */
  static long mix(long z) {
    z += 0x9E3779B97F4A7C15L;
    z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
    return z ^ (z >>> 31);
  }
}
