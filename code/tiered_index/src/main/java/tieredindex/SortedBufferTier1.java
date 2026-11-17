package tieredindex;

import java.util.Arrays;

/**
 * Naive Tier 1 for the crossover experiment: a sorted array (timestamps + row-major counts)
 * with binary-search insertion (O(n M) shift) and a linear-scan range sum (O(log n + r M) for
 * r records in range). Cheaper constants than the tree while n is small.
 * The alternative Tier 1 compared with the red-black tree in experiments/disorder (arm tier1).
 */
public class SortedBufferTier1 implements Tier1 {

  final int channels;
  long[] ts = new long[16];
  long[] vals; // n rows of `channels` longs
  int n;

  public SortedBufferTier1(int channels) {
    this.channels = channels;
    this.vals = new long[16 * channels];
  }

  @Override
  public int size() {
    return n;
  }

  @Override
  public long minTs() {
    return n == 0 ? Long.MAX_VALUE : ts[0];
  }

  @Override
  public void insert(long t, long[] v) {
    if (n == ts.length) {
      ts = Arrays.copyOf(ts, n * 2);
      vals = Arrays.copyOf(vals, n * 2 * channels);
    }
    int pos = upperBound(t); // after equal timestamps: ties stay in insertion order
    System.arraycopy(ts, pos, ts, pos + 1, n - pos);
    System.arraycopy(vals, pos * channels, vals, (pos + 1) * channels, (n - pos) * channels);
    ts[pos] = t;
    System.arraycopy(v, 0, vals, pos * channels, channels);
    n++;
  }

  @Override
  public long rangeSum(long t1, long t2, long[] out) {
    if (t1 > t2)
      return 0;
    int lo = t1 == Long.MIN_VALUE ? 0 : upperBound(t1 - 1); // first ts >= t1
    int hi = upperBound(t2);
    for (int i = lo; i < hi; i++) {
      int base = i * channels;
      for (int c = 0; c < channels; c++)
        out[c] += vals[base + c];
    }
    return hi - lo;
  }

  @Override
  public Entries splitPrefix(long w) {
    int k = upperBound(w);
    Entries e = new Entries(k, channels, Arrays.copyOf(ts, k), Arrays.copyOf(vals, k * channels));
    System.arraycopy(ts, k, ts, 0, n - k);
    System.arraycopy(vals, k * channels, vals, 0, (n - k) * channels);
    n -= k;
    return e;
  }

  @Override
  public Entries entries() {
    return new Entries(n, channels, Arrays.copyOf(ts, n), Arrays.copyOf(vals, n * channels));
  }

  /** First position with ts > t. */
  int upperBound(long t) {
    int lo = 0;
    int hi = n;
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (ts[mid] <= t)
        lo = mid + 1;
      else
        hi = mid;
    }
    return lo;
  }
}
