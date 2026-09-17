package temporalindex;

// Paper 5.1 naive baseline: scan every record in range after binary search on timestamps.
// Correctness oracle for CapsIndex — slow but simple when n is not huge.
public class ArrayStore {

  private final int channels;
  // Column-major per-channel arrays — not the prefix layout CapsIndex uses.
  private long[] ts = new long[16];
  private long[][] counts;
  private int size;

  public ArrayStore(int channels) {
    this.channels = channels;
    this.counts = new long[channels][16];
  }

  public void append(long timestamp, long[] c) {
    // Out-of-order append would make the binary search bounds wrong — same rule as CapsIndex.
    if (size > 0 && timestamp < ts[size - 1]) {
      throw new IllegalArgumentException("records must be appended in time order");
    }
    if (size == ts.length) {
      grow();
    }
    ts[size] = timestamp;
    for (int ch = 0; ch < channels; ch++) {
      counts[ch][size] = c[ch];
    }
    size++;
  }

  /** How-much per channel over [tsFrom, tsTo] (inclusive): scan and aggregate. */
  public long[] howMuch(long tsFrom, long tsTo) {
    long[] result = new long[channels];
    int from = lowerBound(tsFrom);
    // Every occurence in [from, to] is summed — not prefix subtraction like CapsIndex.
    for (int i = from; i < size && ts[i] <= tsTo; i++) {
      for (int ch = 0; ch < channels; ch++) {
        result[ch] += counts[ch][i];
      }
    }
    return result;
  }

  public int records() {
    return size;
  }

  public long tsAt(int i) {
    return ts[i];
  }

  /** Copies record i's per-channel counts into {@code out}. */
  public void countsAt(int i, long[] out) {
    for (int ch = 0; ch < channels; ch++) {
      out[ch] = counts[ch][i];
    }
  }

  private int lowerBound(long t) {
    int lo = 0;
    int hi = size;
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (ts[mid] < t) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  private void grow() {
    int capacity = ts.length * 2;
    long[] newTs = new long[capacity];
    System.arraycopy(ts, 0, newTs, 0, size);
    ts = newTs;
    for (int ch = 0; ch < channels; ch++) {
      long[] bigger = new long[capacity];
      System.arraycopy(counts[ch], 0, bigger, 0, size);
      counts[ch] = bigger;
    }
  }
}
