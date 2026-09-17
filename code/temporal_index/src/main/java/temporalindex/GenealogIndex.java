package temporalindex;

// GeneaLog baseline index: TemporalIndex plus tuple refs — query range then BFS each tuple's graph.
// No expiry or summarize; tuples must stay reachable becasue traversal needs the full graph.
public class GenealogIndex<T> {

  /** Records per leaf block and separators per group (same as CapsIndex). */
  public static final int CAPACITY = TemporalIndex.CAPACITY;

  private final TemporalIndex tsIndex = new TemporalIndex();

  // Grows with append only — no dropFront here, unlike CapsIndex prefix rings.
  private Object[] values = new Object[16];

  private long firstEverTs = Long.MAX_VALUE;

  /** Append the next record; timestamps must be non-decreasing. */
  public void append(long timestamp, T value) {
    if (firstEverTs == Long.MAX_VALUE) {
      firstEverTs = timestamp;
    }
    int pos = tsIndex.append(timestamp);
    if (pos == values.length) {
      Object[] bigger = new Object[values.length * 2];
      System.arraycopy(values, 0, bigger, 0, values.length);
      values = bigger;
    }
    values[pos] = value;
  }

  public int size() {
    return tsIndex.size();
  }

  public long firstTs() {
    return firstEverTs;
  }

  public long tsAt(int i) {
    return tsIndex.tsAt(i);
  }

  @SuppressWarnings("unchecked")
  public T get(int i) {
    return (T) values[i];
  }

  /** First index with ts >= t (start of a [t, ...] range). */
  public int lowerBound(long t) {
    return tsIndex.lowerBound(t);
  }

  /** First index with ts > t (exclusive end of a [..., t] range). */
  public int upperBound(long t) {
    return tsIndex.upperBound(t);
  }
}
