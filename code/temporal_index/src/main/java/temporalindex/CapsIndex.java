package temporalindex;

import temporalindex.TemporalIndex.LongRing;

// Section-5 temporal how-much index: prefix-sum rings over {@link TemporalIndex} (shared with Genealog).
// HP[ts,te] = estPrefix(te) - estPrefix(ts-1); expiry keeps a guard row; summarize uses a second B-tree.
public class CapsIndex {

  /** Records per leaf block and separators per group. */
  public static final int CAPACITY = TemporalIndex.CAPACITY;

  private final int channels;

  // Exact region: timestamps + sparse B-tree (shared core, ring-buffered).
  private final TemporalIndex tsIndex = new TemporalIndex();

  // We keep prefix rings and tsIndex seperate — dropFront advances both in lockstep.
  private final LongRing[] prefix;

  // Running totals since stream start — not reset on expiry; guard row handles the drop.
  private final long[] running;

  private long firstEverTs = Long.MAX_VALUE;

  // Guard: cumulative row + timestamp of the last record removed from the
  // exact region (by expiry or summarization).
  private boolean hasGuard;
  private long guardTs;
  private long[] guardPrefix;

  // Summaries: the second, append-only B-tree (paper Fig. index(b)). Bucket b
  // covers [summaryIndex.tsAt(b), bucketEnd.get(b)] and stores the cumulative
  // row at its end in bucketPrefix.
  private final TemporalIndex summaryIndex = new TemporalIndex();
  private final LongRing bucketEnd = new LongRing();
  private final LongRing[] bucketPrefix;

  public CapsIndex(int channels) {
    this.channels = channels;
    this.prefix = new LongRing[channels];
    this.running = new long[channels];
    this.guardPrefix = new long[channels];
    this.bucketPrefix = new LongRing[channels];
    for (int c = 0; c < channels; c++) {
      prefix[c] = new LongRing();
      bucketPrefix[c] = new LongRing();
    }
  }

  public int channels() {
    return channels;
  }

  public int exactRecords() {
    return tsIndex.size();
  }

  public int summaryBuckets() {
    return summaryIndex.size();
  }

  /** Timestamp of the first record ever appended. */
  public long firstTs() {
    return firstEverTs;
  }

  /** Append the next sink output record; timestamps must be non-decreasing. */
  public void append(long timestamp, long[] counts) {
    if (tsIndex.size() == 0 && hasGuard && timestamp < guardTs) {
      throw new IllegalArgumentException(
          "records must be appended in time order: " + timestamp + " < " + guardTs);
    }
    if (firstEverTs == Long.MAX_VALUE) {
      firstEverTs = timestamp;
    }
    tsIndex.append(timestamp);
    for (int c = 0; c < channels; c++) {
      running[c] += counts[c];
      prefix[c].addLast(running[c]);
    }
  }

  /**
   * How-much provenance per channel over [tsFrom, tsTo] (inclusive).
   * Exact over exact records; approximate where a boundary falls inside a
   * summarized bucket.
   */
  public double[] howMuch(long tsFrom, long tsTo) {
    if (tsFrom > tsTo) {
      throw new IllegalArgumentException("empty range: [" + tsFrom + ", " + tsTo + "]");
    }
    double[] hi = estPrefix(tsTo);
    // tsFrom-1 overflows at Long.MIN_VALUE; treat the prefix before the stream as zero.
    double[] lo = tsFrom == Long.MIN_VALUE ? new double[channels] : estPrefix(tsFrom - 1);
    double[] result = new double[channels];
    for (int c = 0; c < channels; c++) {
      result[c] = hi[c] - lo[c];
    }
    return result;
  }

  /** Absolute versioning (paper Def. versioning-abs): HP(w2) - HP(w1) per channel. */
  public double[] deltaHowMuch(long ts1, long te1, long ts2, long te2) {
    double[] a = howMuch(ts1, te1);
    double[] b = howMuch(ts2, te2);
    double[] result = new double[channels];
    for (int c = 0; c < channels; c++) {
      result[c] = b[c] - a[c];
    }
    return result;
  }

  /** Contribution share rho per channel: HP(c) / sum over channels of HP. */
  public double[] share(long tsFrom, long tsTo) {
    double[] hp = howMuch(tsFrom, tsTo);
    double total = 0;
    for (int c = 0; c < channels; c++) {
      total += hp[c];
    }
    double[] result = new double[channels];
    for (int c = 0; c < channels; c++) {
      result[c] = total == 0 ? 0 : hp[c] / total;
    }
    return result;
  }

  /** Relative versioning (paper Def. versioning-rel): rho(w2) - rho(w1) per channel. */
  public double[] deltaShare(long ts1, long te1, long ts2, long te2) {
    double[] a = share(ts1, te1);
    double[] b = share(ts2, te2);
    double[] result = new double[channels];
    for (int c = 0; c < channels; c++) {
      result[c] = b[c] - a[c];
    }
    return result;
  }

  /** Drop exact records with timestamp < horizon, keeping the guard row. */
  public void expireBefore(long horizon) {
    int k = tsIndex.lowerBound(horizon);
    if (k == 0) {
      return;
    }
    setGuard(k - 1);
    dropFront(k);
  }

  /**
   * Compress exact records older than {@code horizon} into buckets of
   * {@code bucketWidth} time units, aligned to the first timestamp ever seen
   * (paper Fig. prefixC: buckets [1,3], [4,6] for a stream starting at 1).
   * Only whole buckets are summarized: the horizon is snapped down to a
   * bucket boundary first.
   */
  public void summarizeBefore(long horizon, long bucketWidth) {
    if (firstEverTs == Long.MAX_VALUE) {
      return;
    }
    // Snap horizon down to a bucket boundary — partial buckets at the edge stay exact.
    long cutoff = firstEverTs
        + Math.floorDiv(horizon - firstEverTs, bucketWidth) * bucketWidth;
    int k = tsIndex.lowerBound(cutoff);
    if (k == 0) {
      return;
    }
    long currentBucket = Long.MIN_VALUE;
    for (int i = 0; i < k; i++) {
      long bucket = Math.floorDiv(tsIndex.tsAt(i) - firstEverTs, bucketWidth);
      if (bucket != currentBucket) {
        if (currentBucket != Long.MIN_VALUE) {
          closeBucket(currentBucket, bucketWidth, i - 1);
        }
        currentBucket = bucket;
      }
    }
    closeBucket(currentBucket, bucketWidth, k - 1);
    setGuard(k - 1);
    dropFront(k);
  }

  /** Bytes of the exact region: timestamps, prefix rows, sparse-index entries. */
  public long exactBytes() {
    long entries = (long) tsIndex.size() * (1 + channels);
    entries += tsIndex.separatorEntries();
    return entries * 8;
  }

  /** Bytes of the summarized region: bucket boundaries, cumulative rows,
   * sparse-index entries of the summary tree. */
  public long summaryBytes() {
    long entries = (long) summaryIndex.size() * (2 + channels);
    entries += summaryIndex.separatorEntries();
    return entries * 8;
  }

  /** Bytes held: timestamps, prefix rows, sparse-index entries (both trees), buckets. */
  public long memoryBytes() {
    return exactBytes() + summaryBytes();
  }

  // ── internals ───────────────────────────────────────────────

  /**
   * Estimated cumulative row at time t: the contribution of all records with
   * timestamp <= t, per channel.
   */
  private double[] estPrefix(long t) {
    double[] result = new double[channels];
    if (t < firstEverTs) {
      return result;
    }
    int i = tsIndex.search(t);
    if (i >= 0) {
      for (int c = 0; c < channels; c++) {
        result[c] = prefix[c].get(i);
      }
      return result;
    }
    // t precedes the exact region: answer from the summaries / guard.
    if (summaryIndex.size() > 0) {
      if (t > bucketEnd.get(bucketEnd.size() - 1)) {
        if (hasGuard && t >= guardTs) {
          for (int c = 0; c < channels; c++) {
            result[c] = guardPrefix[c];
          }
          return result;
        }
        throw outOfHorizon(t);
      }
      int b = summaryIndex.search(t); // last bucket with start <= t, or -1
      if (b < 0) {
        return result;
      }
      if (t >= bucketEnd.get(b)) {
        for (int c = 0; c < channels; c++) {
          result[c] = bucketPrefix[c].get(b);
        }
        return result;
      }
      // Boundary inside the bucket: interpolate over its timerange (paper 5.3).
      long bs = summaryIndex.tsAt(b);
      long be = bucketEnd.get(b);
      double frac = (double) (t - bs + 1) / (double) (be - bs + 1);
      for (int c = 0; c < channels; c++) {
        double prev = b > 0 ? bucketPrefix[c].get(b - 1) : 0;
        result[c] = prev + frac * (bucketPrefix[c].get(b) - prev);
      }
      return result;
    }
    if (hasGuard) {
      if (t >= guardTs) {
        for (int c = 0; c < channels; c++) {
          result[c] = guardPrefix[c];
        }
        return result;
      }
      throw outOfHorizon(t);
    }
    return result;
  }

  private IllegalStateException outOfHorizon(long t) {
    return new IllegalStateException(
        "query boundary " + t + " precedes the retained horizon (expired data)");
  }

  /** Append one summary bucket to the second B-tree and its payload. */
  private void closeBucket(long bucket, long bucketWidth, int lastRecord) {
    summaryIndex.append(firstEverTs + bucket * bucketWidth);
    bucketEnd.addLast(firstEverTs + bucket * bucketWidth + bucketWidth - 1);
    for (int c = 0; c < channels; c++) {
      bucketPrefix[c].addLast(prefix[c].get(lastRecord));
    }
  }

  private void setGuard(int record) {
    hasGuard = true;
    guardTs = tsIndex.tsAt(record);
    for (int c = 0; c < channels; c++) {
      guardPrefix[c] = prefix[c].get(record);
    }
  }

  /** Drop records [0, from): payload ring heads advance, core sheds separators. */
  private void dropFront(int from) {
    for (int c = 0; c < channels; c++) {
      prefix[c].removeFirst(from);
    }
    tsIndex.dropFront(from);
  }
}
