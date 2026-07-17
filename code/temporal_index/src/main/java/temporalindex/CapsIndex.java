package temporalindex;

import temporalindex.TemporalIndex.LongRing;

public class CapsIndex {

  
  public static final int CAPACITY = TemporalIndex.CAPACITY;

  private final int channels;

  
  private final TemporalIndex tsIndex = new TemporalIndex();

  
  
  private final LongRing[] prefix;

  
  private final long[] running;

  private long firstEverTs = Long.MAX_VALUE;

  
  
  private boolean hasGuard;
  private long guardTs;
  private long[] guardPrefix;

  
  
  
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

  
  public long firstTs() {
    return firstEverTs;
  }

  
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

  
  public double[] howMuch(long tsFrom, long tsTo) {
    if (tsFrom > tsTo) {
      throw new IllegalArgumentException("empty range: [" + tsFrom + ", " + tsTo + "]");
    }
    double[] hi = estPrefix(tsTo);
    double[] lo = estPrefix(tsFrom - 1);
    double[] result = new double[channels];
    for (int c = 0; c < channels; c++) {
      result[c] = hi[c] - lo[c];
    }
    return result;
  }

  
  public double[] deltaHowMuch(long ts1, long te1, long ts2, long te2) {
    double[] a = howMuch(ts1, te1);
    double[] b = howMuch(ts2, te2);
    double[] result = new double[channels];
    for (int c = 0; c < channels; c++) {
      result[c] = b[c] - a[c];
    }
    return result;
  }

  
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

  
  public double[] deltaShare(long ts1, long te1, long ts2, long te2) {
    double[] a = share(ts1, te1);
    double[] b = share(ts2, te2);
    double[] result = new double[channels];
    for (int c = 0; c < channels; c++) {
      result[c] = b[c] - a[c];
    }
    return result;
  }

  
  public void expireBefore(long horizon) {
    int k = tsIndex.lowerBound(horizon);
    if (k == 0) {
      return;
    }
    setGuard(k - 1);
    dropFront(k);
  }

  
  public void summarizeBefore(long horizon, long bucketWidth) {
    if (firstEverTs == Long.MAX_VALUE) {
      return;
    }
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

  
  public long exactBytes() {
    long entries = (long) tsIndex.size() * (1 + channels);
    entries += tsIndex.separatorEntries();
    return entries * 8;
  }

  
  public long summaryBytes() {
    long entries = (long) summaryIndex.size() * (2 + channels);
    entries += summaryIndex.separatorEntries();
    return entries * 8;
  }

  
  public long memoryBytes() {
    return exactBytes() + summaryBytes();
  }

  

  
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
      int b = summaryIndex.search(t); 
      if (b < 0) {
        return result;
      }
      if (t >= bucketEnd.get(b)) {
        for (int c = 0; c < channels; c++) {
          result[c] = bucketPrefix[c].get(b);
        }
        return result;
      }
      
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

  
  private void dropFront(int from) {
    for (int c = 0; c < channels; c++) {
      prefix[c].removeFirst(from);
    }
    tsIndex.dropFront(from);
  }
}
