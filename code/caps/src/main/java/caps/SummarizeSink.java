package caps;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import temporalindex.ArrayStore;
import temporalindex.CapsIndex;

// Paper 5.3 online: one CapsIndex per bucket width, summarize on a fixed 6h event-time schedule.
// QUERY_WINDOW_MS is fixed so every width sees the same queries; error differences are from width alone.
// Writes summarize.csv; bucket_ms=0 row is the uncompressed baseline rebuilt offline.
public class SummarizeSink<T> extends RichSinkFunction<T> {

  private static final long serialVersionUID = 1L;

  public static final int NUM_QUERIES = 1000;

  /** Fixed query window length: the application's query granularity,
   * independent of the bucket width (1 day). */
  public static final long QUERY_WINDOW_MS = 24 * 60 * 60 * 1000L;

  /** Fixed compression schedule (event time), independent of bucket width. */
  public static final long SUMMARIZE_PERIOD_MS = 6 * 60 * 60 * 1000L;

  private final IndexingSink.TsFn<T> tsFn;
  private final IndexingSink.HmFn<T> hmFn;
  private final int channels;
  private final String benchDir;
  private final long horizonMs;
  private final long[] bucketWidthsMs;
  private final long slackMs;

  private transient ArrayStore raw;
  private transient PriorityQueue<Entry> reorder;
  private transient long maxSeenTs;

  // One online index per bucket width, summarized as the stream advances.
  private transient CapsIndex[] online;
  private transient long firstTs;
  private transient long lastSummarizeTs;
  private transient long[] summarizeNanos;
  private transient int[] summarizeCalls;

  public SummarizeSink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn, int channels,
                       String benchDir, long horizonMs, long[] bucketWidthsMs, long slackMs) {
    this.tsFn = tsFn;
    this.hmFn = hmFn;
    this.channels = channels;
    this.benchDir = benchDir;
    this.horizonMs = horizonMs;
    this.bucketWidthsMs = bucketWidthsMs;
    this.slackMs = slackMs;
  }

  @Override
  public void open(Configuration parameters) {
    raw = new ArrayStore(channels);
    reorder = new PriorityQueue<>();
    maxSeenTs = Long.MIN_VALUE;
    firstTs = Long.MIN_VALUE;
    online = new CapsIndex[bucketWidthsMs.length];
    lastSummarizeTs = Long.MIN_VALUE;
    summarizeNanos = new long[bucketWidthsMs.length];
    summarizeCalls = new int[bucketWidthsMs.length];
    for (int i = 0; i < bucketWidthsMs.length; i++) {
      online[i] = new CapsIndex(channels);
    }
  }

  @Override
  public void invoke(T value, Context context) {
    long ts = tsFn.ts(value);
    reorder.add(new Entry(ts, hmFn.hm(value).counts.clone()));
    maxSeenTs = Math.max(maxSeenTs, ts);
    while (!reorder.isEmpty() && reorder.peek().ts <= maxSeenTs - slackMs) {
      release(reorder.peek().ts, reorder.poll().counts);
    }
  }

  /**
   * Appends a (time-ordered) record to the raw store and to every online
   * index; every {@link #SUMMARIZE_PERIOD_MS} of event time it compresses,
   * in every index, the complete buckets that fell behind the horizon, so
   * summarization happens WHILE the stream runs (paper 5.3). Attempts that
   * find no complete bucket (widths larger than the schedule) are no-ops
   * inside the index and are not counted.
   */
  private void release(long ts, long[] counts) {
    if (raw.records() == 0) {
      firstTs = ts;
      lastSummarizeTs = ts;
    }
    raw.append(ts, counts);
    for (int i = 0; i < online.length; i++) {
      online[i].append(ts, counts);
    }
    if (ts - lastSummarizeTs >= SUMMARIZE_PERIOD_MS) {
      lastSummarizeTs = ts;
      summarizeAll(ts - horizonMs);
    }
  }

  /** Timed compression of every index; only effective calls are counted. */
  private void summarizeAll(long cutoff) {
    for (int i = 0; i < online.length; i++) {
      int before = online[i].exactRecords();
      long start = System.nanoTime();
      online[i].summarizeBefore(cutoff, bucketWidthsMs[i]);
      long nanos = System.nanoTime() - start;
      if (online[i].exactRecords() != before) {
        summarizeNanos[i] += nanos;
        summarizeCalls[i]++;
      }
    }
  }

  @Override
  public void close() throws Exception {
    while (!reorder.isEmpty()) {
      release(reorder.peek().ts, reorder.poll().counts);
    }
    if (raw.records() == 0) {
      throw new IllegalStateException("no records reached the summarize sink");
    }

    // Final compression up to the end-of-stream horizon (same call the
    // online path makes, so it is timed and counted the same way).
    long horizon = maxSeenTs - horizonMs;
    summarizeAll(horizon);

    // Exact baseline, rebuilt offline from the raw records (verification only).
    CapsIndex full = rebuild();

    // Query ranges fully inside the summarized region for EVERY width: the
    // horizon is snapped down to a bucket boundary, so stay one max-width away.
    long maxWidth = 0;
    for (long w : bucketWidthsMs) {
      maxWidth = Math.max(maxWidth, w);
    }
    // Stay one max-width inside the summarized region so queries do not straddle a bucket edge.
    long queryMax = horizon - maxWidth;
    long minTs = full.firstTs();

    // ONE query set shared by every bucket width: fixed window length
    // (QUERY_WINDOW_MS, clamped to the summarized region), uniformly random
    // start. Exact answers from the uncompressed index.
    long len = Math.min(QUERY_WINDOW_MS, queryMax - minTs);
    long[][] queries = new long[NUM_QUERIES][2];
    double[][] exact = new double[NUM_QUERIES][];
    Random rnd = new Random(42);
    for (int i = 0; i < NUM_QUERIES; i++) {
      long start = minTs + (long) (rnd.nextDouble() * (queryMax - minTs - len));
      queries[i][0] = start;
      queries[i][1] = start + len;
      exact[i] = full.howMuch(start, start + len);
    }

    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/summarize.csv"))) {
      pw.println("bucket_ms,records,exact_records,buckets,avg_tuples_per_bucket,"
          + "exact_bytes,summary_bytes,index_bytes,summarize_calls,avg_summarize_us,"
          + "avg_query_us,avg_rel_err_pct,max_rel_err_pct");
      // Baseline: uncompressed index (the exact[] pass above was the warmup).
      pw.printf(Locale.ROOT, "0,%d,%d,0,0.0,%d,%d,%d,0,0.000,%.3f,0.000,0.000%n",
          raw.records(), full.exactRecords(), full.exactBytes(), full.summaryBytes(),
          full.memoryBytes(), timeQueries(full, queries));
      for (int w = 0; w < bucketWidthsMs.length; w++) {
        CapsIndex idx = online[w];
        double sumErr = 0;
        double maxErr = 0;
        for (int i = 0; i < NUM_QUERIES; i++) {
          double[] est = idx.howMuch(queries[i][0], queries[i][1]);
          for (int c = 0; c < channels; c++) {
            double err = Math.abs(est[c] - exact[i][c]) / Math.max(1.0, exact[i][c]);
            sumErr += err;
            maxErr = Math.max(maxErr, err);
          }
        }
        double avgErr = sumErr / (NUM_QUERIES * channels);
        double avgSummarizeUs = summarizeCalls[w] == 0
            ? 0 : summarizeNanos[w] / 1000.0 / summarizeCalls[w];
        double tuplesPerBucket = idx.summaryBuckets() == 0
            ? 0 : (double) (raw.records() - idx.exactRecords()) / idx.summaryBuckets();
        pw.printf(Locale.ROOT, "%d,%d,%d,%d,%.1f,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f%n",
            bucketWidthsMs[w], raw.records(), idx.exactRecords(), idx.summaryBuckets(),
            tuplesPerBucket, idx.exactBytes(), idx.summaryBytes(), idx.memoryBytes(),
            summarizeCalls[w], avgSummarizeUs,
            timeQueries(idx, queries), avgErr * 100, maxErr * 100);
      }
    }
    System.out.println("SummarizeSink: " + raw.records() + " records, "
        + bucketWidthsMs.length + " bucket widths summarized online, "
        + NUM_QUERIES + " queries each vs exact index");
  }

  /** Average time (µs) to answer the queries on the given index. The caller
   * already ran the same queries once (error pass), which serves as warmup. */
  private double timeQueries(CapsIndex idx, long[][] queries) {
    double checksum = 0;
    long start = System.nanoTime();
    for (long[] q : queries) {
      checksum += idx.howMuch(q[0], q[1])[0];
    }
    long nanos = System.nanoTime() - start;
    if (checksum == Double.MIN_VALUE) {
      throw new IllegalStateException("unreachable");  // keep the loop live
    }
    return nanos / 1000.0 / queries.length;
  }

  /** Fresh exact index from the raw records. */
  private CapsIndex rebuild() {
    CapsIndex idx = new CapsIndex(channels);
    long[] counts = new long[channels];
    for (int i = 0; i < raw.records(); i++) {
      raw.countsAt(i, counts);
      idx.append(raw.tsAt(i), counts);
    }
    return idx;
  }

  private static final class Entry implements Comparable<Entry> {
    final long ts;
    final long[] counts;

    Entry(long ts, long[] counts) {
      this.ts = ts;
      this.counts = counts;
    }

    @Override
    public int compareTo(Entry other) {
      return Long.compare(ts, other.ts);
    }
  }
}
