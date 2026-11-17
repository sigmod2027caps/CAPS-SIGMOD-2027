package caps;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import temporalindex.CapsIndex;
import tieredindex.TwoTierIndex;

/**
 * Paper 5.3 online: one summarising Tier-2 replica per bucket width in every shard, summarize on a
 * fixed 6h event-time schedule. The replicas are fed by the same ordered stream the two-tier index
 * migrates into its (exact, never summarised) Tier 2, which doubles as the uncompressed baseline.
 * Summarisation acts on Tier 2 only. QUERY_WINDOW_MS is fixed so every width sees the same queries;
 * error differences are from width alone. Writes summarize.csv; bucket_ms=0 row is the uncompressed
 * baseline. With several shards the estimate of a query is the sum of the shard estimates.
 */
public class SummarizeSink<T> extends TieredIndexSink<T, SummarizeSink.SummarizeShard> {

  private static final long serialVersionUID = 2L;

  public static final int NUM_QUERIES = 1000;

  /** Fixed query window length: the application's query granularity,
   * independent of the bucket width (1 day). */
  public static final long QUERY_WINDOW_MS = 24 * 60 * 60 * 1000L;

  /** Fixed compression schedule (event time), independent of bucket width. */
  public static final long SUMMARIZE_PERIOD_MS = 6 * 60 * 60 * 1000L;

  private final long horizonMs;
  private final long[] bucketWidthsMs;

  /** One live index per bucket width in bucketWidthsMs (plus the uncompressed baseline) behind a horizon of horizonMs. */
  public SummarizeSink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn, int channels,
                       String benchDir, long horizonMs, long[] bucketWidthsMs) {
    super(tsFn, hmFn, channels, benchDir, "summarize", QUERY_WINDOW_MS);
    this.horizonMs = horizonMs;
    this.bucketWidthsMs = bucketWidthsMs;
  }

  @Override
  protected boolean checkpointable() {
    return false; // the expiring / summarised replicas are not part of the checkpoint
  }

  @Override
  protected SummarizeShard newShard(int subtask, TwoTierIndex index) {
    return new SummarizeShard(subtask, index, channels, horizonMs, bucketWidthsMs);
  }

  /** One online summarising index per bucket width, summarized as the stream advances. */
  public static final class SummarizeShard extends TieredIndexSink.Shard {
    final CapsIndex[] online;
    final long horizonMs;
    final long[] widths;
    final long[] row;
    long seen;
    long lastSummarizeTs = Long.MIN_VALUE;
    final long[] summarizeNanos;
    final int[] summarizeCalls;

    SummarizeShard(int subtask, TwoTierIndex index, int channels, long horizonMs, long[] widths) {
      super(subtask, index);
      this.horizonMs = horizonMs;
      this.widths = widths;
      this.row = new long[channels];
      this.online = new CapsIndex[widths.length];
      this.summarizeNanos = new long[widths.length];
      this.summarizeCalls = new int[widths.length];
      for (int i = 0; i < widths.length; i++)
        online[i] = new CapsIndex(channels);
    }

    /**
     * Appends a (time-ordered) record to every online index; every SUMMARIZE_PERIOD_MS of event time it
     * compresses, in every index, the complete buckets that fell behind the horizon, so summarization
     * happens WHILE the stream runs (paper 5.3). Attempts that find no complete bucket (widths larger
     * than the schedule) are no-ops inside the index and are not counted.
     */
    @Override
    public void migrated(long ts, long[] vals, int off) {
      System.arraycopy(vals, off, row, 0, row.length);
      if (seen++ == 0)
        lastSummarizeTs = ts;
      for (CapsIndex idx : online)
        idx.append(ts, row);
      if (ts - lastSummarizeTs >= SUMMARIZE_PERIOD_MS) {
        lastSummarizeTs = ts;
        summarizeAll(ts - horizonMs);
      }
    }

    /** Timed compression of every index; only effective calls are counted. */
    void summarizeAll(long cutoff) {
      for (int i = 0; i < online.length; i++) {
        int before = online[i].exactRecords();
        long start = System.nanoTime();
        online[i].summarizeBefore(cutoff, widths[i]);
        long nanos = System.nanoTime() - start;
        if (online[i].exactRecords() != before) {
          summarizeNanos[i] += nanos;
          summarizeCalls[i]++;
        }
      }
    }
  }

  @Override
  protected void finish(List<SummarizeShard> shards) throws Exception {
    long records = 0;
    for (SummarizeShard s : shards)
      records += s.index.tier2Size();

    // Final compression up to the end-of-stream horizon (same call the
    // online path makes, so it is timed and counted the same way).
    long horizon = maxTs(shards) - horizonMs;
    for (SummarizeShard s : shards)
      s.summarizeAll(horizon);

    CapsIndex[] full = new CapsIndex[shards.size()]; // exact baseline: the Tier 2 of each shard
    for (int i = 0; i < full.length; i++)
      full[i] = shards.get(i).index.tier2();

    // Query ranges fully inside the summarized region for EVERY width: the
    // horizon is snapped down to a bucket boundary, so stay one max-width away.
    long maxWidth = 0;
    for (long w : bucketWidthsMs)
      maxWidth = Math.max(maxWidth, w);
    // Stay one max-width inside the summarized region so queries do not straddle a bucket edge.
    long queryMax = horizon - maxWidth;
    long minTs = Long.MAX_VALUE;
    for (CapsIndex f : full)
      minTs = Math.min(minTs, f.firstTs());

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
      exact[i] = sum(full, start, start + len);
    }
    queryStatus("summarize_exact", NUM_QUERIES, NUM_QUERIES, NUM_QUERIES);

    long exactRecordsAll = 0, exactBytes = 0, summaryBytes = 0, memBytes = 0;
    for (CapsIndex f : full) {
      exactRecordsAll += f.exactRecords();
      exactBytes += f.exactBytes();
      summaryBytes += f.summaryBytes();
      memBytes += f.memoryBytes();
    }
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/summarize.csv"))) {
      pw.println("bucket_ms,records,exact_records,buckets,avg_tuples_per_bucket,"
          + "exact_bytes,summary_bytes,index_bytes,summarize_calls,avg_summarize_us,"
          + "avg_query_us,avg_rel_err_pct,max_rel_err_pct");
      // Baseline: uncompressed index (the exact[] pass above was the warmup).
      pw.printf(Locale.ROOT, "0,%d,%d,0,0.0,%d,%d,%d,0,0.000,%.3f,0.000,0.000%n",
          records, exactRecordsAll, exactBytes, summaryBytes, memBytes,
          timeQueries(full, queries));
      for (int w = 0; w < bucketWidthsMs.length; w++) {
        CapsIndex[] idxs = new CapsIndex[shards.size()];
        long exactRecs = 0, buckets = 0, exB = 0, suB = 0, memB = 0, nanos = 0;
        int calls = 0;
        for (int s = 0; s < idxs.length; s++) {
          SummarizeShard sh = shards.get(s);
          idxs[s] = sh.online[w];
          exactRecs += idxs[s].exactRecords();
          buckets += idxs[s].summaryBuckets();
          exB += idxs[s].exactBytes();
          suB += idxs[s].summaryBytes();
          memB += idxs[s].memoryBytes();
          nanos += sh.summarizeNanos[w];
          calls += sh.summarizeCalls[w];
        }
        double sumErr = 0;
        double maxErr = 0;
        for (int i = 0; i < NUM_QUERIES; i++) {
          double[] est = sum(idxs, queries[i][0], queries[i][1]);
          for (int c = 0; c < channels; c++) {
            double err = Math.abs(est[c] - exact[i][c]) / Math.max(1.0, exact[i][c]);
            sumErr += err;
            maxErr = Math.max(maxErr, err);
          }
        }
        double avgErr = sumErr / (NUM_QUERIES * channels);
        double avgSummarizeUs = calls == 0 ? 0 : nanos / 1000.0 / calls;
        double tuplesPerBucket = buckets == 0 ? 0 : (double) (records - exactRecs) / buckets;
        pw.printf(Locale.ROOT, "%d,%d,%d,%d,%.1f,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f%n",
            bucketWidthsMs[w], records, exactRecs, buckets, tuplesPerBucket, exB, suB, memB,
            calls, avgSummarizeUs, timeQueries(idxs, queries), avgErr * 100, maxErr * 100);
      }
    }
    System.out.println("SummarizeSink: " + records + " records in " + shards.size()
        + " shard(s), " + bucketWidthsMs.length + " bucket widths summarized online, "
        + NUM_QUERIES + " queries each vs exact index");
  }

  /** Channel-wise sum of the shard answers (the shards partition the outputs). */
  private double[] sum(CapsIndex[] idxs, long from, long to) {
    if (idxs.length == 1)
      return idxs[0].howMuch(from, to);
    double[] total = new double[channels];
    for (CapsIndex idx : idxs) {
      double[] x = idx.howMuch(from, to);
      for (int c = 0; c < channels; c++)
        total[c] += x[c];
    }
    return total;
  }

  /** Average time (us) to answer the queries on the given indexes (summed over the shards). The caller
   * already ran the same queries once (error pass), which serves as warmup. */
  private double timeQueries(CapsIndex[] idxs, long[][] queries) {
    double checksum = 0;
    long start = System.nanoTime();
    for (long[] q : queries)
      checksum += sum(idxs, q[0], q[1])[0];
    long nanos = System.nanoTime() - start;
    if (checksum == Double.MIN_VALUE)
      throw new IllegalStateException("unreachable"); // keep the loop live
    return nanos / 1000.0 / queries.length;
  }
}
