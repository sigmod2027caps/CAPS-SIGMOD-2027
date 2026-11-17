package caps;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import temporalindex.CapsIndex;
import tieredindex.TwoTierIndex;

/**
 * Paper 5.2: two Tier-2 indexes side by side per shard: the full one (ground truth, the exact Tier 2
 * of the two-tier index) and a replica of the same ordered stream that expires on a 6h schedule.
 * Expiry acts on Tier 2 only, never on Tier 1 (which holds nothing older than the watermark).
 * In-horizon queries must match exactly. Same watermark-driven Tier 1 as every other index sink.
 */
public class ExpirySink<T> extends TieredIndexSink<T, ExpirySink.ExpiryShard> {

  private static final long serialVersionUID = 2L;

  /** Sample (and expire) every 6 hours of event time. */
  public static final long SAMPLE_EVERY_MS = 6 * 60 * 60 * 1000L;

  public static final int CHECK_QUERIES = 1000;

  private final long horizonMs;
  private final long queryLenMs;

  /** horizonMs: the expiry horizon in event time; queryLenMs: the length of the in-horizon check queries. */
  public ExpirySink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn, int channels,
                    String benchDir, long horizonMs, long queryLenMs) {
    super(tsFn, hmFn, channels, benchDir, "expiry", queryLenMs);
    this.horizonMs = horizonMs;
    this.queryLenMs = queryLenMs;
  }

  @Override
  protected boolean checkpointable() {
    return false; // the expiring / summarised replicas are not part of the checkpoint
  }

  @Override
  protected ExpiryShard newShard(int subtask, TwoTierIndex index) {
    return new ExpiryShard(subtask, index, channels, horizonMs);
  }

  /** The expiring replica and its sampling schedule; fed by the migration listener, so in timestamp order. */
  public static final class ExpiryShard extends TieredIndexSink.Shard {
    final CapsIndex expiring;
    final long horizonMs;
    final long[] row;
    long recordsTotal;
    long lastSampleTs = Long.MIN_VALUE;
    final List<long[]> samples = new ArrayList<>();
    long expiryCalls;
    long expiryNanos;

    ExpiryShard(int subtask, TwoTierIndex index, int channels, long horizonMs) {
      super(subtask, index);
      this.expiring = new CapsIndex(channels);
      this.horizonMs = horizonMs;
      this.row = new long[channels];
    }

    @Override
    public void migrated(long ts, long[] vals, int off) {
      System.arraycopy(vals, off, row, 0, row.length);
      expiring.append(ts, row);
      recordsTotal++;
      if (lastSampleTs == Long.MIN_VALUE || ts - lastSampleTs >= SAMPLE_EVERY_MS) {
        expire(ts - horizonMs);
        samples.add(new long[] {ts, index.tier2().memoryBytes(), expiring.memoryBytes()});
        lastSampleTs = ts;
      }
    }

    void expire(long cutoff) {
      long t0 = System.nanoTime();
      expiring.expireBefore(cutoff);
      expiryNanos += System.nanoTime() - t0;
      expiryCalls++;
    }
  }

  @Override
  protected void finish(List<ExpiryShard> shards) throws Exception {
    long maxSeenTs = maxTs(shards);
    long cutoff = maxSeenTs - horizonMs;
    for (ExpiryShard s : shards) {
      s.expire(cutoff);
      s.samples.add(new long[] {maxSeenTs, s.index.tier2().memoryBytes(), s.expiring.memoryBytes()});
    }

    // Queries fully inside the retained horizon must be exact despite expiry.
    // Fixed window length (ten slides of the dataflow's window, same as the
    // query-latency experiment), uniformly random start inside the horizon.
    Random rnd = new Random(42);
    long len = Math.min(queryLenMs, maxSeenTs - cutoff);
    long[] qFrom = new long[CHECK_QUERIES];
    long[] qTo = new long[CHECK_QUERIES];
    for (int i = 0; i < CHECK_QUERIES; i++) {
      long start = cutoff + (long) (rnd.nextDouble() * (maxSeenTs - cutoff - len));
      qFrom[i] = start;
      qTo[i] = start + len;
    }
    int mismatches = 0;
    int defined = 0;
    for (ExpiryShard s : shards) { // verification + JIT warm-up, per shard (both indexes hold the same records)
      CapsIndex full = s.index.tier2();
      for (int i = 0; i < CHECK_QUERIES; i++) {
        double[] expected = full.howMuch(qFrom[i], qTo[i]);
        double[] actual = s.expiring.howMuch(qFrom[i], qTo[i]);
        for (int c = 0; c < channels; c++)
          if (Math.round(expected[c]) != Math.round(actual[c]))
            mismatches++;
        if (full.countIn(qFrom[i], qTo[i]) != s.expiring.countIn(qFrom[i], qTo[i]))
          mismatches++;
      }
    }
    if (mismatches > 0)
      throw new IllegalStateException(
          mismatches + " query mismatches between full and expiring index");
    // Finality: after the end of the input every shard is final over the whole range; check through the two-tier API.
    int finals = 0;
    tieredindex.ShardedIndex merged = assemble(shards);
    for (int i = 0; i < CHECK_QUERIES; i++) {
      tieredindex.Answer a = merged.query(qFrom[i], qTo[i]);
      finals += a.isFinal ? 1 : 0;
      defined += a.defined ? 1 : 0;
    }
    queryStatus("expiry_check", CHECK_QUERIES, finals, defined);

    long fullQueryNanos = 0;
    long expiringQueryNanos = 0;
    for (ExpiryShard s : shards) { // timed pass, per index and shard
      CapsIndex full = s.index.tier2();
      for (int i = 0; i < CHECK_QUERIES; i++) {
        long t0 = System.nanoTime();
        full.howMuch(qFrom[i], qTo[i]);
        long t1 = System.nanoTime();
        s.expiring.howMuch(qFrom[i], qTo[i]);
        long t2 = System.nanoTime();
        fullQueryNanos += t1 - t0;
        expiringQueryNanos += t2 - t1;
      }
    }

    long recordsTotal = 0, retained = 0, fullBytes = 0, expiringBytes = 0, expiryCalls = 0, expiryNanos = 0;
    for (ExpiryShard s : shards) {
      recordsTotal += s.recordsTotal;
      retained += s.expiring.exactRecords();
      fullBytes += s.index.tier2().memoryBytes();
      expiringBytes += s.expiring.memoryBytes();
      expiryCalls += s.expiryCalls;
      expiryNanos += s.expiryNanos;
    }
    long shardQueries = (long) CHECK_QUERIES * shards.size();
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/expiry_memory.csv"))) {
      pw.println("event_ts,full_bytes,expiring_bytes,shard");
      for (ExpiryShard s : shards)
        for (long[] x : s.samples)
          pw.println(x[0] + "," + x[1] + "," + x[2] + "," + s.subtask);
    }
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/expiry_stats.csv"))) {
      pw.println("horizon_ms,records_total,records_retained,full_bytes,"
          + "expiring_bytes,queries_checked,mismatches,expiry_calls,avg_expiry_us,"
          + "full_query_us,expiring_query_us");
      pw.printf(Locale.ROOT, "%d,%d,%d,%d,%d,%d,%d,%d,%.3f,%.3f,%.3f%n",
          horizonMs, recordsTotal, retained, fullBytes, expiringBytes,
          CHECK_QUERIES, mismatches, expiryCalls,
          expiryNanos / 1000.0 / Math.max(1, expiryCalls),
          fullQueryNanos / 1000.0 / shardQueries,
          expiringQueryNanos / 1000.0 / shardQueries);
    }
    System.out.println("ExpirySink: " + recordsTotal + " records in " + shards.size()
        + " shard(s), " + retained + " retained after expiry (horizon "
        + horizonMs + " ms), " + CHECK_QUERIES + " in-horizon queries exact, "
        + expiryCalls + " expiry calls avg "
        + String.format(Locale.ROOT, "%.1f",
            expiryNanos / 1000.0 / Math.max(1, expiryCalls)) + " us");
  }
}
