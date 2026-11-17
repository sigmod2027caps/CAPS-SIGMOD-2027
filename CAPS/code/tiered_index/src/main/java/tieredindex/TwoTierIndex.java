package tieredindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import temporalindex.CapsIndex;

/**
 * Two-tier index of one sink subtask (paper: "Time, order, recovery, and indexing").
 * Tier 1: records ahead of the watermark (pluggable: HybridTier1 = ordered run + AugmentedRBTree, the default;
 * AugmentedRBTree alone; or SortedBufferTier1).
 * Tier 2: append-only prefix-sum CapsIndex holding every record with ts &lt;= watermark.
 * advanceWatermark(W) moves the Tier-1 prefix &lt;= W into Tier 2, in timestamp order, so Tier 2
 * never sees an out-of-order append. A record with ts &lt;= W is late: counted, not indexed.
 * Stores: the annotations (M counters) of every output, ahead of the watermark in Tier 1 and at or below it in
 * Tier 2 as prefix sums. Complexity: add O(M log n) (tree) or O(M) amortized (ordered run); advanceWatermark
 * O(M) per migrated record; query O(log n + M) final, plus O(M log n) over Tier 1 if provisional.
 * "Does any output exist in [t1, t2]" (the undefined / bottom answer) comes from the record
 * positions of Tier 2 (CapsIndex.countIn) and the record count of Tier 1, not from an extra channel.
 */
public class TwoTierIndex {

  /** Called, in timestamp order, once per record that migrates, after it was appended to Tier 2. */
  public interface Listener {
    /** The record's counts are vals[off .. off + channels). */
    void migrated(long ts, long[] vals, int off);
  }

  final int channels;
  final Tier1 tier1;
  final CapsIndex tier2;
  long watermark = Long.MIN_VALUE;
  final long[] row; // scratch row for Tier-2 appends

  /** Optional replicas of the ordered stream (scan baseline, expiring or summarised copies). */
  public Listener listener;

  // Stats (public for the experiments).
  public boolean timing = false; // true: System.nanoTime around every Tier-1 insert and split (explicit; never in a timed run)
  public long lateCount;
  public long migratedCount;
  public long splitNanos; // Tier-1 split, per migrating watermark
  public long migrationNanos; // Tier-2 appends, per migrating watermark
  public long listenerNanos; // listener calls, per migrating watermark
  public long insertNanos; // Tier-1 inserts
  public int peakTier1;

  /** An empty index for M = channels; tier1 is the (empty) structure for the uncertified outputs. */
  public TwoTierIndex(int channels, Tier1 tier1) {
    this.channels = channels;
    this.tier1 = tier1;
    this.tier2 = new CapsIndex(channels);
    this.row = new long[channels];
  }

  public int channels() {
    return channels;
  }

  public long watermark() {
    return watermark;
  }

  public int tier1Size() {
    return tier1.size();
  }

  public int tier2Size() {
    return tier2.exactRecords();
  }

  /** Tier 2 itself (exact, final records only): for memory accounting and for expiry / summarisation experiments. */
  public CapsIndex tier2() {
    return tier2;
  }

  /** Index one output. Returns false (and counts it) if it is late; the caller routes it to the side output. */
  public boolean add(long ts, long[] v) {
    if (ts <= watermark) {
      lateCount++;
      return false;
    }
    if (timing) {
      long t0 = System.nanoTime();
      tier1.insert(ts, v);
      insertNanos += System.nanoTime() - t0;
    } else
      tier1.insert(ts, v);
    if (tier1.size() > peakTier1)
      peakTier1 = tier1.size();
    return true;
  }

  /**
   * Monotone: a watermark that does not advance is ignored. Certifies and migrates everything <= w.
   * A watermark that certifies nothing (Tier 1 starts above it) costs O(1), which matters because
   * a punctuated source emits one per record.
   */
  public void advanceWatermark(long w) {
    if (w <= watermark)
      return;
    watermark = w;
    if (tier1.minTs() > w)
      return;
    long t0 = timing ? System.nanoTime() : 0;
    Tier1.Entries e = tier1.splitPrefix(w);
    long t1 = timing ? System.nanoTime() : 0;
    for (int i = 0; i < e.n; i++) {
      System.arraycopy(e.vals, i * channels, row, 0, channels);
      tier2.append(e.ts[i], row);
    }
    long t2 = timing ? System.nanoTime() : 0;
    if (listener != null)
      for (int i = 0; i < e.n; i++)
        listener.migrated(e.ts[i], e.vals, i * channels);
    migratedCount += e.n;
    if (timing) {
      splitNanos += t1 - t0;
      migrationNanos += t2 - t1;
      listenerNanos += System.nanoTime() - t2;
    }
  }

  /**
   * HMP over [t1, t2]. Final iff t2 <= watermark. Otherwise Tier 2 answers [t1, min(t2, W)]
   * and Tier 1 the part in (W, t2] (provisional). Cost O(log N + M) final, plus O(M log n) provisional.
   */
  public Answer query(long t1, long t2) {
    long[] out = new long[channels];
    boolean fin = t2 <= watermark;
    if (t1 > t2)
      return new Answer(out, fin, false);
    boolean any = false;
    long hi = Math.min(t2, watermark);
    if (t1 <= hi) {
      double[] hp = tier2.howMuch(t1, hi);
      long total = 0;
      for (int c = 0; c < channels; c++) {
        out[c] = Math.round(hp[c]);
        total += out[c];
      }
      // counts are non-negative, so a positive total proves a record; only an all-zero answer needs the positions
      any = total > 0 || tier2.countIn(t1, hi) > 0;
    }
    if (!fin) {
      long from = Math.max(t1, watermark + 1); // watermark < t2 <= Long.MAX_VALUE
      any |= tier1.rangeSum(from, t2, out) > 0;
    }
    return new Answer(out, fin, any);
  }

  /** Absolute and relative versioning of intervals a = [a1, a2] and b = [b1, b2]. */
  public Answer.Version versioning(long a1, long a2, long b1, long b2) {
    return new Answer.Version(query(a1, a2), query(b1, b2));
  }

  // Expiry and summarisation act on Tier 2 only (Tier 1 holds nothing older than the watermark).

  /** Drops Tier-2 records older than the horizon (keeps a guard row, so in-horizon answers are unchanged). */
  public void expireBefore(long horizon) {
    tier2.expireBefore(horizon);
  }

  /** Compresses Tier-2 records older than the horizon into buckets of the given width (approximate answers there). */
  public void summarizeBefore(long horizon, long bucketWidth) {
    tier2.summarizeBefore(horizon, bucketWidth);
  }

  // checkpoint and restore
  // The state of the index is: Tier 1 (the records ahead of the watermark, bounded by the watermark
  // lag), Tier 2 (an append-only list of records; its prefix sums are re-derived by re-appending),
  // the watermark and the counters. The sink writes Tier 2 as immutable segments, so only the
  // records migrated since the previous checkpoint are written; Tier 1 and the counters go into
  // the operator state of the checkpoint itself.

  /** Tier 1 of the checkpoint: every record that no watermark has certified yet, in order. */
  public Tier1.Entries tier1Entries() {
    return tier1.entries();
  }

  /** Copies the Tier-2 records [from, to) out: ts[i], vals[i * channels + c] (per-record counts). */
  public void exportTier2(int from, int to, long[] ts, long[] vals) {
    tier2.exportRecords(from, to, ts, vals);
  }

  /** Restore: appends n records to Tier 2 and feeds the listener, as a migration would. */
  public void restoreTier2(long[] ts, long[] vals, int n) {
    for (int i = 0; i < n; i++) {
      System.arraycopy(vals, i * channels, row, 0, channels);
      tier2.append(ts[i], row);
    }
    if (listener != null)
      for (int i = 0; i < n; i++)
        listener.migrated(ts[i], vals, i * channels);
  }

  /** Restore: inserts the records into Tier 1 in their stored order (equal timestamps keep their order). */
  public void restoreTier1(Tier1.Entries e) {
    for (int i = 0; i < e.n; i++) {
      System.arraycopy(e.vals, i * channels, row, 0, channels);
      tier1.insert(e.ts[i], row);
    }
  }

  /** The watermark and the counters (everything of the index except Tier 1 and Tier 2). */
  public void writeState(DataOutput out) throws IOException {
    out.writeLong(watermark);
    out.writeLong(lateCount);
    out.writeLong(migratedCount);
    out.writeLong(splitNanos);
    out.writeLong(migrationNanos);
    out.writeLong(listenerNanos);
    out.writeLong(insertNanos);
    out.writeInt(peakTier1);
  }

  public void readState(DataInput in) throws IOException {
    watermark = in.readLong();
    lateCount = in.readLong();
    migratedCount = in.readLong();
    splitNanos = in.readLong();
    migrationNanos = in.readLong();
    listenerNanos = in.readLong();
    insertNanos = in.readLong();
    peakTier1 = in.readInt();
  }

  /**
   * A 64-bit digest of the Tier-2 records: the sum, over the records, of a mix of the timestamp and the counts.
   * It does not depend on the order of records with equal timestamps (the window firing order of the keys of one
   * window is not fixed, and no answer depends on it), but a record applied twice or lost changes it.
   */
  public long tier2Digest() {
    final int chunk = 1 << 14;
    int n = tier2.exactRecords();
    long[] ts = new long[Math.min(n, chunk)];
    long[] vals = new long[ts.length * channels];
    long sum = 0;
    for (int from = 0; from < n; from += chunk) {
      int to = Math.min(n, from + chunk);
      tier2.exportRecords(from, to, ts, vals);
      for (int i = 0; i < to - from; i++) {
        long x = ts[i] * 0x9E3779B97F4A7C15L;
        for (int c = 0; c < channels; c++) {
          x = (x ^ vals[i * channels + c]) * 0xC4CEB9FE1A85EC53L;
          x ^= x >>> 32;
        }
        x *= 0xFF51AFD7ED558CCDL;
        x ^= x >>> 29;
        sum += x;
      }
    }
    return sum;
  }
}
