package tieredindex;

/**
 * Tier 1: holds the records no watermark has certified yet. Accepts out-of-order insertions,
 * answers range sums over them, and hands a timestamp prefix to Tier 2 when the watermark moves.
 * Three implementations: AugmentedRBTree (O(M log n) everything, any arrival order), SortedBufferTier1 (the naive one)
 * and HybridTier1 (an ordered run with prefix sums for in-order arrivals, the tree for the rest: the default).
 * The Tier-1 contract (Paper Section 5.4).
 */
public interface Tier1 {

  /** Insert a record; v (length M) is copied. Equal timestamps are allowed. */
  void insert(long ts, long[] v);

  /**
   * Add the per-channel sums of all records with t1 <= ts <= t2 into out (length M)
   * and return how many records that is. Returns 0 and touches nothing if t1 > t2.
   */
  long rangeSum(long t1, long t2, long[] out);

  /** Remove and return, in timestamp order (ties in insertion order), every record with ts <= w. */
  Entries splitPrefix(long w);

  /** Every record held, in timestamp order (ties in insertion order), without removing it: the checkpoint of Tier 1. */
  Entries entries();

  int size();

  /** Smallest timestamp held, or Long.MAX_VALUE if empty (lets a watermark that certifies nothing skip the split). */
  long minTs();

  /** A run of records extracted from Tier 1: n timestamps and n*M counts, row-major, in time order. */
  final class Entries {
    public final int n;
    public final int channels;
    public final long[] ts;
    public final long[] vals;

    public Entries(int n, int channels, long[] ts, long[] vals) {
      this.n = n;
      this.channels = channels;
      this.ts = ts;
      this.vals = vals;
    }
  }
}
