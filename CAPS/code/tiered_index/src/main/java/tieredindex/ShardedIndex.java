package tieredindex;

import java.util.function.IntFunction;

/**
 * K two-tier indexes, one per sink subtask. Each output is added to exactly one shard.
 * Global frontier = min over the shard watermarks. A query is the channel-wise sum of the shard
 * answers (exact because the shards partition the outputs); it is final iff frontier &gt;= t2
 * and undefined iff every shard is undefined.
 * At parallelism above 1: the merge of the per-subtask shards after the job (Paper Section 5.4).
 */
public class ShardedIndex {

  final int channels;
  public final TwoTierIndex[] shards;

  /** tier1Factory builds the Tier 1 of a shard from the channel count. */
  public ShardedIndex(int k, int channels, IntFunction<Tier1> tier1Factory) {
    this.channels = channels;
    this.shards = new TwoTierIndex[k];
    for (int i = 0; i < k; i++)
      shards[i] = new TwoTierIndex(channels, tier1Factory.apply(channels));
  }

  /** Assembles already-filled shards (the post-job merge in the experiment harness). */
  public ShardedIndex(TwoTierIndex[] shards) {
    this.channels = shards[0].channels();
    this.shards = shards;
  }

  /** Number of shards K (one per sink subtask). */
  public int numShards() {
    return shards.length;
  }

  /** Index one output in the given shard (Tier 1 of that shard). False if late for that shard. */
  public boolean add(int shard, long ts, long[] v) {
    return shards[shard].add(ts, v);
  }

  /** The watermark of one sink subtask: migrates that shard's certified Tier-1 prefix to its Tier 2. */
  public void advanceWatermark(int shard, long w) {
    shards[shard].advanceWatermark(w);
  }

  /** Minimum of the shard watermarks: nothing at or below it can still arrive anywhere. */
  public long frontier() {
    long f = Long.MAX_VALUE;
    for (TwoTierIndex s : shards)
      f = Math.min(f, s.watermark());
    return f;
  }

  /**
   * HMP over [t1, t2]: the channel-wise sum of the shard answers. Cost K times that of
   * {@link TwoTierIndex#query}; final iff t2 &lt;= frontier(), undefined iff no shard has an output in range.
   */
  public Answer query(long t1, long t2) {
    if (shards.length == 1)
      return shards[0].query(t1, t2); // the frontier is the shard's own watermark: nothing to merge
    long[] sum = new long[channels];
    boolean any = false;
    for (TwoTierIndex s : shards) {
      Answer a = s.query(t1, t2);
      for (int c = 0; c < channels; c++)
        sum[c] += a.counts[c];
      any |= a.defined;
    }
    return new Answer(sum, t2 <= frontier(), any);
  }

  /** Versioning of intervals a and b: two queries, absolute and relative differences from the answers. */
  public Answer.Version versioning(long a1, long a2, long b1, long b2) {
    return new Answer.Version(query(a1, a2), query(b1, b2));
  }

  /** Total number of outputs that arrived at or below their shard's watermark. */
  public long lateCount() {
    long n = 0;
    for (TwoTierIndex s : shards)
      n += s.lateCount;
    return n;
  }
}
