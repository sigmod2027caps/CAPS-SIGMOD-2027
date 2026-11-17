package tieredindex;

import java.util.Arrays;

/**
 * Tier 1 with an ordered-append fast path (the default Tier 1 of every index sink).
 *
 * Most streams reach a sink (almost) in timestamp order, and the paper's monotonicity lemma makes the input of a
 * per-subtask shard ordered by construction. A record whose timestamp is &gt;= the largest timestamp that Tier 1 holds
 * is therefore appended to an ORDERED RUN: a sorted array of timestamps with per-channel prefix sums. Only a record
 * that arrives out of order (ts &lt; max) goes into the AugmentedRBTree, which is kept for disorder: it bounds the cost
 * of ANY arrival order by O(M log n) per insert, as the paper promises.
 *
 * insert, in order      O(M) amortized (append a row of prefix sums)
 * insert, out of order  O(M log n) (red-black tree)
 * rangeSum              O(log n + M) over the run, plus O(M log n) over the tree (O(1) when the tree is empty)
 * splitPrefix           O(log n + k M) for the k records extracted from the run, plus the tree's split; the two
 * sorted sequences are merged in timestamp order, ties in arrival order
 *
 * Ties: a run record and a tree record can share a timestamp only if the run record arrived first (a tree record t
 * stays in the tree only while a record with ts &gt; t exists, and every later run record has ts &gt;= that maximum), so a
 * merge that prefers the run on ties yields arrival order.
 * The common case of ordered arrivals does not pay for the tree.
 */
public class HybridTier1 implements Tier1 {

  final int channels;
  final AugmentedRBTree tree;

  // The ordered run: records [head, n) of ts[] and of the prefix sums cum[] (row i = the sum of the records [0, i), so
  // the sum of the records [a, b) is row b minus row a; rows are absolute, the head moves forward without rebasing).
  long[] ts = new long[16];
  long[] cum; // (n + 1) rows of `channels` longs
  int head;
  int n;
  long maxTs = Long.MIN_VALUE; // largest timestamp held (run or tree) since the last time Tier 1 was empty

  public long runInserts; // statistics: records appended to the run / inserted into the tree
  public long treeInserts;

  public HybridTier1(int channels) {
    this.channels = channels;
    this.tree = new AugmentedRBTree(channels);
    this.cum = new long[17 * channels];
  }

  @Override
  public int size() {
    return (n - head) + tree.size();
  }

  @Override
  public long minTs() {
    long a = head < n ? ts[head] : Long.MAX_VALUE;
    return Math.min(a, tree.minTs());
  }

  @Override
  public void insert(long t, long[] v) {
    if (t < maxTs) {
      treeInserts++;
      tree.insert(t, v);
      return;
    }
    runInserts++;
    maxTs = t;
    if (n == ts.length) {
      if (head > n / 2)
        compact();
      else {
        ts = Arrays.copyOf(ts, n * 2);
        cum = Arrays.copyOf(cum, (n * 2 + 1) * channels);
      }
    }
    ts[n] = t;
    int from = n * channels;
    int to = from + channels;
    for (int c = 0; c < channels; c++)
      cum[to + c] = cum[from + c] + v[c];
    n++;
  }

  /** Drops the consumed head of the run: moves the live part to the front of the arrays. */
  private void compact() {
    int live = n - head;
    System.arraycopy(ts, head, ts, 0, live);
    System.arraycopy(cum, head * channels, cum, 0, (live + 1) * channels);
    head = 0;
    n = live;
  }

  @Override
  public long rangeSum(long t1, long t2, long[] out) {
    if (t1 > t2)
      return 0;
    long count = tree.size() == 0 ? 0 : tree.rangeSum(t1, t2, out);
    if (head == n)
      return count;
    int lo = t1 == Long.MIN_VALUE ? head : upperBound(t1 - 1); // first ts >= t1
    int hi = upperBound(t2);
    if (hi <= lo)
      return count;
    int a = lo * channels;
    int b = hi * channels;
    for (int c = 0; c < channels; c++)
      out[c] += cum[b + c] - cum[a + c];
    return count + (hi - lo);
  }

  @Override
  public Entries splitPrefix(long w) {
    int k = upperBound(w); // run records [head, k) are extracted
    Entries te = tree.size() == 0 || tree.minTs() > w ? null : tree.splitPrefix(w);
    int r = k - head;
    Entries e;
    if (te == null)
      e = runEntries(head, k);
    else if (r == 0)
      e = te;
    else
      e = merge(runEntries(head, k), te);
    head = k;
    if (head == n) { // empty run: restart the arrays (row 0 is the zero row of the rebased sums)
      head = 0;
      n = 0;
      Arrays.fill(cum, 0, channels, 0L);
    }
    if (size() == 0)
      maxTs = Long.MIN_VALUE;
    return e;
  }

  @Override
  public Entries entries() {
    Entries r = runEntries(head, n);
    Entries te = tree.entries();
    return te.n == 0 ? r : r.n == 0 ? te : merge(r, te);
  }

  /** The run records [a, b) as an Entries: the per-record counts are the differences of the prefix sums. */
  private Entries runEntries(int a, int b) {
    int k = b - a;
    long[] vals = new long[k * channels];
    for (int i = 0; i < k; i++) {
      int hi = (a + i + 1) * channels;
      int lo = (a + i) * channels;
      for (int c = 0; c < channels; c++)
        vals[i * channels + c] = cum[hi + c] - cum[lo + c];
    }
    return new Entries(k, channels, Arrays.copyOfRange(ts, a, b), vals);
  }

  /** Merge of two time-ordered runs; on equal timestamps the first (the ordered run, which arrived first) wins. */
  private Entries merge(Entries x, Entries y) {
    int total = x.n + y.n;
    long[] mts = new long[total];
    long[] mv = new long[total * channels];
    int i = 0;
    int j = 0;
    for (int o = 0; o < total; o++) {
      boolean fromX = j == y.n || (i < x.n && x.ts[i] <= y.ts[j]);
      if (fromX) {
        mts[o] = x.ts[i];
        System.arraycopy(x.vals, i * channels, mv, o * channels, channels);
        i++;
      } else {
        mts[o] = y.ts[j];
        System.arraycopy(y.vals, j * channels, mv, o * channels, channels);
        j++;
      }
    }
    return new Entries(total, channels, mts, mv);
  }

  /** First run position in [head, n) with ts > t. */
  private int upperBound(long t) {
    int lo = head;
    int hi = n;
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (ts[mid] <= t)
        lo = mid + 1;
      else
        hi = mid;
    }
    return lo;
  }
}
