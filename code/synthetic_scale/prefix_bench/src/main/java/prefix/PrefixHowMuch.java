package prefix;

import java.io.Serializable;

/**
 * Exact how-much annotation stored as a prefix tree (trie) over the branch choices of the path.
 * The M = 2^d paths are the root-to-leaf paths of the binary multiplex tree: level l splits on bit l of
 * the path id (the same order in which the dataflow tree splits). Paths with a common prefix share the
 * internal nodes of the prefix. Every node keeps one counter: a leaf the count of its full path, an
 * internal node the sum of its subtree (so the counter of any prefix is a stored value, the root holds
 * the total).
 *
 * Encoding, level order, primitive arrays only (no object per node):
 * cnt[i]   counter of node i, all nodes (levels 0..d); level 0 is the root, nodes of one level are
 * sorted by the bits of their prefix (bit 0 most significant)
 * mask[i]  for the internal nodes (levels 0..d-1, they come first in level order, so the same index i):
 * bit 0 set = the node has a child on branch 0, bit 1 set = on branch 1
 * The children of a level are stored in the order of their parents, so the position of a child is
 * given by counting the set mask bits before it in its parent's level (rank).
 * An empty annotation has no nodes. The size is O(nodes) = O(support x (d - log support) + support).
 */
public class PrefixHowMuch implements Serializable {

  private static final long serialVersionUID = 1L;

  public int d;        // depth of the tree, M = 2^d
  public byte[] mask;  // children presence of the internal nodes, level order
  public long[] cnt;   // counter of every node, level order

  public PrefixHowMuch(int d) {
    this.d = d;
    mask = new byte[0];
    cnt = new long[0];
  }

  private PrefixHowMuch(int d, byte[] mask, long[] cnt) {
    this.d = d;
    this.mask = mask;
    this.cnt = cnt;
  }

  /** Source instrumentation: a fresh tuple counting 1 on the path `leaf`: a chain of d+1 nodes. */
  public static PrefixHowMuch one(int d, int leaf) {
    byte[] m = new byte[d];
    long[] c = new long[d + 1];
    for (int l = 0; l < d; l++) {
      m[l] = (byte) (1 << ((leaf >> l) & 1));
      c[l] = 1;
    }
    c[d] = 1;
    return new PrefixHowMuch(d, m, c);
  }

  public PrefixHowMuch copy() {
    return new PrefixHowMuch(d, mask.clone(), cnt.clone());
  }

  // Scratch of the merge (the merged node list), one per thread: no allocation in the hot path.
  private static final class Scratch {
    int[] a = new int[64];
    int[] b = new int[64];
    byte[] m = new byte[64];
    int[] p0 = new int[64];
    int[] p1 = new int[64];

    void ensure(int n) {
      if (a.length >= n) return;
      int c = Math.max(n, 2 * a.length);
      a = new int[c];
      b = new int[c];
      m = new byte[c];
      p0 = new int[c];
      p1 = new int[c];
    }
  }

  private static final ThreadLocal<Scratch> SCRATCH = new ThreadLocal<Scratch>() {
    @Override
    protected Scratch initialValue() {
      return new Scratch();
    }
  };

  /**
   * Aggregate / join instrumentation: merge the other trie into this one, level by level. The two
   * trees are walked together; node k of the merged tree comes from node a[k] of this (or -1) and node
   * b[k] of the other (or -1). If every node of the other already exists here, the counters are added
   * in place (no allocation), otherwise the merged tree is built in new arrays.
   */
  public void mergeWith(PrefixHowMuch o) {
    if (o.cnt.length == 0) return;
    if (cnt.length == 0) {
      mask = o.mask.clone();
      cnt = o.cnt.clone();
      return;
    }
    Scratch s = SCRATCH.get();
    s.ensure(cnt.length + o.cnt.length);
    final int[] sa = s.a, sb = s.b;
    final byte[] sm = s.m;
    sa[0] = 0;
    sb[0] = 0;
    int n = 1, lo = 0, hi = 1, nextA = 1, nextB = 1;
    boolean grew = false;
    for (int l = 0; l < d; l++) {
      for (int k = lo; k < hi; k++) {
        int ma = sa[k] >= 0 ? mask[sa[k]] : 0;
        int mb = sb[k] >= 0 ? o.mask[sb[k]] : 0;
        if ((mb & ~ma) != 0) grew = true;
        int m = ma | mb;
        sm[k] = (byte) m;
        for (int bit = 0; bit < 2; bit++)
          if (((m >> bit) & 1) != 0) {
            sa[n] = ((ma >> bit) & 1) != 0 ? nextA++ : -1;
            sb[n] = ((mb >> bit) & 1) != 0 ? nextB++ : -1;
            n++;
          }
      }
      lo = hi;
      hi = n;
    }
    if (!grew) {
      for (int k = 0; k < n; k++)
        if (sb[k] >= 0) cnt[sa[k]] += o.cnt[sb[k]];
      return;
    }
    byte[] nm = new byte[lo];
    System.arraycopy(sm, 0, nm, 0, lo);
    long[] nc = new long[n];
    for (int k = 0; k < n; k++)
      nc[k] = (sa[k] >= 0 ? cnt[sa[k]] : 0) + (sb[k] >= 0 ? o.cnt[sb[k]] : 0);
    mask = nm;
    cnt = nc;
  }

  public static PrefixHowMuch merge(PrefixHowMuch a, PrefixHowMuch b) {
    PrefixHowMuch r = a.copy();
    r.mergeWith(b);
    return r;
  }

  /** Total: the counter of the root, O(1). */
  public long total() {
    return cnt.length == 0 ? 0 : cnt[0];
  }

  /** Number of paths (leaves) with a non-zero count. */
  public int support() {
    return cnt.length - mask.length;
  }

  /** Number of stored nodes. */
  public int nodes() {
    return cnt.length;
  }

  /** The counter of node i of the level-order arrays: O(1). */
  public long node(int i) {
    return cnt[i];
  }

  /**
   * The counter of a prefix: the first `level` branch choices (bit l of `prefix` is the choice at level l).
   * The counter is a stored value; finding its position is a walk of `level` steps, each a rank over the
   * masks of one level, O(nodes of the first `level` levels). level = d is the count of one path.
   */
  public long prefixCount(int level, int prefix) {
    if (cnt.length == 0) return 0;
    int p = 0, ls = 0, w = 1;
    for (int l = 0; l < level; l++) {
      int bit = (prefix >> l) & 1;
      int m = mask[p];
      if (((m >> bit) & 1) == 0) return 0;
      int before = 0;
      for (int i = ls; i < p; i++) before += Integer.bitCount(mask[i]);
      int rank = before + (bit == 1 ? (m & 1) : 0);
      int after = before + Integer.bitCount(m);
      for (int i = p + 1; i < ls + w; i++) after += Integer.bitCount(mask[i]);
      ls += w;
      w = after;
      p = ls + rank;
    }
    return cnt[p];
  }

  /** The counter of one path (0 if absent). */
  public long count(int leaf) {
    return prefixCount(d, leaf);
  }

  /** The dense vector HowMuch.counts would hold: one sweep over the nodes, tracking each node's path id. */
  public long[] toDense(int channels) {
    long[] r = new long[channels];
    if (cnt.length == 0) return r;
    Scratch s = SCRATCH.get();
    s.ensure(Math.max(channels, 2));
    int[] cur = s.p0, nxt = s.p1;
    cur[0] = 0;
    int ls = 0, w = 1;
    for (int l = 0; l < d; l++) {
      int c = 0;
      for (int j = 0; j < w; j++) {
        int m = mask[ls + j];
        if ((m & 1) != 0) nxt[c++] = cur[j];
        if ((m & 2) != 0) nxt[c++] = cur[j] | (1 << l);
      }
      ls += w;
      w = c;
      int[] t = cur;
      cur = nxt;
      nxt = t;
    }
    for (int j = 0; j < w; j++) r[cur[j]] = cnt[ls + j];
    return r;
  }

  /** Invariant: every internal counter equals the sum of its children's counters. */
  public boolean consistent() {
    if (cnt.length == 0) return true;
    int ls = 0, w = 1;
    for (int l = 0; l < d; l++) {
      int child = ls + w;
      int c = 0;
      for (int j = 0; j < w; j++) {
        long sum = 0;
        for (int k = Integer.bitCount(mask[ls + j]); k > 0; k--) sum += cnt[child + c++];
        if (sum != cnt[ls + j]) return false;
      }
      ls += w;
      w = c;
    }
    return ls + w == cnt.length;
  }

  /** Payload bytes: 8 B per node counter + 1 B per internal node mask. */
  public long payloadBytes() {
    return 8L * cnt.length + mask.length;
  }

  /** "c:n;c:n" over the non-zero paths, ascending, the row format of the file sinks. */
  @Override
  public String toString() {
    long[] dense = toDense(1 << d);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < dense.length; i++)
      if (dense[i] != 0) {
        if (sb.length() > 0) sb.append(';');
        sb.append(i).append(':').append(dense[i]);
      }
    return sb.toString();
  }
}
