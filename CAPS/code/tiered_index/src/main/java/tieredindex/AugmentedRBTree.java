package tieredindex;

import java.util.Arrays;

/**
 * Tier 1 as a red-black tree keyed by (derivedTs, seq) whose nodes carry the per-channel sums
 * and the size of their subtree. insert and rangeSum cost O(M log n); splitPrefix(W) is a
 * join-based split (Blelloch, Ferizovic, Sun, "Just Join for Parallel Ordered Sets", SPAA 2016)
 * that costs O(M log n + k M) for k extracted records.
 * Representation: classic red-black (root black, no red-red, equal black height on all paths).
 * Every node stores its black height bh (black nodes on the path down to a leaf, itself
 * included, nil = 0) because join needs it in O(1); bh is derived from the left child only.
 * Tier 1 of the two-tier index (Paper Section 5.4): the structure that accepts out-of-order outputs.
 */
public class AugmentedRBTree implements Tier1 {

  static final class Node {
    long ts;
    long seq; // insertion counter, makes equal timestamps distinct keys
    final long[] counts; // this record's vector
    final long[] sum; // channel-wise sum over the whole subtree
    int size; // records in the subtree
    int bh;
    boolean red;
    Node left, right;

    Node(long ts, long seq, long[] counts, long[] sum) {
      this.ts = ts;
      this.seq = seq;
      this.counts = counts;
      this.sum = sum;
    }
  }

  final int channels;
  Node root;
  long nextSeq;

  // Result slots of the recursive split (avoids allocating a pair per level).
  Node splitLeft, splitRight;
  int fillPos;

  /** Number of node recomputations so far (tests use it to check the O(log n) bound of a split). */
  public long updateCalls;

  private final Entries empty;

  public AugmentedRBTree(int channels) {
    this.channels = channels;
    this.empty = new Entries(0, channels, new long[0], new long[0]);
  }

  @Override
  public int size() {
    return root == null ? 0 : root.size;
  }

  // insert
  /** O(M log n): one descent adding v to every ancestor's sum, plus O(1) amortised rebalancing. */
  @Override
  public void insert(long ts, long[] v) {
    long[] counts = Arrays.copyOf(v, channels);
    Node n = new Node(ts, nextSeq++, counts, counts.clone());
    n.size = 1;
    n.red = true;
    root = insert(root, n);
    if (root.red) {
      root.red = false;
      root.bh++;
    }
  }

  private Node insert(Node t, Node n) {
    if (t == null)
      return n;
    long[] s = t.sum;
    long[] v = n.counts;
    for (int c = 0; c < channels; c++)
      s[c] += v[c];
    t.size++;
    if (less(n, t))
      t.left = insert(t.left, n);
    else
      t.right = insert(t.right, n);
    return balance(t);
  }

  private static boolean less(Node a, Node b) {
    return a.ts < b.ts || (a.ts == b.ts && a.seq < b.seq);
  }

  /**
   * Okasaki-style repair for a black node with a red child that has a red child: the four
   * shapes all become a red root with two black children. Sums are recomputed on the three
   * restructured nodes only (the subtrees t1..t4 are untouched).
   */
  private Node balance(Node t) {
    if (t.red)
      return t;
    Node l = t.left;
    Node r = t.right;
    Node x, y, z;
    Node t1, t2, t3, t4;
    if (l != null && l.red && l.left != null && l.left.red) {
      x = l.left; y = l; z = t;
      t1 = x.left; t2 = x.right; t3 = y.right; t4 = z.right;
    } else if (l != null && l.red && l.right != null && l.right.red) {
      x = l; y = l.right; z = t;
      t1 = x.left; t2 = y.left; t3 = y.right; t4 = z.right;
    } else if (r != null && r.red && r.left != null && r.left.red) {
      x = t; y = r.left; z = r;
      t1 = x.left; t2 = y.left; t3 = y.right; t4 = z.right;
    } else if (r != null && r.red && r.right != null && r.right.red) {
      x = t; y = r; z = r.right;
      t1 = x.left; t2 = y.left; t3 = z.left; t4 = z.right;
    } else
      return t;
    x.left = t1; x.right = t2;
    z.left = t3; z.right = t4;
    y.left = x; y.right = z;
    x.red = false; z.red = false; y.red = true;
    update(x);
    update(z);
    update(y);
    return y;
  }

  // range sum
  /** O(M log n): two root-to-leaf descents, each adding O(log n) whole-subtree aggregates. */
  @Override
  public long rangeSum(long t1, long t2, long[] out) {
    if (t1 > t2)
      return 0;
    long hi = prefix(t2, 1, out);
    long lo = t1 == Long.MIN_VALUE ? 0 : prefix(t1 - 1, -1, out);
    return hi - lo;
  }

  /** out += sign * (sum of records with ts <= x); returns how many records that is. */
  private long prefix(long x, int sign, long[] out) {
    long count = 0;
    Node t = root;
    while (t != null) {
      if (t.ts <= x) {
        // t and its whole left subtree qualify: take their aggregate, continue right.
        Node l = t.left;
        if (l != null) {
          addScaled(out, l.sum, sign);
          count += l.size;
        }
        addScaled(out, t.counts, sign);
        count++;
        t = t.right;
      } else
        t = t.left;
    }
    return count;
  }

  private void addScaled(long[] out, long[] v, int sign) {
    if (sign > 0)
      for (int c = 0; c < channels; c++)
        out[c] += v[c];
    else
      for (int c = 0; c < channels; c++)
        out[c] -= v[c];
  }

  // split
  /**
   * Remove and return all records with ts <= w, in timestamp order. The tree is cut along one
   * root-to-leaf path; the O(log n) hanging subtrees are re-joined into a left part (<= w) and
   * a right part (> w). Join cost is proportional to the black-height difference, which
   * telescopes to O(log n) over the whole path. Extraction is an in-order walk of the left part.
   */
  @Override
  public Entries splitPrefix(long w) {
    if (root == null || minTs() > w)
      return empty;
    splitAt(root, w);
    Node left = splitLeft;
    root = splitRight;
    splitLeft = null;
    splitRight = null;
    if (root != null && root.red)
      setBlack(root);
    int k = left == null ? 0 : left.size;
    Entries e = new Entries(k, channels, new long[k], new long[k * channels]);
    fillPos = 0;
    fill(left, e);
    return e;
  }

  /** O(M n): an in-order walk, the tree is left as it is. */
  @Override
  public Entries entries() {
    int k = size();
    Entries e = new Entries(k, channels, new long[k], new long[k * channels]);
    fillPos = 0;
    fill(root, e);
    return e;
  }

  /** O(log n): the leftmost node. */
  @Override
  public long minTs() {
    Node t = root;
    if (t == null)
      return Long.MAX_VALUE;
    while (t.left != null)
      t = t.left;
    return t.ts;
  }

  /** Split t into splitLeft (ts <= w) and splitRight (ts > w); t's nodes are reused as join pivots. */
  private void splitAt(Node t, long w) {
    if (t == null) {
      splitLeft = null;
      splitRight = null;
      return;
    }
    if (t.ts <= w) {
      Node tl = t.left;
      splitAt(t.right, w);
      splitLeft = join(tl, t, splitLeft);
    } else {
      Node tr = t.right;
      splitAt(t.left, w);
      splitRight = join(splitRight, t, tr);
    }
  }

  private void fill(Node t, Entries e) {
    if (t == null)
      return;
    fill(t.left, e);
    e.ts[fillPos] = t.ts;
    System.arraycopy(t.counts, 0, e.vals, fillPos * channels, channels);
    fillPos++;
    fill(t.right, e);
  }

  // join
  /**
   * Tree of all of l, then k, then all of r (every key in l < k < every key in r), with the
   * aggregates of every touched node recomputed. k is a detached node and is reused as the
   * pivot. Cost O(M (|bh(l) - bh(r)| + 1)).
   */
  Node join(Node l, Node k, Node r) {
    // Black roots on both sides, so the new red pivot / spine repairs never meet a red root.
    if (l != null && l.red)
      setBlack(l);
    if (r != null && r.red)
      setBlack(r);
    int bl = bh(l);
    int br = bh(r);
    Node t;
    if (bl > br)
      t = joinRight(l, k, r, br);
    else if (br > bl)
      t = joinLeft(r, k, l, bl);
    else {
      k.left = l;
      k.right = r;
      k.red = true;
      update(k);
      t = k;
    }
    if (t.red)
      setBlack(t);
    return t;
  }

  /**
   * bh(l) > bhR. Walk down the right spine of l to the first black node c with bh(c) = bhR,
   * and replace c by the red node (c, k, r); then repair red-red on the way back up.
   */
  private Node joinRight(Node l, Node k, Node r, int bhR) {
    if (l == null || (!l.red && l.bh == bhR)) {
      k.left = l;
      k.right = r;
      k.red = true;
      update(k);
      return k;
    }
    l.right = joinRight(l.right, k, r, bhR);
    update(l);
    Node p = l.right;
    // Black l, red child p, red grandchild: rotate left, blacken the grandchild. p stays red;
    // a red-red with p's parent is repaired by the same rule one black level up.
    if (!l.red && p.red && p.right != null && p.right.red) {
      l.right = p.left;
      p.left = l;
      setBlack(p.right);
      update(l);
      update(p);
      return p;
    }
    return l;
  }

  /** Mirror image of joinRight: spine = r (taller), small = l (black height bhL). */
  private Node joinLeft(Node r, Node k, Node l, int bhL) {
    if (r == null || (!r.red && r.bh == bhL)) {
      k.left = l;
      k.right = r;
      k.red = true;
      update(k);
      return k;
    }
    r.left = joinLeft(r.left, k, l, bhL);
    update(r);
    Node p = r.left;
    if (!r.red && p.red && p.left != null && p.left.red) {
      r.left = p.right;
      p.right = r;
      setBlack(p.left);
      update(r);
      update(p);
      return p;
    }
    return r;
  }

  // helpers
  private static int bh(Node n) {
    return n == null ? 0 : n.bh;
  }

  /** Red node with black children becomes black: its sums stay valid, its black height grows by one. */
  private static void setBlack(Node n) {
    n.red = false;
    n.bh++;
  }

  /** Recompute size, black height and channel sums of n from its children: O(M). */
  private void update(Node n) {
    updateCalls++;
    Node l = n.left;
    Node r = n.right;
    long[] s = n.sum;
    System.arraycopy(n.counts, 0, s, 0, channels);
    int size = 1;
    if (l != null) {
      long[] ls = l.sum;
      for (int c = 0; c < channels; c++)
        s[c] += ls[c];
      size += l.size;
    }
    if (r != null) {
      long[] rs = r.sum;
      for (int c = 0; c < channels; c++)
        s[c] += rs[c];
      size += r.size;
    }
    n.size = size;
    n.bh = (l == null ? 0 : l.bh) + (n.red ? 0 : 1);
  }

  // invariants (tests)
  /** Verify BST order, red-black rules, black heights, sizes and subtree sums; throw if broken. */
  public void checkInvariants() {
    if (root == null)
      return;
    if (root.red)
      throw new IllegalStateException("root is red");
    check(root, null, null);
  }

  /** Returns the black height of t's subtree after checking everything below it. */
  private int check(Node t, Node lo, Node hi) {
    if (t == null)
      return 0;
    if (lo != null && !less(lo, t))
      throw new IllegalStateException("BST order violated at ts=" + t.ts);
    if (hi != null && !less(t, hi))
      throw new IllegalStateException("BST order violated at ts=" + t.ts);
    if (t.red && ((t.left != null && t.left.red) || (t.right != null && t.right.red)))
      throw new IllegalStateException("red node with red child at ts=" + t.ts);
    int bl = check(t.left, lo, t);
    int br = check(t.right, t, hi);
    if (bl != br)
      throw new IllegalStateException("black heights differ " + bl + " vs " + br + " at ts=" + t.ts);
    int bhere = bl + (t.red ? 0 : 1);
    if (t.bh != bhere)
      throw new IllegalStateException("stored bh " + t.bh + " != " + bhere + " at ts=" + t.ts);
    int size = 1 + (t.left == null ? 0 : t.left.size) + (t.right == null ? 0 : t.right.size);
    if (t.size != size)
      throw new IllegalStateException("stored size " + t.size + " != " + size + " at ts=" + t.ts);
    for (int c = 0; c < channels; c++) {
      long s = t.counts[c];
      if (t.left != null)
        s += t.left.sum[c];
      if (t.right != null)
        s += t.right.sum[c];
      if (t.sum[c] != s)
        throw new IllegalStateException("stored sum wrong in channel " + c + " at ts=" + t.ts);
    }
    return bhere;
  }
}
