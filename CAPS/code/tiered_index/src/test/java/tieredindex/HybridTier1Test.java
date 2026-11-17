package tieredindex;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Random;
import org.junit.Test;
import tieredindex.AugmentedRBTreeTest.Mirror;

// Randomized differential tests of HybridTier1 (ordered run + red-black tree) against a brute-force mirror, with
// mixed in-order and out-of-order arrivals, equal timestamps, range sums, prefix splits, checkpoint entries.
public class HybridTier1Test {

  static long[] vec(Random rnd, int m) {
    long[] v = new long[m];
    for (int c = 0; c < m; c++)
      v[c] = rnd.nextInt(100) - 10; // negatives too: sums must still be exact
    return v;
  }

  /** One scenario: orderedPct percent of the arrivals are >= the current maximum, the others land below it. */
  static void scenario(int m, int orderedPct, int ops, long seed) {
    Random rnd = new Random(seed);
    HybridTier1 h = new HybridTier1(m);
    Mirror mir = new Mirror(m);
    long max = 0;
    for (int op = 0; op < ops; op++) {
      int kind = rnd.nextInt(100);
      if (kind < 62) {
        long ts;
        if (rnd.nextInt(100) < orderedPct)
          ts = max + (rnd.nextInt(3) == 0 ? 0 : rnd.nextInt(5)); // equal timestamps happen
        else
          ts = max - 1 - rnd.nextInt(rnd.nextInt(4) == 0 ? 40 : 3); // mostly just behind the maximum
        max = Math.max(max, ts);
        long[] v = vec(rnd, m);
        h.insert(ts, v);
        mir.insert(ts, v);
      } else if (kind < 84) {
        long t1 = max - rnd.nextInt(120);
        long t2 = rnd.nextInt(5) == 0 ? t1 - rnd.nextInt(3) : t1 + rnd.nextInt(150);
        if (!mir.recs.isEmpty() && rnd.nextBoolean()) { // bounds exactly on (or next to) held timestamps
          t1 = mir.recs.get(rnd.nextInt(mir.recs.size()))[0] - rnd.nextInt(2);
          t2 = mir.recs.get(rnd.nextInt(mir.recs.size()))[0] + rnd.nextInt(2);
        }
        AugmentedRBTreeTest.checkRange(h, mir, t1, t2, m);
      } else if (kind < 93) {
        long w = max - 60 + rnd.nextInt(70); // sometimes below everything, sometimes above the maximum
        if (!mir.recs.isEmpty() && rnd.nextInt(3) == 0)
          w = mir.recs.get(rnd.nextInt(mir.recs.size()))[0] - rnd.nextInt(2);
        List<long[]> expect = mir.split(w);
        mir.removedBefore += expect.size();
        AugmentedRBTreeTest.checkSplit(h.splitPrefix(w), expect, m);
      } else {
        // the checkpoint of Tier 1: everything held, in order, without removing it
        Tier1.Entries e = h.entries();
        List<long[]> all = mir.split(Long.MAX_VALUE);
        mir.removedBefore += 0;
        AugmentedRBTreeTest.checkSplit(e, all, m);
        for (long[] r : all) // put them back with their original sequence numbers
          mir.recs.add(r);
      }
      h.tree.checkInvariants();
      assertEquals(mir.recs.size(), h.size());
      long min = Long.MAX_VALUE;
      for (long[] r : mir.recs)
        min = Math.min(min, r[0]);
      assertEquals(min, h.minTs());
    }
    AugmentedRBTreeTest.checkRange(h, mir, Long.MIN_VALUE, Long.MAX_VALUE, m);
    List<long[]> rest = mir.split(Long.MAX_VALUE);
    AugmentedRBTreeTest.checkSplit(h.splitPrefix(Long.MAX_VALUE), rest, m);
    assertEquals(0, h.size());
  }

  @Test
  public void differentialMixedArrivals() {
    for (int m : new int[] {1, 2, 4, 16})
      for (int pct : new int[] {100, 95, 70, 30, 0})
        for (int seed = 0; seed < 8; seed++)
          scenario(m, pct, 1500, seed * 131L + m * 7 + pct);
  }

  @Test
  public void orderedArrivalsNeverTouchTheTree() {
    HybridTier1 h = new HybridTier1(2);
    for (int i = 0; i < 5000; i++)
      h.insert(i / 3, new long[] {1, i}); // non-decreasing, with ties
    assertEquals(5000, h.runInserts);
    assertEquals(0, h.treeInserts);
    assertEquals(0, h.tree.size());
    long[] out = new long[2];
    assertEquals(3L * 10, h.rangeSum(100, 109, out));
    assertEquals(30, out[0]);
    h.insert(10, new long[] {1, 1}); // below the maximum: the tree
    assertEquals(1, h.treeInserts);
    assertEquals(1, h.tree.size());
  }

  @Test
  public void aRecordJustBehindTheMaximumGoesToTheTree() {
    HybridTier1 h = new HybridTier1(1);
    h.insert(10, new long[] {1});
    h.insert(20, new long[] {2});
    h.insert(19, new long[] {4}); // out of order by one
    assertEquals(1, h.treeInserts);
    long[] out = new long[1];
    assertEquals(1, h.rangeSum(19, 19, out));
    assertEquals(4, out[0]);
    out[0] = 0;
    assertEquals(2, h.rangeSum(15, 25, out));
    assertEquals(6, out[0]);
    Tier1.Entries e = h.splitPrefix(19);
    assertArrayEquals(new long[] {10, 19}, e.ts);
    assertArrayEquals(new long[] {1, 4}, e.vals);
    assertEquals(1, h.size());
  }

  @Test
  public void tiesKeepArrivalOrderAcrossRunAndTree() {
    HybridTier1 h = new HybridTier1(1);
    h.insert(10, new long[] {1}); // run
    h.insert(20, new long[] {2}); // run
    h.insert(10, new long[] {3}); // tree, ties with the run record that arrived first
    h.insert(10, new long[] {4}); // tree
    h.insert(20, new long[] {5}); // run (ts >= max)
    Tier1.Entries e = h.splitPrefix(20);
    assertArrayEquals(new long[] {10, 10, 10, 20, 20}, e.ts);
    assertArrayEquals(new long[] {1, 3, 4, 2, 5}, e.vals);
  }

  @Test
  public void reusableAfterBeingEmptiedAndAfterManySplits() {
    HybridTier1 h = new HybridTier1(3);
    Mirror mir = new Mirror(3);
    Random rnd = new Random(9);
    long ts = 0;
    for (int round = 0; round < 200; round++) { // the head moves, the arrays compact and grow
      for (int i = 0; i < 40; i++) {
        ts += rnd.nextInt(3);
        long[] v = vec(rnd, 3);
        h.insert(ts, v);
        mir.insert(ts, v);
      }
      long w = ts - (round % 7 == 0 ? 0 : 25);
      List<long[]> expect = mir.split(w);
      mir.removedBefore += expect.size();
      AugmentedRBTreeTest.checkSplit(h.splitPrefix(w), expect, 3);
      AugmentedRBTreeTest.checkRange(h, mir, w - 30, w + 30, 3);
    }
    assertTrue(h.size() >= 0);
  }
}
