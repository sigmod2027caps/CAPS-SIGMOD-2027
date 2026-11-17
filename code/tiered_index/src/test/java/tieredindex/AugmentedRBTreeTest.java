package tieredindex;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Test;

public class AugmentedRBTreeTest {

  /** Brute-force mirror: records in insertion order; sorted by (ts, insertion) on demand. */
  static final class Mirror {
    final int m;
    final List<long[]> recs = new ArrayList<>(); // [ts, seq, v...]

    Mirror(int m) {
      this.m = m;
    }

    void insert(long ts, long[] v) {
      long[] r = new long[m + 2];
      r[0] = ts;
      r[1] = recs.size() + removedBefore;
      System.arraycopy(v, 0, r, 2, m);
      recs.add(r);
    }

    long removedBefore;

    long range(long t1, long t2, long[] out) {
      long n = 0;
      for (long[] r : recs)
        if (r[0] >= t1 && r[0] <= t2) {
          n++;
          for (int c = 0; c < m; c++)
            out[c] += r[2 + c];
        }
      return n;
    }

    /** Remove and return records with ts <= w sorted by (ts, seq). */
    List<long[]> split(long w) {
      List<long[]> gone = new ArrayList<>();
      List<long[]> keep = new ArrayList<>();
      for (long[] r : recs)
        (r[0] <= w ? gone : keep).add(r);
      gone.sort((a, b) -> a[0] != b[0] ? Long.compare(a[0], b[0]) : Long.compare(a[1], b[1]));
      recs.clear();
      recs.addAll(keep);
      return gone;
    }
  }

  static long[] vec(Random rnd, int m) {
    long[] v = new long[m];
    for (int c = 0; c < m; c++)
      v[c] = rnd.nextInt(100) - 10; // negatives too: sums must still be exact
    return v;
  }

  static void checkSplit(Tier1.Entries e, List<long[]> expect, int m) {
    assertEquals(expect.size(), e.n);
    for (int i = 0; i < e.n; i++) {
      assertEquals(expect.get(i)[0], e.ts[i]);
      for (int c = 0; c < m; c++)
        assertEquals(expect.get(i)[2 + c], e.vals[i * m + c]);
    }
  }

  static void checkRange(Tier1 t, Mirror mir, long t1, long t2, int m) {
    long[] a = new long[m];
    long[] b = new long[m];
    long na = t.rangeSum(t1, t2, a);
    long nb = mir.range(t1, t2, b);
    assertEquals(nb, na);
    assertArrayEquals(b, a);
  }

  @Test
  public void randomOpsKeepInvariantsAndMatchBruteForce() {
    for (int m : new int[] {1, 2, 4, 16})
      for (int seed = 0; seed < 30; seed++) {
        Random rnd = new Random(seed * 31 + m);
        AugmentedRBTree t = new AugmentedRBTree(m);
        Mirror mir = new Mirror(m);
        int range = new int[] {5, 50, 1000}[seed % 3]; // few distinct ts => many duplicates
        for (int op = 0; op < 400; op++) {
          int kind = rnd.nextInt(10);
          if (kind < 6) {
            long ts = rnd.nextInt(range) - range / 3;
            long[] v = vec(rnd, m);
            t.insert(ts, v);
            mir.insert(ts, v);
          } else if (kind < 9) {
            long t1 = rnd.nextInt(range + 10) - range / 3 - 5;
            long t2 = rnd.nextInt(4) == 0 ? t1 - rnd.nextInt(3) : t1 + rnd.nextInt(range);
            checkRange(t, mir, t1, t2, m);
          } else {
            long w = rnd.nextInt(range + 10) - range / 3 - 5;
            List<long[]> expect = mir.split(w);
            mir.removedBefore += expect.size();
            checkSplit(t.splitPrefix(w), expect, m);
          }
          t.checkInvariants();
          assertEquals(mir.recs.size(), t.size());
        }
        checkRange(t, mir, Long.MIN_VALUE, Long.MAX_VALUE, m);
      }
  }

  @Test
  public void sequentialAndConstantTimestamps() {
    int m = 2;
    for (int mode = 0; mode < 3; mode++) {
      AugmentedRBTree t = new AugmentedRBTree(m);
      Mirror mir = new Mirror(m);
      Random rnd = new Random(mode);
      for (int i = 0; i < 2000; i++) {
        long ts = mode == 0 ? i : mode == 1 ? 2000 - i : 7; // ascending, descending, all equal
        long[] v = vec(rnd, m);
        t.insert(ts, v);
        mir.insert(ts, v);
        if (i % 100 == 0)
          t.checkInvariants();
      }
      t.checkInvariants();
      checkRange(t, mir, 100, 900, m);
      checkSplit(t.splitPrefix(mode == 2 ? 6 : 700), mir.split(mode == 2 ? 6 : 700), m); // none / part
      t.checkInvariants();
      long w = mode == 2 ? 7 : 5000; // everything
      checkSplit(t.splitPrefix(w), mir.split(w), m);
      t.checkInvariants();
      assertEquals(0, t.size());
      t.insert(3, new long[m]); // reusable after being emptied
      t.checkInvariants();
      assertEquals(1, t.size());
    }
  }

  @Test
  public void largeTreeSplitsKeepInvariants() {
    int m = 4;
    Random rnd = new Random(5);
    AugmentedRBTree t = new AugmentedRBTree(m);
    Mirror mir = new Mirror(m);
    for (int i = 0; i < 50_000; i++) {
      long ts = rnd.nextInt(1_000_000);
      long[] v = vec(rnd, m);
      t.insert(ts, v);
      mir.insert(ts, v);
    }
    t.checkInvariants();
    long w = 0;
    for (int round = 0; round < 20; round++) {
      w += rnd.nextInt(100_000);
      List<long[]> expect = mir.split(w);
      mir.removedBefore += expect.size();
      checkSplit(t.splitPrefix(w), expect, m);
      t.checkInvariants();
      for (int q = 0; q < 20; q++) {
        long t1 = rnd.nextInt(1_100_000);
        checkRange(t, mir, t1, t1 + rnd.nextInt(300_000), m);
      }
    }
  }

  /** A split must touch O(log n) nodes, not O(n): count node recomputations for tiny k. */
  @Test
  public void splitTouchesLogarithmicallyManyNodes() {
    int m = 2;
    int n = 1 << 17;
    AugmentedRBTree t = new AugmentedRBTree(m);
    for (int i = 0; i < n; i++)
      t.insert(i, new long[m]);
    long before = t.updateCalls;
    Tier1.Entries e = t.splitPrefix(9);
    long used = t.updateCalls - before;
    assertEquals(10, e.n);
    assertTrue("update calls " + used, used <= 8 * 17);
    t.checkInvariants();
    // split in the middle: still logarithmic work apart from the k extracted records (read-only walk)
    before = t.updateCalls;
    e = t.splitPrefix(n / 2);
    used = t.updateCalls - before;
    assertEquals(n / 2 + 1 - 10, e.n);
    assertTrue("update calls " + used, used <= 8 * 17);
    t.checkInvariants();
  }
}
