package tieredindex;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.function.IntFunction;
import org.junit.Test;

public class TwoTierIndexTest {

  static final IntFunction<Tier1> RB = AugmentedRBTree::new;
  static final IntFunction<Tier1> BUF = SortedBufferTier1::new;
  static final IntFunction<Tier1> HYB = HybridTier1::new;

  static void same(String ctx, Answer expect, Answer got) {
    assertArrayEquals(ctx, expect.counts, got.counts);
    assertEquals(ctx + " isFinal", expect.isFinal, got.isFinal);
    assertEquals(ctx + " defined", expect.defined, got.defined);
  }

  static void sameVersion(String ctx, Answer.Version expect, Answer.Version got) {
    assertEquals(ctx + " isFinal", expect.isFinal, got.isFinal);
    assertEquals(ctx + " defined", expect.defined, got.defined);
    assertArrayEquals(ctx, expect.delta, got.delta);
    if (expect.deltaRho == null)
      assertNull(ctx + " deltaRho", got.deltaRho);
    else
      assertArrayEquals(ctx, expect.deltaRho, got.deltaRho, 0.0);
  }

  /** Brute-force version of the K-shard answer: sum the shard oracles, final iff min watermark >= t2. */
  static Answer oracleQuery(Oracle[] os, int m, long t1, long t2) {
    long[] sum = new long[m];
    boolean any = false;
    long frontier = Long.MAX_VALUE;
    for (Oracle o : os) {
      Answer a = o.query(t1, t2);
      for (int c = 0; c < m; c++)
        sum[c] += a.counts[c];
      any |= a.defined;
      frontier = Math.min(frontier, o.wm);
    }
    return new Answer(sum, t2 <= frontier, any);
  }

  static long[] randomVec(Random rnd, int m) {
    long[] v = new long[m];
    if (rnd.nextInt(10) > 0) // zero vectors are legal records too, but rare
      for (int c = 0; c < m; c++)
        v[c] = rnd.nextInt(4);
    return v;
  }

  /**
   * One randomised scenario over K shards (K = 1 is the plain TwoTierIndex path via shard 0).
   * disorder D: an output's ts is `clock - U[0, D]`; a shard's watermark never exceeds
   * clock - D - 1, so only the deliberately late records (3%) can land at or below it.
   */
  static void scenario(IntFunction<Tier1> tier1, int m, int d, int k, int ops, long seed,
      boolean checkInv) {
    Random rnd = new Random(seed);
    ShardedIndex idx = new ShardedIndex(k, m, tier1);
    Oracle[] os = new Oracle[k];
    for (int i = 0; i < k; i++)
      os[i] = new Oracle(m);
    long clock = new long[] {-100, 0, 1_000_000_000_000L}[rnd.nextInt(3)];
    long[] lastTs = new long[k];
    long[][] lastV = new long[k][];
    double[] advanceProb = new double[k]; // skewed watermark speed per shard
    for (int i = 0; i < k; i++)
      advanceProb[i] = 0.02 + 0.3 * rnd.nextDouble();
    String ctx = "m=" + m + " d=" + d + " k=" + k + " seed=" + seed;

    for (int op = 0; op < ops; op++) {
      int kind = rnd.nextInt(100);
      if (kind < 55) {
        int s = rnd.nextInt(k);
        long ts;
        long[] v;
        if (lastV[s] != null && rnd.nextInt(20) == 0) { // exact duplicate output
          ts = lastTs[s];
          v = lastV[s];
        } else {
          if (os[s].wm != Long.MIN_VALUE && rnd.nextInt(33) == 0) // deliberately late
            ts = os[s].wm - rnd.nextInt(4);
          else
            ts = clock - rnd.nextInt(d + 1);
          v = randomVec(rnd, m);
          lastTs[s] = ts;
          lastV[s] = v;
        }
        assertEquals(ctx, os[s].add(ts, v), idx.add(s, ts, v));
      } else if (kind < 65)
        clock += rnd.nextInt(6);
      else if (kind < 80) {
        int s = rnd.nextInt(k);
        if (rnd.nextDouble() < advanceProb[s] * 3) {
          long w = clock - d - 1 - rnd.nextInt(8);
          if (rnd.nextInt(10) == 0)
            w = os[s].wm - rnd.nextInt(3); // regression or repeat: must be ignored
          os[s].advance(w);
          idx.advanceWatermark(s, w);
        }
      } else {
        long wmMin = Long.MAX_VALUE;
        for (Oracle o : os)
          wmMin = Math.min(wmMin, o.wm);
        long lo = clock - 60;
        long t1;
        long t2;
        switch (rnd.nextInt(6)) {
          case 0: // straddling the frontier
            t1 = wmMin == Long.MIN_VALUE ? lo : wmMin - rnd.nextInt(15);
            t2 = t1 + rnd.nextInt(30);
            break;
          case 1: // empty range
            t1 = lo + rnd.nextInt(60);
            t2 = t1 - 1 - rnd.nextInt(3);
            break;
          case 2: // everything
            t1 = Long.MIN_VALUE;
            t2 = Long.MAX_VALUE;
            break;
          case 3: // single point
            t1 = lo + rnd.nextInt(80);
            t2 = t1;
            break;
          default:
            t1 = lo + rnd.nextInt(80);
            t2 = t1 + rnd.nextInt(80);
        }
        same(ctx + " q[" + t1 + "," + t2 + "] op=" + op, oracleQuery(os, m, t1, t2),
            idx.query(t1, t2));
        long b1 = lo + rnd.nextInt(80);
        long b2 = b1 + rnd.nextInt(40);
        Answer.Version want = new Answer.Version(oracleQuery(os, m, t1, t2),
            oracleQuery(os, m, b1, b2));
        sameVersion(ctx + " ver op=" + op, want, idx.versioning(t1, t2, b1, b2));
      }
      if (checkInv)
        for (TwoTierIndex sh : idx.shards)
          if (sh.tier1 instanceof AugmentedRBTree)
            ((AugmentedRBTree) sh.tier1).checkInvariants();
          else if (sh.tier1 instanceof HybridTier1)
            ((HybridTier1) sh.tier1).tree.checkInvariants();
    }

    // Counters agree with the oracle, and nothing is lost between the tiers.
    long late = 0;
    for (int i = 0; i < k; i++) {
      assertEquals(ctx, os[i].late, idx.shards[i].lateCount);
      assertEquals(ctx, os[i].recs.size(),
          idx.shards[i].tier1Size() + idx.shards[i].migratedCount);
      assertEquals(ctx, idx.shards[i].migratedCount, idx.shards[i].tier2Size());
      late += os[i].late;
    }
    assertEquals(ctx, late, idx.lateCount());

    // Flush: a final watermark certifies everything, every query is final and exact.
    for (int i = 0; i < k; i++) {
      os[i].advance(Long.MAX_VALUE);
      idx.advanceWatermark(i, Long.MAX_VALUE);
      assertEquals(0, idx.shards[i].tier1Size());
    }
    same(ctx + " flushed", oracleQuery(os, m, Long.MIN_VALUE, Long.MAX_VALUE),
        idx.query(Long.MIN_VALUE, Long.MAX_VALUE));
    assertTrue(idx.query(clock - 30, clock).isFinal);
  }

  @Test
  public void differentialSingleShard() {
    for (IntFunction<Tier1> t1 : new IntFunction[] {RB, BUF, HYB})
      for (int m : new int[] {1, 2, 4, 16})
        for (int d : new int[] {0, 5, 50})
          for (int seed = 0; seed < 8; seed++)
            scenario(t1, m, d, 1, 1500, seed * 100 + m + d, false);
  }

  @Test
  public void differentialShardedSkewedWatermarks() {
    for (IntFunction<Tier1> t1 : new IntFunction[] {RB, BUF, HYB})
      for (int k : new int[] {1, 2, 4, 8})
        for (int m : new int[] {1, 2, 4, 16})
          for (int seed = 0; seed < 5; seed++)
            scenario(t1, m, 10 + 20 * seed, k, 2000, seed * 7919 + k * 31 + m, false);
  }

  @Test
  public void rbInvariantsAfterEveryOperationInSmallScenarios() {
    for (int m : new int[] {1, 4})
      for (int k : new int[] {1, 4})
        for (int seed = 0; seed < 10; seed++) {
          scenario(RB, m, 4 * seed, k, 400, seed + 1000L * m, true);
          scenario(HYB, m, 4 * seed, k, 400, seed + 1000L * m, true);
        }
  }

  @Test
  public void bothTier1ImplementationsAgreeOnLargerRuns() {
    for (int m : new int[] {2, 4})
      for (int k : new int[] {1, 4})
        scenario(RB, m, 200, k, 20_000, 42 + m + k, false);
    for (int m : new int[] {2, 4})
      for (int k : new int[] {1, 4})
        scenario(BUF, m, 200, k, 20_000, 42 + m + k, false);
    for (int m : new int[] {2, 4})
      for (int k : new int[] {1, 4})
        scenario(HYB, m, 200, k, 20_000, 42 + m + k, false);
  }

  // hand-written cases
  @Test
  public void finalAndProvisionalAnswers() {
    TwoTierIndex idx = new TwoTierIndex(2, new AugmentedRBTree(2));
    idx.add(10, new long[] {1, 0});
    idx.add(20, new long[] {0, 2});
    idx.add(30, new long[] {4, 4});
    idx.advanceWatermark(20); // 10 and 20 are certified, 30 is not
    Answer fin = idx.query(0, 20);
    assertTrue(fin.isFinal);
    assertArrayEquals(new long[] {1, 2}, fin.counts);
    Answer prov = idx.query(0, 30);
    assertFalse(prov.isFinal);
    assertArrayEquals(new long[] {5, 6}, prov.counts);
    Answer onlyProv = idx.query(25, 40);
    assertFalse(onlyProv.isFinal);
    assertArrayEquals(new long[] {4, 4}, onlyProv.counts);
    idx.advanceWatermark(30);
    assertTrue(idx.query(0, 30).isFinal);
    assertArrayEquals(new long[] {5, 6}, idx.query(0, 30).counts);
  }

  @Test
  public void lateRecordsAreCountedNotIndexed() {
    TwoTierIndex idx = new TwoTierIndex(1, new AugmentedRBTree(1));
    idx.advanceWatermark(100);
    assertFalse(idx.add(100, new long[] {5})); // ts <= W is late
    assertFalse(idx.add(3, new long[] {5}));
    assertTrue(idx.add(101, new long[] {7}));
    assertEquals(2, idx.lateCount);
    assertArrayEquals(new long[] {7}, idx.query(0, 1000).counts);
    assertEquals(1, idx.peakTier1);
  }

  @Test
  public void watermarkNeverGoesBackwards() {
    TwoTierIndex idx = new TwoTierIndex(1, new SortedBufferTier1(1));
    idx.advanceWatermark(50);
    idx.advanceWatermark(40);
    idx.advanceWatermark(50);
    assertEquals(50, idx.watermark());
    assertFalse(idx.add(45, new long[] {1}));
    assertFalse(idx.query(0, 60).isFinal);
    assertTrue(idx.query(0, 50).isFinal);
  }

  @Test
  public void emptyIntervalIsUndefinedNotZero() {
    for (IntFunction<Tier1> f : new IntFunction[] {RB, BUF, HYB}) {
      TwoTierIndex idx = new TwoTierIndex(2, f.apply(2));
      Answer none = idx.query(0, 100); // nothing indexed at all
      assertFalse(none.defined);
      assertArrayEquals(new long[2], none.counts);
      idx.add(10, new long[] {1, 1});
      idx.add(500, new long[] {2, 0});
      idx.advanceWatermark(100);
      assertTrue(idx.query(0, 100).defined);
      assertFalse(idx.query(11, 100).defined); // final, empty
      assertFalse(idx.query(11, 400).defined); // straddles W, empty
      assertTrue(idx.query(11, 600).defined); // provisional part has the record at 500
      assertFalse(idx.query(50, 40).defined); // t1 > t2
      Answer.Version v = idx.versioning(0, 100, 11, 100);
      assertFalse(v.defined);
      assertNull(v.delta);
      assertNull(v.deltaRho);
      assertTrue(v.isFinal);
      Answer.Version w = idx.versioning(0, 100, 0, 600);
      assertTrue(w.defined);
      assertFalse(w.isFinal);
      assertArrayEquals(new long[] {2, 0}, w.delta);
      // rho(a) = (1/2, 1/2); rho(b) = (3/4, 1/4)
      assertArrayEquals(new double[] {0.25, -0.25}, w.deltaRho, 1e-12);
    }
  }

  @Test
  public void shareIsUndefinedWhenTheDenominatorIsZero() {
    for (IntFunction<Tier1> f : new IntFunction[] {RB, BUF, HYB}) {
      TwoTierIndex idx = new TwoTierIndex(2, f.apply(2));
      idx.add(5, new long[] {0, 0}); // an output exists, but carries no contribution
      idx.add(20, new long[] {1, 3});
      idx.advanceWatermark(30);
      Answer zero = idx.query(0, 10);
      assertTrue(zero.defined);
      assertArrayEquals(new long[] {0, 0}, zero.counts);
      assertNull("rho over a zero total is bottom, not 0", zero.shares());
      assertArrayEquals(new double[] {0.25, 0.75}, idx.query(0, 30).shares(), 1e-12);
      assertNull(new Answer(new long[] {0, 0}, true, false).shares()); // no output at all
      Answer.Version v = idx.versioning(0, 10, 0, 30); // a has total 0, b does not
      assertTrue(v.defined);
      assertArrayEquals("delta stays defined", new long[] {1, 3}, v.delta);
      assertNull("deltaRho is bottom if either total is 0", v.deltaRho);
      Answer.Version w = idx.versioning(0, 30, 0, 10);
      assertArrayEquals(new long[] {-1, -3}, w.delta);
      assertNull(w.deltaRho);
      Answer.Version both = idx.versioning(0, 30, 15, 30); // both totals positive: rho defined
      assertTrue(both.defined);
      assertArrayEquals(new double[] {0, 0}, both.deltaRho, 1e-12);
    }
  }

  @Test
  public void recordWithZeroVectorStillMakesTheIntervalDefined() {
    TwoTierIndex idx = new TwoTierIndex(3, new AugmentedRBTree(3));
    idx.add(5, new long[3]);
    idx.advanceWatermark(5);
    assertTrue(idx.query(0, 10).defined);
    assertTrue(idx.query(5, 5).defined);
    assertFalse(idx.query(6, 10).defined);
  }

  @Test
  public void shardedFrontierIsTheMinimum() {
    ShardedIndex idx = new ShardedIndex(3, 1, RB);
    idx.add(0, 10, new long[] {1});
    idx.add(1, 12, new long[] {2});
    idx.add(2, 11, new long[] {4});
    idx.advanceWatermark(0, 100);
    idx.advanceWatermark(1, 100);
    assertEquals(Long.MIN_VALUE, idx.frontier());
    Answer a = idx.query(0, 50);
    assertFalse(a.isFinal); // shard 2 is still at -inf
    assertArrayEquals(new long[] {7}, a.counts);
    idx.advanceWatermark(2, 20);
    assertEquals(20, idx.frontier());
    assertTrue(idx.query(0, 20).isFinal);
    assertFalse(idx.query(0, 21).isFinal);
    assertArrayEquals(new long[] {7}, idx.query(0, 50).counts);
    // bottom only if every shard is bottom
    assertTrue(idx.query(12, 12).defined);
    assertFalse(idx.query(13, 99).defined);
  }

  @Test
  public void expiryActsOnTier2Only() {
    TwoTierIndex idx = new TwoTierIndex(1, new AugmentedRBTree(1));
    for (int t = 1; t <= 100; t++)
      idx.add(t, new long[] {t});
    idx.advanceWatermark(80);
    idx.expireBefore(50);
    // [51, 80] final and exact after the guard row; (80, 100] still provisional
    Answer a = idx.query(51, 100);
    assertArrayEquals(new long[] {(51 + 100) * 50L / 2}, a.counts);
    assertFalse(a.isFinal);
    assertTrue(idx.query(51, 80).defined);
    idx.summarizeBefore(70, 5);
    assertArrayEquals(new long[] {(71 + 100) * 30L / 2}, idx.query(71, 100).counts);
    assertTrue(idx.query(71, 100).defined);
  }
}
