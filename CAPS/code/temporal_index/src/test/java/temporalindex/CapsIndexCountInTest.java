package temporalindex;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import org.junit.Test;

public class CapsIndexCountInTest {

  @Test
  public void countInMatchesBruteForceAcrossExpiry() {
    Random rnd = new Random(7);
    CapsIndex idx = new CapsIndex(2);
    long[] ts = new long[5000];
    long t = 0;
    for (int i = 0; i < ts.length; i++) {
      t += rnd.nextInt(3); // duplicates
      ts[i] = t;
      idx.append(t, new long[] {1, 2});
    }
    for (int q = 0; q < 2000; q++) {
      long a = rnd.nextInt((int) t + 10) - 5;
      long b = a + rnd.nextInt(200) - 20;
      int expect = 0;
      if (a <= b)
        for (long x : ts)
          if (x >= a && x <= b)
            expect++;
      assertEquals(expect, idx.countIn(a, b));
    }
    assertEquals(ts.length, idx.countIn(Long.MIN_VALUE, Long.MAX_VALUE));
    idx.expireBefore(ts[2500]);
    int live = 0;
    for (long x : ts)
      if (x >= ts[2500])
        live++;
    assertEquals(live, idx.countIn(Long.MIN_VALUE, Long.MAX_VALUE));
    assertEquals(0, idx.countIn(5, 4));
  }

  @Test
  public void mergeHelpersCombineSubtaskState() {
    LogicalSampler.SelectedSet a = new LogicalSampler.SelectedSet();
    LogicalSampler.SelectedSet b = new LogicalSampler.SelectedSet();
    a.add(1);
    a.add(1);
    b.add(1);
    b.add(2);
    a.merge(b);
    assertEquals(4, a.selected());
    assertEquals(2, a.unique());

    LogicalSampler.VerifyGroup g = new LogicalSampler.VerifyGroup();
    LogicalSampler.VerifyGroup h = new LogicalSampler.VerifyGroup();
    g.add(Long.valueOf(5), 6, 7);
    h.add(Long.valueOf(8), 9, 10);
    LogicalSampler.VerifyGroup both = new LogicalSampler.VerifyGroup();
    both.add(Long.valueOf(8), 9, 10);
    both.add(Long.valueOf(5), 6, 7);
    g.merge(h);
    assertEquals(2, g.count);
    org.junit.Assert.assertArrayEquals(both.vectorDigest(), g.vectorDigest());

    QueryIdManifest.Generator x = new QueryIdManifest.Generator(5, 42L);
    QueryIdManifest.Generator y = new QueryIdManifest.Generator(5, 42L);
    QueryIdManifest.Generator all = new QueryIdManifest.Generator(5, 42L);
    for (long i = 0; i < 200; i++) {
      (i % 2 == 0 ? x : y).observe(i * 7919);
      all.observe(i * 7919);
    }
    x.merge(y);
    assertEquals(all.retained(), x.retained());
  }
}
