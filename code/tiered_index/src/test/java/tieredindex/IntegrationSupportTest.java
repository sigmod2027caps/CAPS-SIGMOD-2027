package tieredindex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Test;

// Pieces the Flink sinks rely on: migration listener, O(1) empty watermark, WatermarkOrderer
// (object payloads for the GeneaLog baseline), ShardGroup (post-job assembly of the shards).
public class IntegrationSupportTest {

  @Test
  public void listenerSeesTheTier2StreamInOrder() {
    for (long seed = 0; seed < 20; seed++) {
      Random rnd = new Random(seed);
      TwoTierIndex idx = new TwoTierIndex(3, seed % 3 == 0 ? new AugmentedRBTree(3) : seed % 3 == 1 ? new SortedBufferTier1(3) : new HybridTier1(3));
      List<long[]> seen = new ArrayList<>();
      idx.listener = (ts, vals, off) -> seen.add(new long[] {ts, vals[off], vals[off + 1], vals[off + 2]});
      long clock = 0;
      int accepted = 0;
      for (int i = 0; i < 3000; i++) {
        clock += rnd.nextInt(5);
        long ts = clock - rnd.nextInt(40);
        if (idx.add(ts, new long[] {1, ts & 7, i}))
          accepted++;
        if (rnd.nextInt(7) == 0)
          idx.advanceWatermark(clock - 41 - rnd.nextInt(3));
      }
      idx.advanceWatermark(Long.MAX_VALUE);
      assertEquals(accepted, seen.size());
      assertEquals(accepted, idx.tier2Size());
      assertEquals(accepted, idx.migratedCount);
      long prev = Long.MIN_VALUE;
      for (long[] r : seen) {
        assertTrue(r[0] >= prev);
        prev = r[0];
      }
      // the listener got the same counts as Tier 2: total of channel 0 over the stream
      assertEquals(accepted, idx.query(Long.MIN_VALUE, Long.MAX_VALUE).counts[0]);
    }
  }

  @Test
  public void watermarkThatCertifiesNothingIsFree() {
    TwoTierIndex idx = new TwoTierIndex(1, new AugmentedRBTree(1));
    idx.timing = true; // off by default
    idx.add(100, new long[] {1});
    for (long w = 0; w < 100; w++)
      idx.advanceWatermark(w);
    assertEquals(0, idx.migratedCount);
    assertEquals(0, idx.splitNanos);
    assertEquals(99, idx.watermark());
    idx.advanceWatermark(100);
    assertEquals(1, idx.migratedCount);
    assertTrue(idx.splitNanos > 0);
  }

  @Test
  public void timingIsOffByDefault() {
    TwoTierIndex idx = new TwoTierIndex(1, new HybridTier1(1));
    for (int i = 0; i < 100; i++)
      idx.add(i, new long[] {1});
    idx.advanceWatermark(1000);
    assertEquals(100, idx.migratedCount);
    assertEquals(0, idx.insertNanos);
    assertEquals(0, idx.splitNanos);
    assertEquals(0, idx.migrationNanos);
    WatermarkOrderer<String> o = new WatermarkOrderer<>();
    o.add(5, "x");
    o.advanceWatermark(10, (ts, p) -> { });
    assertEquals(0, o.insertNanos);
    assertEquals(0, o.migrationNanos);
  }

  @Test
  public void definedFromPositionsNotFromACountChannel() {
    TwoTierIndex idx = new TwoTierIndex(2, new AugmentedRBTree(2));
    idx.add(5, new long[] {0, 0}); // a zero-vector output still makes the interval defined
    idx.add(9, new long[] {1, 2});
    idx.advanceWatermark(6);
    assertTrue(idx.query(5, 5).defined);
    assertFalse(idx.query(6, 8).defined); // final, nothing there
    assertTrue(idx.query(6, 9).defined); // provisional part holds the record at 9
    assertFalse(idx.query(6, 8).counts[0] != 0);
    assertTrue(idx.tier2().memoryBytes() > 0);
    assertEquals(2, idx.tier2().channels()); // no extra count channel
  }

  @Test
  public void watermarkOrdererAppendsInOrderAndRecyclesSlots() {
    for (long seed = 0; seed < 20; seed++) {
      Random rnd = new Random(seed);
      WatermarkOrderer<String> o = new WatermarkOrderer<>();
      List<long[]> out = new ArrayList<>();
      List<String> outP = new ArrayList<>();
      WatermarkOrderer.Appender<String> sink = (ts, p) -> {
        out.add(new long[] {ts});
        outP.add(p);
      };
      long clock = 0;
      int accepted = 0;
      int late = 0;
      List<long[]> expect = new ArrayList<>(); // ts, arrival index
      for (int i = 0; i < 5000; i++) {
        clock += rnd.nextInt(4);
        long ts = clock - rnd.nextInt(30);
        if (o.add(ts, Integer.toString(i))) {
          accepted++;
          expect.add(new long[] {ts, i});
        } else
          late++;
        if (rnd.nextInt(5) == 0)
          o.advanceWatermark(clock - 31, sink);
      }
      o.advanceWatermark(Long.MAX_VALUE, sink);
      assertEquals(accepted, out.size());
      assertEquals(late, o.lateCount);
      assertEquals(0, o.tier1Size());
      expect.sort((a, b) -> a[0] != b[0] ? Long.compare(a[0], b[0]) : Long.compare(a[1], b[1]));
      for (int i = 0; i < accepted; i++) {
        assertEquals(expect.get(i)[0], out.get(i)[0]);
        assertEquals(Long.toString(expect.get(i)[1]), outP.get(i));
      }
      assertTrue("slots are recycled", o.nextSlot < 5000);
    }
  }

  @Test
  public void shardGroupHandsAllShardsToTheLastSubtask() throws Exception {
    final int k = 4;
    final String key = "test-group";
    final List<List<Integer>> results = new ArrayList<>();
    Thread[] ts = new Thread[k];
    for (int i = 0; i < k; i++) {
      final int sub = i;
      ts[i] = new Thread(() -> {
        List<Integer> r = ShardGroup.arrive(key, k, sub, sub * 10);
        if (r != null)
          synchronized (results) {
            results.add(r);
          }
      });
    }
    for (Thread t : ts)
      t.start();
    for (Thread t : ts)
      t.join();
    assertEquals(1, results.size());
    assertEquals(k, results.get(0).size());
    for (int i = 0; i < k; i++)
      assertEquals(Integer.valueOf(i * 10), results.get(0).get(i));
    // the key is free again
    assertNull(ShardGroup.arrive(key, 2, 1, 1));
    assertNotNull(ShardGroup.arrive(key, 2, 0, 0));
  }

  @Test
  public void shardedIndexOverExistingShardsAndSingleShardShortcut() {
    TwoTierIndex a = new TwoTierIndex(1, new AugmentedRBTree(1));
    TwoTierIndex b = new TwoTierIndex(1, new AugmentedRBTree(1));
    a.add(1, new long[] {2});
    b.add(2, new long[] {3});
    a.advanceWatermark(10);
    b.advanceWatermark(5);
    ShardedIndex two = new ShardedIndex(new TwoTierIndex[] {a, b});
    assertEquals(5, two.query(0, 4).counts[0]);
    assertTrue(two.query(0, 5).isFinal);
    assertFalse(two.query(0, 6).isFinal);
    ShardedIndex one = new ShardedIndex(new TwoTierIndex[] {a});
    assertEquals(2, one.query(0, 4).counts[0]);
    assertTrue(one.query(0, 10).isFinal);
  }
}
