package tieredindex;

import java.util.Arrays;

/**
 * Tier 1 for payloads that are objects (the GeneaLog baseline stores output tuples with their
 * provenance graph, not a count vector): the same HybridTier1 (ordered run + AugmentedRBTree), with one channel that holds the
 * slot of the payload in a side table, so the baselines face exactly the disorder CAPS faces.
 * add(ts, p) buffers the record (false if late), advanceWatermark(W, sink) appends every record with
 * ts &lt;= W to the sink in timestamp order (ties in arrival order). The tree's sums are not used.
 * For the baselines: GeneaLog's and Green's temporal-query sinks order their outputs exactly like CAPS.
 */
public class WatermarkOrderer<P> {

  /** Receives the certified records in timestamp order (the ordered, append-only structure). */
  public interface Appender<P> {
    void append(long ts, P payload);
  }

  final HybridTier1 tier1 = new HybridTier1(1);
  final long[] slotRow = new long[1];
  Object[] slots = new Object[1024];
  int[] free = new int[1024]; // stack of recycled slots
  int nFree;
  int nextSlot;
  long watermark = Long.MIN_VALUE;

  public boolean timing = false; // true: nanoTime around every insert and split (explicit, never in a timed run)
  public long lateCount;
  public long migratedCount;
  public long insertNanos;
  public long migrationNanos; // split plus the appender calls
  public int peakTier1;

  public long watermark() {
    return watermark;
  }

  public int tier1Size() {
    return tier1.size();
  }

  /** Buffers one output until the watermark passes it. False (counted late) if ts is already at or below the watermark. */
  public boolean add(long ts, P payload) {
    if (ts <= watermark) {
      lateCount++;
      return false;
    }
    long t0 = timing ? System.nanoTime() : 0;
    int slot;
    if (nFree > 0)
      slot = free[--nFree];
    else {
      if (nextSlot == slots.length)
        slots = Arrays.copyOf(slots, nextSlot * 2);
      slot = nextSlot++;
    }
    slots[slot] = payload;
    slotRow[0] = slot;
    tier1.insert(ts, slotRow);
    if (timing)
      insertNanos += System.nanoTime() - t0;
    if (tier1.size() > peakTier1)
      peakTier1 = tier1.size();
    return true;
  }

  @SuppressWarnings("unchecked")
  public void advanceWatermark(long w, Appender<P> sink) {
    if (w <= watermark)
      return;
    watermark = w;
    if (tier1.minTs() > w)
      return;
    long t0 = timing ? System.nanoTime() : 0;
    Tier1.Entries e = tier1.splitPrefix(w);
    if (nFree + e.n > free.length)
      free = Arrays.copyOf(free, Math.max(free.length * 2, nFree + e.n));
    for (int i = 0; i < e.n; i++) {
      int slot = (int) e.vals[i];
      P p = (P) slots[slot];
      slots[slot] = null;
      free[nFree++] = slot;
      sink.append(e.ts[i], p);
    }
    migratedCount += e.n;
    if (timing)
      migrationNanos += System.nanoTime() - t0;
  }
}
