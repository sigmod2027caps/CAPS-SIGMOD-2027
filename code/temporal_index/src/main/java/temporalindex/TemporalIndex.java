package temporalindex;

import java.util.ArrayList;
import java.util.List;

// Paper 5.4 sparse B-tree over a ring-buffered timestamp sequence; CapsIndex and GenealogIndex share this core.
// Block indices count from stream start (absolute), not from the live window — expiry only advances heads.
public class TemporalIndex {

  // Leaf block size — also the fan-out at every separator level (paper Fig. index).
  public static final int CAPACITY = 1024;

  private final LongRing ts = new LongRing();

  /** Records dropped from the front so far = absolute index of position 0. */
  private long expired;

  private final List<Level> levels = new ArrayList<>();
  private long currentBlockFirstKey;

  /** One separator level: a ring of keys plus how many were dropped from the front. */
  private static final class Level {
    final LongRing keys = new LongRing();
    long dropped;
    // First key of the group being filled — may already be expired when the group completes.
    long pendingGroupKey;

    long appended() {
      return dropped + keys.size();
    }
  }

  /**
   * Append the next timestamp (must be non-decreasing) and return its
   * position; the caller stores its payload at the same position.
   */
  public int append(long timestamp) {
    if (ts.size() > 0 && timestamp < ts.get(ts.size() - 1)) {
      throw new IllegalArgumentException("records must be appended in time order: "
          + timestamp + " < " + ts.get(ts.size() - 1));
    }
    long abs = expired + ts.size();
    if (abs % CAPACITY == 0) {
      currentBlockFirstKey = timestamp;
    }
    ts.addLast(timestamp);
    // Block complete: append its first timestamp as a level-0 separator.
    if ((abs + 1) % CAPACITY == 0) {
      addSeparator(currentBlockFirstKey);
    }
    return ts.size() - 1;
  }

  public int size() {
    return ts.size();
  }

  public long tsAt(int i) {
    return ts.get(i);
  }

  /**
   * Position of the last record with ts <= t, or -1 if none. Walks the
   * separator levels top down, then binary-searches the selected leaf tail.
   */
  public int search(long t) {
    if (ts.size() == 0 || t < ts.get(0)) {
      return -1;
    }
    long child = 0; // absolute block/group index selected by the level above
    for (int l = levels.size() - 1; l >= 0; l--) {
      Level lev = levels.get(l);
      long absStart = Math.max(child * CAPACITY, lev.dropped);
      long absEnd = Math.min(child * CAPACITY + CAPACITY, lev.appended());
      int pos = upperBound(lev.keys, (int) (absStart - lev.dropped),
          (int) (absEnd - lev.dropped), t);
      long absPos = pos + lev.dropped;
      child = absPos > absStart ? absPos - 1 : absStart;
    }
    long absLeafStart = Math.max(child * CAPACITY, expired);
    int pos = upperBound(ts, (int) (absLeafStart - expired), ts.size(), t);
    return pos - 1;
  }

  /** First position with ts >= t (start of a [t, ...] range). */
  public int lowerBound(long t) {
    return search(t - 1) + 1;
  }

  /** First position with ts > t (exclusive end of a [..., t] range). */
  public int upperBound(long t) {
    return search(t) + 1;
  }

  /**
   * Drop positions [0, k) — expiry. O(1) plus one separator removal per fully
   * expired block/group: the ring head advances and each level sheds the
   * separators of blocks that no longer have live records. The caller drops
   * the front of its payload arrays the same way.
   */
  public void dropFront(int k) {
    ts.removeFirst(k);
    expired += k;
    long childDropped = expired / CAPACITY; // fully expired leaf blocks
    for (Level lev : levels) {
      long target = Math.min(childDropped, lev.appended());
      while (lev.dropped < target) {
        lev.keys.removeFirst(1);
        lev.dropped++;
      }
      childDropped = lev.dropped / CAPACITY; // fully expired groups one level up
    }
  }

  /** Number of separator entries across all levels (for memory accounting). */
  public long separatorEntries() {
    long entries = 0;
    for (Level lev : levels) {
      entries += lev.keys.size();
    }
    return entries;
  }

  /**
   * Append a level-0 separator and propagate: every CAPACITY separators at a
   * level push the group's first separator up one level.
   * Group alignment is absolute, so the group's first key is remembered when
   * the group starts (it may already be expired when the group completes).
   */
  private void addSeparator(long key) {
    long entry = key;
    for (int l = 0; ; l++) {
      if (l >= levels.size()) {
        levels.add(new Level());
      }
      Level lev = levels.get(l);
      long abs = lev.appended();
      if (abs % CAPACITY == 0) {
        lev.pendingGroupKey = entry;
      }
      lev.keys.addLast(entry);
      if ((abs + 1) % CAPACITY != 0) {
        break;
      }
      entry = lev.pendingGroupKey;
    }
  }

  /** First index in [from, to) with value > t (values ascending). */
  private static int upperBound(LongRing a, int from, int to, long t) {
    int lo = from;
    int hi = to;
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (a.get(mid) <= t) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  /**
   * Growable ring buffer of longs (the torus): O(1) addLast, O(1) removeFirst,
   * O(1) get by logical index. Capacity is a power of two so the wrap-around
   * is a mask. Freed front slots are reused by later appends.
   */
  static final class LongRing {
    private long[] a = new long[16];
    private int head; // physical index of logical position 0
    private int n;

    void addLast(long v) {
      if (n == a.length) {
        grow();
      }
      a[(head + n) & (a.length - 1)] = v;
      n++;
    }

    void removeFirst(int k) {
      head = (head + k) & (a.length - 1);
      n -= k;
    }

    long get(int i) {
      return a[(head + i) & (a.length - 1)];
    }

    int size() {
      return n;
    }

    private void grow() {
      long[] bigger = new long[a.length * 2];
      for (int i = 0; i < n; i++) {
        bigger[i] = get(i);
      }
      a = bigger;
      head = 0;
    }
  }
}
