package temporalindex;

import java.util.ArrayList;
import java.util.List;

public class TemporalIndex {

  
  public static final int CAPACITY = 1024;

  private final LongRing ts = new LongRing();

  
  private long expired;

  private final List<Level> levels = new ArrayList<>();
  private long currentBlockFirstKey;

  
  private static final class Level {
    final LongRing keys = new LongRing();
    long dropped;
    long pendingGroupKey; 

    long appended() {
      return dropped + keys.size();
    }
  }

  
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

  
  public int search(long t) {
    if (ts.size() == 0 || t < ts.get(0)) {
      return -1;
    }
    long child = 0; 
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

  
  public int lowerBound(long t) {
    return search(t - 1) + 1;
  }

  
  public int upperBound(long t) {
    return search(t) + 1;
  }

  
  public void dropFront(int k) {
    ts.removeFirst(k);
    expired += k;
    long childDropped = expired / CAPACITY; 
    for (Level lev : levels) {
      long target = Math.min(childDropped, lev.appended());
      while (lev.dropped < target) {
        lev.keys.removeFirst(1);
        lev.dropped++;
      }
      childDropped = lev.dropped / CAPACITY; 
    }
  }

  
  public long separatorEntries() {
    long entries = 0;
    for (Level lev : levels) {
      entries += lev.keys.size();
    }
    return entries;
  }

  
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

  
  static final class LongRing {
    private long[] a = new long[16];
    private int head; 
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
