package tieredindex;

import java.util.ArrayList;
import java.util.List;

// Brute-force reference for one shard: a plain list of accepted records and a watermark.
final class Oracle {
  final int m;
  final List<long[]> recs = new ArrayList<>(); // row = [ts, v_0 .. v_{m-1}]
  long wm = Long.MIN_VALUE;
  long late;
  long minTs = Long.MAX_VALUE;

  Oracle(int m) {
    this.m = m;
  }

  boolean add(long ts, long[] v) {
    if (ts <= wm) {
      late++;
      return false;
    }
    long[] row = new long[m + 1];
    row[0] = ts;
    System.arraycopy(v, 0, row, 1, m);
    recs.add(row);
    minTs = Math.min(minTs, ts);
    return true;
  }

  void advance(long w) {
    wm = Math.max(wm, w);
  }

  /** Counts and existence over [t1, t2], summed straight from the list. */
  Answer query(long t1, long t2) {
    long[] sum = new long[m];
    boolean any = false;
    for (long[] r : recs)
      if (r[0] >= t1 && r[0] <= t2) {
        any = true;
        for (int c = 0; c < m; c++)
          sum[c] += r[1 + c];
      }
    return new Answer(sum, t2 <= wm, any);
  }
}
