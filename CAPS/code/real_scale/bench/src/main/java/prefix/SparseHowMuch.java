package prefix;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Compressed how-much annotation: only the non-zero (channel, count) pairs, as two parallel primitive
 * arrays sorted by channel. Semantically identical to caps.HowMuch (count(c) equals the dense counter
 * of channel c for every c); the size is O(support) instead of O(M). Arrays have exactly the support
 * size (no slack), so the retained size is 12 B per pair + the two array headers.
 */
public class SparseHowMuch implements Serializable {

  private static final long serialVersionUID = 1L;

  public int[] ch;     // strictly increasing channel ids
  public long[] cnt;   // cnt[i] > 0, the counter of channel ch[i]

  public SparseHowMuch() {
    ch = new int[0];
    cnt = new long[0];
  }

  private SparseHowMuch(int[] ch, long[] cnt) {
    this.ch = ch;
    this.cnt = cnt;
  }

  /** Source instrumentation: a fresh tuple counting 1 on the given channel. */
  public static SparseHowMuch one(int channel) {
    return new SparseHowMuch(new int[] {channel}, new long[] {1});
  }

  public SparseHowMuch copy() {
    return new SparseHowMuch(ch.clone(), cnt.clone());
  }

  /** Aggregate / join instrumentation: channel-wise sum of another annotation, a sorted merge. */
  public void mergeWith(SparseHowMuch o) {
    int n = ch.length;
    int m = o.ch.length;
    if (m == 0) return;
    // size of the union
    int i = 0, j = 0, u = 0;
    while (i < n && j < m) {
      if (ch[i] < o.ch[j]) i++;
      else if (ch[i] > o.ch[j]) j++;
      else {
        i++;
        j++;
      }
      u++;
    }
    u += (n - i) + (m - j);
    if (u == n) { // every channel of o is already here: add in place, no allocation
      i = 0;
      for (j = 0; j < m; j++) {
        while (ch[i] < o.ch[j]) i++;
        cnt[i] += o.cnt[j];
      }
      return;
    }
    int[] nc = new int[u];
    long[] nn = new long[u];
    i = 0;
    j = 0;
    int k = 0;
    while (i < n && j < m) {
      if (ch[i] < o.ch[j]) {
        nc[k] = ch[i];
        nn[k++] = cnt[i++];
      } else if (ch[i] > o.ch[j]) {
        nc[k] = o.ch[j];
        nn[k++] = o.cnt[j++];
      } else {
        nc[k] = ch[i];
        nn[k++] = cnt[i++] + o.cnt[j++];
      }
    }
    while (i < n) {
      nc[k] = ch[i];
      nn[k++] = cnt[i++];
    }
    while (j < m) {
      nc[k] = o.ch[j];
      nn[k++] = o.cnt[j++];
    }
    ch = nc;
    cnt = nn;
  }

  public static SparseHowMuch merge(SparseHowMuch a, SparseHowMuch b) {
    SparseHowMuch r = a.copy();
    r.mergeWith(b);
    return r;
  }

  /** The counter of one channel (0 if absent): O(log support). */
  public long count(int channel) {
    int p = Arrays.binarySearch(ch, channel);
    return p >= 0 ? cnt[p] : 0;
  }

  /** Scalar fold: sum of the counters, no allocation. */
  public long total() {
    long s = 0;
    for (int i = 0; i < cnt.length; i++) s += cnt[i];
    return s;
  }

  /** Number of stored pairs. */
  public int support() {
    return ch.length;
  }

  /** The dense vector HowMuch.counts would hold. */
  public long[] toDense(int channels) {
    long[] d = new long[channels];
    for (int i = 0; i < ch.length; i++) d[ch[i]] = cnt[i];
    return d;
  }

  /** Payload bytes: 4 B channel id + 8 B count per stored pair. */
  public long payloadBytes() {
    return 12L * ch.length;
  }

  /** "c:n;c:n" over the non-zero channels, the row format of the file sinks. */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < ch.length; i++) {
      if (i > 0) sb.append(';');
      sb.append(ch[i]).append(':').append(cnt[i]);
    }
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SparseHowMuch)) return false;
    SparseHowMuch s = (SparseHowMuch) o;
    return Arrays.equals(ch, s.ch) && Arrays.equals(cnt, s.cnt);
  }

  @Override
  public int hashCode() {
    return 31 * Arrays.hashCode(ch) + Arrays.hashCode(cnt);
  }
}
