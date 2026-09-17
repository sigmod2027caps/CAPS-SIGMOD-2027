package temporalindex;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// Standalone checks for LogicalSampler — run with java -cp target/classes temporalindex.LogicalSamplerSelfTest.
public final class LogicalSamplerSelfTest {

  private static final SampleKeyFn<long[]> KEY = new SampleKeyFn<long[]>() {
    @Override
    public void writeKey(long[] value, DataOutput out) throws IOException {
      for (int i = 0; i < value.length; i++) {
        out.writeLong(value[i]);
      }
    }
  };

  private LogicalSamplerSelfTest() {}

  public static void main(String[] args) {
    try {
      reorderDoesNotChangeSelectedIdentities();
      duplicatesSelectedTogether();
      arrivalIndexWouldDisagree();
      harnessChecksumIsOrderIndependent();
      momentCollisionDigestsDiffer();
      swappedVectorsDigestsDiffer();
      System.out.println("LOGICALSAMPLER SELFTEST OK");
    } catch (AssertionError e) {
      System.err.println("LOGICALSAMPLER SELFTEST FAIL: " + e.getMessage());
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  private static void reorderDoesNotChangeSelectedIdentities() {
    long[][] orderA = keys(8);
    long[][] orderB = new long[orderA.length][];
    for (int i = 0; i < orderA.length; i++) {
      orderB[i] = orderA[orderA.length - 1 - i];
    }
    List<String> a = select(orderA, 0.4);
    List<String> b = select(orderB, 0.4);
    String[] as = a.toArray(new String[a.size()]);
    String[] bs = b.toArray(new String[b.size()]);
    Arrays.sort(as);
    Arrays.sort(bs);
    check(Arrays.equals(as, bs),
        "reorder changed selected identities: " + a + " vs " + b);
  }

  private static void duplicatesSelectedTogether() {
    long[] k = new long[] {1L, 1L, 1L};
    long[] other = new long[] {2L, 2L, 2L};
    long[][] items = new long[][] {k, other, k, k};
    List<String> selected = select(items, 0.5);
    int kCount = 0;
    for (int i = 0; i < selected.size(); i++) {
      if (selected.get(i).equals(id(k))) {
        kCount++;
      }
    }
    // Same logical key must select together — seperate rule from arrival-index sampling.
    check(kCount == 0 || kCount == 3,
        "duplicates split: " + selected);
  }

  // Proves arrival-index LCG sampling is order-dependent — that is why we hash logical keys instead.
  private static void arrivalIndexWouldDisagree() {
    long[][] orderA = keys(8);
    long[][] orderB = new long[orderA.length][];
    for (int i = 0; i < orderA.length; i++) {
      orderB[i] = orderA[orderA.length - 1 - i];
    }
    List<String> logical = select(orderA, 0.4);
    List<String> indexA = arrivalIndexSelect(orderA, 0.4);
    List<String> indexB = arrivalIndexSelect(orderB, 0.4);
    String[] ia = indexA.toArray(new String[indexA.size()]);
    String[] ib = indexB.toArray(new String[indexB.size()]);
    Arrays.sort(ia);
    Arrays.sort(ib);
    check(!Arrays.equals(ia, ib),
        "arrival-index replica unexpectedly order-independent; test is vacuous");
    check(!sameSet(logical, indexA) || !sameSet(logical, indexB),
        "logical sampler matched arrival-index on a known-disagreeing pair");
  }

  private static void harnessChecksumIsOrderIndependent() {
    long[][] orderA = keys(8);
    long[][] orderB = new long[orderA.length][];
    for (int i = 0; i < orderA.length; i++) {
      orderB[i] = orderA[orderA.length - 1 - i];
    }
    check(predicateChecksum(orderA) == predicateChecksum(orderB),
        "always-per-output hash checksum depends on arrival order");
  }

  private static void momentCollisionDigestsDiffer() {
    ArrayList<Long> a = new ArrayList<Long>();
    a.add(Long.valueOf(LogicalSampler.hashScalar(0)));
    a.add(Long.valueOf(LogicalSampler.hashScalar(3)));
    a.add(Long.valueOf(LogicalSampler.hashScalar(3)));
    ArrayList<Long> b = new ArrayList<Long>();
    b.add(Long.valueOf(LogicalSampler.hashScalar(1)));
    b.add(Long.valueOf(LogicalSampler.hashScalar(1)));
    b.add(Long.valueOf(LogicalSampler.hashScalar(4)));
    long[] da = LogicalSampler.digest128(a);
    long[] db = LogicalSampler.digest128(b);
    check(da[0] != db[0] || da[1] != db[1],
        "128-bit digest collided on {0,3,3} vs {1,1,4}");
  }

  private static void swappedVectorsDigestsDiffer() {
    long ha = LogicalSampler.hashLongs(new long[] {3, 1});
    long hb = LogicalSampler.hashLongs(new long[] {1, 3});
    check(ha != hb, "hashLongs([3,1]) == hashLongs([1,3])");
    ArrayList<Long> a = new ArrayList<Long>();
    a.add(Long.valueOf(ha));
    ArrayList<Long> b = new ArrayList<Long>();
    b.add(Long.valueOf(hb));
    long[] da = LogicalSampler.digest128(a);
    long[] db = LogicalSampler.digest128(b);
    check(da[0] != db[0] || da[1] != db[1],
        "group digest accepted swapped channel vectors");
  }

  private static List<String> select(long[][] items, double p) {
    LogicalSampler sampler = new LogicalSampler();
    long thresh = LogicalSampler.threshold53(p);
    List<String> out = new ArrayList<String>();
    for (int i = 0; i < items.length; i++) {
      long h = sampler.hashKey(KEY, items[i]);
      if (LogicalSampler.selected(h, thresh)) {
        out.add(id(items[i]));
      }
    }
    return out;
  }

  private static long predicateChecksum(long[][] items) {
    LogicalSampler sampler = new LogicalSampler();
    long sum = 0;
    for (int i = 0; i < items.length; i++) {
      sum += sampler.hashKey(KEY, items[i]);
    }
    return sum;
  }

  private static List<String> arrivalIndexSelect(long[][] items, double p) {
    long thresh = LogicalSampler.threshold53(p);
    long state = 42L;
    List<String> out = new ArrayList<String>();
    for (int i = 0; i < items.length; i++) {
      state = state * 6364136223846793005L + 1442695040888963407L;
      if ((state >>> 11) < thresh) {
        out.add(id(items[i]));
      }
    }
    return out;
  }

  private static long[][] keys(int n) {
    long[][] items = new long[n][];
    for (int i = 0; i < n; i++) {
      items[i] = new long[] {i, 1000L * (i + 1), i * 3L};
    }
    return items;
  }

  private static String id(long[] k) {
    return k[0] + "," + k[1] + "," + k[2];
  }

  private static boolean sameSet(List<String> a, List<String> b) {
    String[] as = a.toArray(new String[a.size()]);
    String[] bs = b.toArray(new String[b.size()]);
    Arrays.sort(as);
    Arrays.sort(bs);
    return Arrays.equals(as, bs);
  }

  private static void check(boolean cond, String msg) {
    if (!cond) {
      throw new AssertionError(msg);
    }
  }
}
