package caps;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import temporalindex.ArrayStore;
import temporalindex.CapsIndex;

public final class Main {

  private Main() {}

  static final class Record {
    final long ts;
    final long[] counts;

    Record(long ts, long[] counts) {
      this.ts = ts;
      this.counts = counts;
    }
  }

  public static void main(String[] args) throws Exception {
    List<String> a = new ArrayList<>(Arrays.asList(args));
    if (a.contains("selftest")) {
      selfTest();
      return;
    }
    if (a.size() < 4) {
      usage();
    }
    String file = a.get(0);
    int channels = Integer.parseInt(a.get(1));
    int next = 2;
    int tsCol = 1;
    if (a.get(next).matches("\\d+") && isCommand(a, next + 1)) {
      tsCol = Integer.parseInt(a.get(next));
      next++;
    }
    String command = a.get(next++);

    List<Record> records = load(file, tsCol, channels);
    CapsIndex index = new CapsIndex(channels);
    for (Record r : records) {
      index.append(r.ts, r.counts);
    }
    System.err.println("loaded " + records.size() + " records from " + file
        + " (ts range " + records.get(0).ts + ".."
        + records.get(records.size() - 1).ts + ")");

    switch (command) {
      case "query": {
        long ts = Long.parseLong(a.get(next));
        long te = Long.parseLong(a.get(next + 1));
        print("HP[" + ts + "," + te + "]", index.howMuch(ts, te));
        print("rho", index.share(ts, te));
        break;
      }
      case "delta": {
        long ts1 = Long.parseLong(a.get(next));
        long te1 = Long.parseLong(a.get(next + 1));
        long ts2 = Long.parseLong(a.get(next + 2));
        long te2 = Long.parseLong(a.get(next + 3));
        print("HP[w1]", index.howMuch(ts1, te1));
        print("HP[w2]", index.howMuch(ts2, te2));
        print("deltaHP", index.deltaHowMuch(ts1, te1, ts2, te2));
        print("deltaRho", index.deltaShare(ts1, te1, ts2, te2));
        break;
      }
      case "bench":
        bench(records, channels, Integer.parseInt(a.get(next)));
        break;
      default:
        usage();
    }
  }

  private static boolean isCommand(List<String> a, int i) {
    if (i >= a.size()) {
      return false;
    }
    String s = a.get(i);
    return s.equals("query") || s.equals("delta") || s.equals("bench");
  }

  static List<Record> load(String file, int tsCol, int channels) throws Exception {
    List<Record> records = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = br.readLine()) != null) {
        String row = line.trim();
        if (row.isEmpty() || row.startsWith("---")) {
          continue;
        }
        String[] parts = row.split(",");
        long ts = Long.parseLong(parts[tsCol]);
        long[] counts = new long[channels];
        for (int c = 0; c < channels; c++) {
          counts[c] = Long.parseLong(parts[parts.length - channels + c]);
        }
        records.add(new Record(ts, counts));
      }
    }
    
    
    records.sort(Comparator.comparingLong(r -> r.ts));
    return records;
  }

  
  private static void bench(List<Record> records, int channels, int numQueries) {
    ArrayStore naive = new ArrayStore(channels);
    CapsIndex index = new CapsIndex(channels);
    for (Record r : records) {
      naive.append(r.ts, r.counts);
      index.append(r.ts, r.counts);
    }
    long minTs = records.get(0).ts;
    long maxTs = records.get(records.size() - 1).ts;

    long[][] queries = new long[numQueries][2];
    Random rnd = new Random(42);
    for (int i = 0; i < numQueries; i++) {
      long x = minTs + (long) (rnd.nextDouble() * (maxTs - minTs));
      long y = minTs + (long) (rnd.nextDouble() * (maxTs - minTs));
      queries[i][0] = Math.min(x, y);
      queries[i][1] = Math.max(x, y);
    }

    
    for (int i = 0; i < Math.min(numQueries, 1000); i++) {
      long[] expected = naive.howMuch(queries[i][0], queries[i][1]);
      double[] actual = index.howMuch(queries[i][0], queries[i][1]);
      for (int c = 0; c < channels; c++) {
        if (Math.round(actual[c]) != expected[c]) {
          throw new IllegalStateException("mismatch on query " + i + " channel " + c
              + ": naive=" + expected[c] + " index=" + actual[c]);
        }
      }
    }

    long checksum = 0;
    long naiveStart = System.nanoTime();
    for (long[] q : queries) {
      long[] hp = naive.howMuch(q[0], q[1]);
      checksum += hp[0];
    }
    long naiveNanos = System.nanoTime() - naiveStart;

    long indexStart = System.nanoTime();
    for (long[] q : queries) {
      double[] hp = index.howMuch(q[0], q[1]);
      checksum += (long) hp[0];
    }
    long indexNanos = System.nanoTime() - indexStart;

    System.err.println("checksum " + checksum + " (ignore; defeats dead-code elimination)");
    System.out.println("method,records,queries,avg_us,total_ms");
    System.out.printf(Locale.ROOT, "array,%d,%d,%.3f,%.3f%n", records.size(), numQueries,
        naiveNanos / 1000.0 / numQueries, naiveNanos / 1e6);
    System.out.printf(Locale.ROOT, "prefix,%d,%d,%.3f,%.3f%n", records.size(), numQueries,
        indexNanos / 1000.0 / numQueries, indexNanos / 1e6);
  }

  private static void print(String label, double[] values) {
    StringBuilder sb = new StringBuilder(label);
    for (double v : values) {
      sb.append(',');
      if (v == Math.rint(v)) {
        sb.append((long) v);
      } else {
        sb.append(String.format(Locale.ROOT, "%.4f", v));
      }
    }
    System.out.println(sb);
  }

  

  private static void selfTest() {
    selftestPrefixSum();
    selftestSummarize();
    expiryTest();
    versioningTest();
    sparseIndexTest();
    ringExpiryTest();
    System.out.println("selftest OK");
  }

  
  private static void selftestPrefixSum() {
    CapsIndex index = exampleIndex();
    double[] hp = index.howMuch(3, 5);
    check("prefixsum S1", 7, hp[0]);
    check("prefixsum S2", 5, hp[1]);
  }

  
  private static void selftestSummarize() {
    CapsIndex index = exampleIndex();
    index.summarizeBefore(4, 3);
    if (index.summaryBuckets() != 1) {
      throw new AssertionError("expected 1 summary bucket, got " + index.summaryBuckets());
    }
    double[] hp = index.howMuch(3, 5);
    check("summarize S1", 7, hp[0]);
    checkApprox("summarize S2", 6.3333, hp[1]);
  }

  
  private static void expiryTest() {
    CapsIndex index = exampleIndex();
    index.expireBefore(4);
    if (index.exactRecords() != 3) {
      throw new AssertionError("expected 3 exact records, got " + index.exactRecords());
    }
    double[] hp = index.howMuch(4, 6);
    check("expiry S1", 8, hp[0]);   
    check("expiry S2", 4, hp[1]);   
    boolean threw = false;
    try {
      index.howMuch(2, 5);
    } catch (IllegalStateException e) {
      threw = true;
    }
    if (!threw) {
      throw new AssertionError("query before horizon should throw");
    }
  }

  
  private static void versioningTest() {
    CapsIndex index = new CapsIndex(2);
    index.append(1, new long[] {2, 2});   
    index.append(3, new long[] {6, 2});   
    double[] dhp = index.deltaHowMuch(1, 2, 3, 4);
    check("versioning dHP S1", 4, dhp[0]);
    check("versioning dHP S2", 0, dhp[1]);
    double[] drho = index.deltaShare(1, 2, 3, 4);
    checkApprox("versioning dRho S1", 0.25, drho[0]);
    checkApprox("versioning dRho S2", -0.25, drho[1]);
  }

  
  private static void sparseIndexTest() {
    int n = CapsIndex.CAPACITY * CapsIndex.CAPACITY * 3 + 17;
    CapsIndex index = new CapsIndex(1);
    ArrayStore naive = new ArrayStore(1);
    Random rnd = new Random(7);
    long t = 0;
    for (int i = 0; i < n; i++) {
      t += rnd.nextInt(3); 
      long[] c = new long[] {rnd.nextInt(5)};
      index.append(t, c);
      naive.append(t, c);
    }
    Random qs = new Random(8);
    for (int i = 0; i < 500; i++) {
      long x = (long) (qs.nextDouble() * t);
      long y = (long) (qs.nextDouble() * t);
      long from = Math.min(x, y);
      long to = Math.max(x, y);
      check("sparse q" + i, naive.howMuch(from, to)[0], index.howMuch(from, to)[0]);
    }
  }

  
  private static void ringExpiryTest() {
    int chunk = CapsIndex.CAPACITY * (CapsIndex.CAPACITY / 4 + 3);
    CapsIndex index = new CapsIndex(1);
    ArrayStore full = new ArrayStore(1);
    Random rnd = new Random(11);
    Random qs = new Random(12);
    long t = 0;
    long horizon = 0;
    for (int round = 0; round < 6; round++) {
      for (int i = 0; i < chunk; i++) {
        t += rnd.nextInt(3);
        long[] c = new long[] {rnd.nextInt(5)};
        index.append(t, c);
        full.append(t, c);
      }
      horizon += (t - horizon) / 2;
      index.expireBefore(horizon);
      for (int i = 0; i < 200; i++) {
        long x = horizon + (long) (qs.nextDouble() * (t - horizon));
        long y = horizon + (long) (qs.nextDouble() * (t - horizon));
        long from = Math.min(x, y);
        long to = Math.max(x, y);
        check("ring r" + round + " q" + i,
            full.howMuch(from, to)[0], index.howMuch(from, to)[0]);
      }
    }
  }

  private static CapsIndex exampleIndex() {
    
    
    
    
    CapsIndex index = new CapsIndex(2);
    index.append(1, new long[] {1, 4});
    index.append(2, new long[] {1, 4});
    index.append(3, new long[] {1, 2});
    index.append(4, new long[] {3, 3});
    index.append(5, new long[] {3, 0});
    index.append(6, new long[] {2, 1});
    return index;
  }

  private static void check(String label, double expected, double actual) {
    if (Math.abs(expected - actual) > 1e-9) {
      throw new AssertionError(label + ": expected " + expected + ", got " + actual);
    }
  }

  private static void checkApprox(String label, double expected, double actual) {
    if (Math.abs(expected - actual) > 1e-3) {
      throw new AssertionError(label + ": expected ~" + expected + ", got " + actual);
    }
  }

  private static void usage() {
    System.err.println("usage: Main <sinkFile> <channels> [tsCol=1] query <ts> <te>");
    System.err.println("       Main <sinkFile> <channels> [tsCol=1] delta <ts1> <te1> <ts2> <te2>");
    System.err.println("       Main <sinkFile> <channels> [tsCol=1] bench <numQueries>");
    System.err.println("       Main selftest");
    System.exit(1);
  }
}
