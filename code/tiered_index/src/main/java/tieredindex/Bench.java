package tieredindex;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

// Microbenchmark of the Tier-1 candidates (AugmentedRBTree, SortedBufferTier1, HybridTier1).
//   insert   : ns per insert while occupancy grows from n to ~1.1 n (fresh structure per repetition)
//   query    : ns per provisional rangeSum over random ranges covering a fixed fraction of the key span
//   split    : ns per extracted record of Tier1.splitPrefix, extracting a fraction of the n records
//   migrate  : the same through TwoTierIndex.advanceWatermark (adds the Tier-2 append per record)
// Workloads: "uniform" (every ts uniform in the key span: worst case for the sorted buffer) and
// "nearsorted" (ts = 4 i +- 256: arrivals within ~64 positions of the end, the bounded-disorder case).
// Warm-up first, then several repetitions; the median is reported.
// A third workload, "sorted" (ts = 4 i: strictly ordered arrivals), is selected with the third argument (a comma-separated
// list of workloads; default uniform,nearsorted).
// Usage: java -cp target/classes:<temporal-index.jar> tieredindex.Bench [out.csv] [maxN] [workloads]
public class Bench {

  interface Factory {
    Tier1 make(int m);
  }

  static final int[] NS = {100, 300, 1000, 3000, 10_000, 30_000, 100_000, 300_000, 1_000_000};
  static final int[] CHANNELS = {2, 4};
  static String[] WORKLOADS = {"uniform", "nearsorted"};
  static final double[] QUERY_FRACS = {0.01, 0.1, 0.5};
  static final double[] SPLIT_FRACS = {0.5, 0.01};
  static final int JITTER = 256;

  static long sink; // keeps the JIT from discarding results

  static int reps(int n) {
    return n <= 10_000 ? 21 : n <= 100_000 ? 9 : n <= 300_000 ? 7 : 5;
  }

  /** Timestamps in generation (arrival) order. */
  static long[] genTs(int n, String wl, Random rnd) {
    long[] ts = new long[n];
    for (int i = 0; i < n; i++)
      ts[i] = nextTs(wl, n, i, rnd);
    return ts;
  }

  static long nextTs(String wl, int n, int i, Random rnd) {
    if (wl.equals("uniform"))
      return rnd.nextInt(4 * n);
    if (wl.equals("sorted"))
      return 4L * i; // strictly ordered arrivals: what a per-subtask sink shard receives
    return 4L * i + rnd.nextInt(2 * JITTER + 1) - JITTER;
  }

  static Tier1 build(Factory f, int m, long[] ts, String wl) {
    Tier1 t = f.make(m);
    long[] v = new long[m];
    Arrays.fill(v, 3);
    long[] order = ts;
    // The sorted buffer reaches the same state from a sorted load without the O(n^2) build.
    if (wl.equals("uniform") && t instanceof SortedBufferTier1) {
      order = ts.clone();
      Arrays.sort(order);
    }
    for (long x : order)
      t.insert(x, v);
    return t;
  }

  static double median(double[] a) {
    double[] b = a.clone();
    Arrays.sort(b);
    return b[b.length / 2];
  }

  static final class Row {
    String impl, wl, metric;
    int m, n;
    double param, ns;
  }

  static void runConfig(String impl, Factory f, int m, String wl, int n, List<Row> out) {
    int reps = reps(n);
    int ops = Math.max(10, Math.min(n / 10, 5000));
    int nq = n <= 10_000 ? 2000 : n <= 100_000 ? 500 : 100;
    long span = 4L * n;
    Random rnd = new Random(12345L + n * 31L + m);
    double[] ins = new double[reps];
    double[][] qry = new double[QUERY_FRACS.length][reps];
    double[][] spl = new double[SPLIT_FRACS.length][reps];
    double[][] mig = new double[SPLIT_FRACS.length][reps];
    long[] v = new long[m];
    Arrays.fill(v, 1);
    long[] acc = new long[m];

    for (int r = 0; r < reps; r++) {
      long[] ts = genTs(n, wl, rnd);
      Tier1 t = build(f, m, ts, wl);
      // insert: occupancy n -> n + ops
      long[] extra = new long[ops];
      for (int i = 0; i < ops; i++)
        extra[i] = nextTs(wl, n, n + i, rnd);
      System.gc();
      long t0 = System.nanoTime();
      for (int i = 0; i < ops; i++)
        t.insert(extra[i], v);
      ins[r] = (System.nanoTime() - t0) / (double) ops;
      // provisional range sums on the same structure
      for (int qi = 0; qi < QUERY_FRACS.length; qi++) {
        long width = (long) (QUERY_FRACS[qi] * span);
        long[] lo = new long[nq];
        for (int i = 0; i < nq; i++)
          lo[i] = (long) (rnd.nextDouble() * (span - width));
        t0 = System.nanoTime();
        for (int i = 0; i < nq; i++) {
          Arrays.fill(acc, 0);
          sink += t.rangeSum(lo[i], lo[i] + width, acc) + acc[0];
        }
        qry[qi][r] = (System.nanoTime() - t0) / (double) nq;
      }
      // split / migrate: destructive, so a fresh structure each
      for (int si = 0; si < SPLIT_FRACS.length; si++) {
        long w = (long) (SPLIT_FRACS[si] * span);
        Tier1 a = build(f, m, ts, wl);
        System.gc();
        t0 = System.nanoTime();
        Tier1.Entries e = a.splitPrefix(w);
        long dt = System.nanoTime() - t0;
        sink += e.n;
        spl[si][r] = dt / (double) Math.max(1, e.n);
        TwoTierIndex idx = new TwoTierIndex(m, build(f, m, ts, wl));
        idx.timing = false;
        System.gc();
        t0 = System.nanoTime();
        idx.advanceWatermark(w);
        dt = System.nanoTime() - t0;
        sink += idx.migratedCount;
        mig[si][r] = dt / (double) Math.max(1, idx.migratedCount);
      }
    }
    add(out, impl, m, wl, n, "insert", 0, median(ins));
    for (int qi = 0; qi < QUERY_FRACS.length; qi++)
      add(out, impl, m, wl, n, "query", QUERY_FRACS[qi], median(qry[qi]));
    for (int si = 0; si < SPLIT_FRACS.length; si++) {
      add(out, impl, m, wl, n, "split", SPLIT_FRACS[si], median(spl[si]));
      add(out, impl, m, wl, n, "migrate", SPLIT_FRACS[si], median(mig[si]));
    }
  }

  static void add(List<Row> out, String impl, int m, String wl, int n, String metric,
      double param, double ns) {
    Row r = new Row();
    r.impl = impl;
    r.m = m;
    r.wl = wl;
    r.n = n;
    r.metric = metric;
    r.param = param;
    r.ns = ns;
    out.add(r);
  }

  static double find(List<Row> rows, String impl, int m, String wl, int n, String metric,
      double param) {
    for (Row r : rows)
      if (r.impl.equals(impl) && r.m == m && r.wl.equals(wl) && r.n == n && r.metric.equals(metric)
          && r.param == param)
        return r.ns;
    return Double.NaN;
  }

  /** Occupancy where the tree becomes faster than the buffer: log-log interpolation of the ratio. */
  static String crossover(List<Row> rows, int m, String wl, String metric, double param,
      int maxN) {
    double prevN = 0;
    double prevLogRatio = 0;
    boolean first = true;
    for (int n : NS) {
      if (n > maxN)
        break;
      double b = find(rows, "SortedBuffer", m, wl, n, metric, param);
      double t = find(rows, "RBTree", m, wl, n, metric, param);
      double lr = Math.log(b / t); // > 0: tree faster
      if (first && lr > 0)
        return "tree faster from n=" + n + " (smallest measured)";
      if (!first && prevLogRatio <= 0 && lr > 0) {
        double x = Math.log(prevN) + (Math.log(n) - Math.log(prevN)) * (-prevLogRatio)
            / (lr - prevLogRatio);
        return String.format("~%.0f", Math.exp(x));
      }
      if (!first && prevLogRatio > 0 && lr <= 0)
        return "tree faster until n=" + (int) prevN + ", then buffer";
      first = false;
      prevN = n;
      prevLogRatio = lr;
    }
    return prevLogRatio > 0 ? "tree always faster" : "buffer always faster (up to n=" + maxN + ")";
  }

  public static void main(String[] args) throws Exception {
    String file = args.length > 0 ? args[0] : "bench_results.csv";
    int maxN = args.length > 1 ? Integer.parseInt(args[1]) : 1_000_000;
    if (args.length > 2)
      WORKLOADS = args[2].split(",");
    String[] impls = {"RBTree", "SortedBuffer", "Hybrid"};
    Factory[] facs = {AugmentedRBTree::new, SortedBufferTier1::new, HybridTier1::new};

    System.err.println("warm-up");
    List<Row> junk = new ArrayList<>();
    for (int pass = 0; pass < 2; pass++)
      for (int i = 0; i < impls.length; i++)
        for (int m : CHANNELS)
          for (String wl : WORKLOADS)
            for (int n : new int[] {3000, 30_000})
              runConfig(impls[i], facs[i], m, wl, n, junk);

    List<Row> rows = new ArrayList<>();
    for (int n : NS) {
      if (n > maxN)
        break;
      for (int i = 0; i < impls.length; i++)
        for (int m : CHANNELS)
          for (String wl : WORKLOADS) {
            runConfig(impls[i], facs[i], m, wl, n, rows);
            System.err.println("done " + impls[i] + " M=" + m + " " + wl + " n=" + n);
          }
    }
    try (PrintWriter w = new PrintWriter(file)) {
      w.println("impl,M,workload,n,metric,param,ns_per_op");
      for (Row r : rows)
        w.println(r.impl + "," + r.m + "," + r.wl + "," + r.n + "," + r.metric + "," + r.param
            + "," + String.format("%.1f", r.ns));
    }
    try (PrintWriter w = new PrintWriter(file.replace(".csv", "_crossover.csv"))) {
      w.println("M,workload,metric,param,crossover_n");
      for (int m : CHANNELS)
        for (String wl : WORKLOADS) {
          w.println(m + "," + wl + ",insert,0.0," + crossover(rows, m, wl, "insert", 0, maxN));
          for (double q : QUERY_FRACS)
            w.println(m + "," + wl + ",query," + q + "," + crossover(rows, m, wl, "query", q, maxN));
          for (double s : SPLIT_FRACS) {
            w.println(m + "," + wl + ",split," + s + "," + crossover(rows, m, wl, "split", s, maxN));
            w.println(m + "," + wl + ",migrate," + s + "," + crossover(rows, m, wl, "migrate", s, maxN));
          }
        }
    }
    System.err.println("sink " + sink);
  }
}
