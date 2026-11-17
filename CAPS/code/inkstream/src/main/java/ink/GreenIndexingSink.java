package ink;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import temporalindex.GenealogIndex;
import tieredindex.ShardGroup;
import tieredindex.WatermarkOrderer;

/**
 * Green temporal-query baseline: the counterpart of caps.IndexingSink / caps.QueryLengthSink
 * for the polynomial annotations of Green, Karvounarakis and Tannen.
 *
 * <pre>
 *   invoke(output)      WatermarkOrderer.add: ts &lt;= W late (counted, written to late_green_s&lt;i&gt;.csv),
 *                       otherwise Tier 1, the same watermark-driven red-black tree as CAPS and GeneaLog
 *   accept(watermark)   the Tier-1 prefix &lt;= W is appended, in timestamp order, to the ordered structure
 *   close()             final flush; the last subtask to close answers the benchmark queries
 * </pre>
 *
 * The ordered structure is the GeneaLog baseline's: a {@link GenealogIndex} (timestamp array with a sparse
 * append-only B-tree, one payload reference per output). The payload is the output's polynomial, which
 * is what a Green-style system has to keep to answer a how-much question later, because the
 * polynomial of an output has one variable per contributing source tuple.
 *
 * A temporal how-much query [from, to] finds the outputs in range with the B-tree (O(log N)) and projects
 * the polynomial of each of them per channel (h_pi, {@link InkMeta#addHowMuchTo}), summing the vectors:
 * O(log N + sum of |supp|) over the outputs in range, where |supp| is the number of distinct variables of
 * a polynomial. CAPS reads two prefix sums instead; GeneaLog traverses a graph per output. A versioning
 * query is two temporal queries and a subtraction.
 *
 * Every sink subtask owns one shard (Tier 1 plus ordered structure); the shards meet in {@link ShardGroup}
 * (the harness runs on one TaskManager JVM) and the last subtask to close answers the queries as the
 * channel-wise sum over the shards.
 *
 * Two benchmarks, chosen by the constructor (same seeds, windows and file formats as the CAPS sinks, so
 * the answers can be compared channel by channel):
 * <pre>
 *   fixed window    query_latency_green.csv, query_answers_green.csv        (numQueries how-much and versioning)
 *   window sweep    query_length_latency_green.csv, query_length_answers_green.csv
 * </pre>
 * and in both cases insert_latency_green.csv, index_stats_green.csv, tiered_stats_green.csv in benchDir.
 */
public class GreenIndexingSink<T> extends RichSinkFunction<T> implements Consumer<Watermark>, Serializable {

  private static final long serialVersionUID = 1L;

  private final InkQuerySink.TsFn<T> tsFn;
  private final InkQuerySink.MetaFn<T> metaFn;
  private final int channels;
  private final String benchDir;
  private final int numQueries;
  private final long[] queryLensMs; // window sweep; the fixed-window benchmark is the single length queryLensMs[0]
  private final boolean sweep;
  private final String token = UUID.randomUUID().toString();
  /** CAPS_INSERT_TIMING=1, read on the client: nanoTime around every Tier-1 insert. Off in every timed run. */
  private final boolean insertTiming = "1".equals(System.getenv("CAPS_INSERT_TIMING"));

  private transient Shard shard;
  /** Index and number of this subtask; set by open() from the runtime context, by tests directly. */
  transient int subtask;
  transient int parallelism;

  /** Fixed-window benchmark: numQueries how-much and numQueries versioning queries of length queryLenMs. */
  public GreenIndexingSink(InkQuerySink.TsFn<T> tsFn, InkQuerySink.MetaFn<T> metaFn, int channels,
                           String benchDir, int numQueries, long queryLenMs) {
    this(tsFn, metaFn, channels, benchDir, numQueries, new long[] {queryLenMs}, false);
  }

  /** Window-length sweep: numQueries how-much queries for each of the lengths. */
  public GreenIndexingSink(InkQuerySink.TsFn<T> tsFn, InkQuerySink.MetaFn<T> metaFn, int channels,
                           String benchDir, int numQueries, long[] queryLensMs) {
    this(tsFn, metaFn, channels, benchDir, numQueries, queryLensMs, true);
  }

  private GreenIndexingSink(InkQuerySink.TsFn<T> tsFn, InkQuerySink.MetaFn<T> metaFn, int channels,
                            String benchDir, int numQueries, long[] queryLensMs, boolean sweep) {
    this.tsFn = tsFn;
    this.metaFn = metaFn;
    this.channels = channels;
    this.benchDir = benchDir;
    this.numQueries = numQueries;
    this.queryLensMs = queryLensMs;
    this.sweep = sweep;
  }

  /** State of one sink subtask. */
  static final class Shard {
    final int subtask;
    final GenealogIndex<InkMeta> index = new GenealogIndex<>();
    final WatermarkOrderer<InkMeta> tier1 = new WatermarkOrderer<>();
    final WatermarkOrderer.Appender<InkMeta> appender = (ts, m) -> index.append(ts, m);
    long records;
    long watermarks;
    long maxWMinusTs = Long.MIN_VALUE;
    long minTs = Long.MAX_VALUE;
    long maxTs = Long.MIN_VALUE;
    PrintWriter late;

    Shard(int subtask) {
      this.subtask = subtask;
    }
  }

  // per-subtask path
  @Override
  public void open(Configuration parameters) {
    try {
      subtask = getRuntimeContext().getIndexOfThisSubtask();
      parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
    } catch (IllegalStateException e) { // outside a Flink task (unit tests)
      if (parallelism == 0)
        parallelism = 1;
    }
    shard = new Shard(subtask);
    shard.tier1.timing = insertTiming;
  }

  @Override
  public void invoke(T value, Context context) throws Exception {
    long ts = tsFn.ts(value);
    Shard s = shard;
    s.records++;
    long w = s.tier1.watermark();
    if (w != Long.MIN_VALUE && w - ts > s.maxWMinusTs)
      s.maxWMinusTs = w - ts;
    // Polynomials are immutable; copy() is a new wrapper around the same one (the annotation stays reachable).
    if (!s.tier1.add(ts, metaFn.meta(value).copy())) {
      if (s.late == null) {
        s.late = new PrintWriter(new FileWriter(benchDir + "/late_green_s" + subtask + ".csv"));
        s.late.println("ts,watermark");
      }
      s.late.println(ts + "," + w);
      return;
    }
    if (ts < s.minTs)
      s.minTs = ts;
    if (ts > s.maxTs)
      s.maxTs = ts;
  }

  /** Watermark callback, delivered by caps.WatermarkTriggeringSink. */
  @Override
  public void accept(Watermark mark) {
    shard.watermarks++;
    shard.tier1.advanceWatermark(mark.getTimestamp(), shard.appender);
  }

  @Override
  public void close() throws Exception {
    if (shard == null)
      return;
    shard.tier1.advanceWatermark(Long.MAX_VALUE, shard.appender); // end of input
    if (shard.late != null)
      shard.late.close();
    List<Shard> all = ShardGroup.arrive(token, parallelism, subtask, shard);
    if (all == null)
      return;
    long size = 0;
    for (Shard s : all)
      size += s.index.size();
    if (size == 0)
      throw new IllegalStateException("no records reached the green indexing sink");
    new Bench(all).run();
  }

  // benchmark, in the last subtask to close
  /** The assembled shards and the queries over them. */
  final class Bench {
    final List<Shard> shards;
    final long records;

    Bench(List<Shard> shards) {
      this.shards = shards;
      long n = 0;
      for (Shard s : shards)
        n += s.index.size();
      records = n;
    }

    /**
     * h_pi of the outputs with timestamp in [from, to]: locate the range in every shard's B-tree, then
     * project every polynomial in it per channel and add. Returns the number of outputs in range.
     */
    int howMuch(long from, long to, long[] sum) {
      int n = 0;
      for (Shard s : shards) {
        int end = s.index.upperBound(to);
        int begin = s.index.lowerBound(from);
        for (int i = begin; i < end; i++)
          s.index.get(i).addHowMuchTo(sum);
        n += Math.max(0, end - begin);
      }
      return n;
    }

    long minTs() {
      long m = Long.MAX_VALUE;
      for (Shard s : shards)
        m = Math.min(m, s.minTs);
      return m;
    }

    long maxTs() {
      long m = Long.MIN_VALUE;
      for (Shard s : shards)
        m = Math.max(m, s.maxTs);
      return m;
    }

    void run() throws IOException {
      writeStats();
      if (sweep)
        lengthSweep();
      else
        fixedWindow();
    }

    /** numQueries how-much and versioning queries; same seeds (42, 43, 44) and windows as caps.IndexingSink. */
    void fixedWindow() throws IOException {
      long lenMs = queryLensMs[0];
      long[][] howMuchQ = randomRanges(numQueries, minTs(), maxTs(), lenMs, new Random(42));
      long[][] versionW1 = randomRanges(numQueries, minTs(), maxTs(), lenMs, new Random(43));
      long[][] versionW2 = randomRanges(numQueries, minTs(), maxTs(), lenMs, new Random(44));

      // Warmup pass (JIT) that also keeps the answers for the verification against CAPS.
      long[][] hmAns = new long[numQueries][];
      long[][] ver1 = new long[numQueries][];
      long[][] ver2 = new long[numQueries][];
      int[] hmN = new int[numQueries], v1N = new int[numQueries], v2N = new int[numQueries];
      for (int i = 0; i < numQueries; i++) {
        hmAns[i] = new long[channels];
        hmN[i] = howMuch(howMuchQ[i][0], howMuchQ[i][1], hmAns[i]);
        ver1[i] = new long[channels];
        v1N[i] = howMuch(versionW1[i][0], versionW1[i][1], ver1[i]);
        ver2[i] = new long[channels];
        v2N[i] = howMuch(versionW2[i][0], versionW2[i][1], ver2[i]);
      }

      // Timed on windows the warm-up pass has not touched (seeds 142-144, as in caps.IndexingSink).
      long[][] timedQ = randomRanges(numQueries, minTs(), maxTs(), lenMs, new Random(142));
      long[][] timedW1 = randomRanges(numQueries, minTs(), maxTs(), lenMs, new Random(143));
      long[][] timedW2 = randomRanges(numQueries, minTs(), maxTs(), lenMs, new Random(144));
      long checksum = 0;
      long t0 = System.nanoTime();
      for (long[] q : timedQ) {
        long[] sum = new long[channels];
        howMuch(q[0], q[1], sum);
        checksum += sum[0];
      }
      long howMuchNanos = System.nanoTime() - t0;

      t0 = System.nanoTime();
      for (int i = 0; i < numQueries; i++) {
        long[] a = new long[channels];
        long[] b = new long[channels];
        howMuch(timedW1[i][0], timedW1[i][1], a);
        howMuch(timedW2[i][0], timedW2[i][1], b);
        checksum += b[0] - a[0];
      }
      long versionNanos = System.nanoTime() - t0;
      if (checksum == Long.MIN_VALUE)
        throw new IllegalStateException("unreachable"); // keep the timed loops live

      try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/query_latency_green.csv"))) {
        pw.println("method,query,records,queries,avg_us,total_ms");
        pw.printf(Locale.ROOT, "green,howmuch,%d,%d,%.3f,%.3f%n", records, numQueries,
            howMuchNanos / 1000.0 / numQueries, howMuchNanos / 1e6);
        pw.printf(Locale.ROOT, "green,versioning,%d,%d,%.3f,%.3f%n", records, numQueries,
            versionNanos / 1000.0 / numQueries, versionNanos / 1e6);
      }
      try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/query_answers_green.csv"))) {
        pw.println(answerHeader("query"));
        for (int i = 0; i < numQueries; i++) {
          pw.println(answerRow("howmuch", i, howMuchQ[i], hmAns[i], hmN[i]));
          pw.println(answerRow("versioning_w1", i, versionW1[i], ver1[i], v1N[i]));
          pw.println(answerRow("versioning_w2", i, versionW2[i], ver2[i], v2N[i]));
        }
      }
      System.out.println("GreenIndexingSink: " + records + " outputs indexed online in " + shards.size()
          + " shard(s), " + numQueries + " how-much + " + numQueries + " versioning queries benchmarked");
    }

    /** numQueries how-much queries per window length; fresh Random(42) per length as in caps.QueryLengthSink. */
    void lengthSweep() throws IOException {
      long minTs = minTs(), maxTs = maxTs();
      try (PrintWriter latency = new PrintWriter(new FileWriter(benchDir + "/query_length_latency_green.csv"));
           PrintWriter answers = new PrintWriter(new FileWriter(benchDir + "/query_length_answers_green.csv"))) {
        latency.println("method,len_ms,records,queries,avg_us");
        answers.println(answerHeader("len_ms"));
        for (long lenMs : queryLensMs) {
          long[][] queries = randomRanges(numQueries, minTs, maxTs, lenMs, new Random(42));
          for (int i = 0; i < numQueries; i++) { // warmup pass, answers for the verification
            long[] sum = new long[channels];
            int n = howMuch(queries[i][0], queries[i][1], sum);
            answers.println(answerRow(Long.toString(lenMs), i, queries[i], sum, n));
          }
          long checksum = 0;
          long t0 = System.nanoTime();
          for (long[] q : queries) {
            long[] sum = new long[channels];
            howMuch(q[0], q[1], sum);
            checksum += sum[0];
          }
          long nanos = System.nanoTime() - t0;
          if (checksum == Long.MIN_VALUE)
            throw new IllegalStateException("unreachable");
          latency.printf(Locale.ROOT, "green,%d,%d,%d,%.3f%n", lenMs, records, numQueries,
              nanos / 1000.0 / numQueries);
          System.out.println("GreenIndexingSink length sweep: len " + lenMs + " ms done ("
              + nanos / 1_000_000 + " ms total)");
        }
      }
    }

    /**
     * tiered_stats_green.csv (Tier 1, as for the other methods), insert_latency_green.csv (Tier-1 insert plus
     * split plus append per record) and index_stats_green.csv (the retained polynomials, 8 bytes per variable:
     * the same model as the memory experiment, so the two can be compared).
     */
    void writeStats() throws IOException {
      long insert = 0, migrate = 0, vars = 0, bytes = 0;
      try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/tiered_stats_green.csv"))) {
        pw.println("sink,shard,parallelism,records,indexed,late,migrated,tier1_peak,tier1_final,"
            + "insert_ns,migrate_ns,watermarks,max_w_minus_ts_ms,min_ts,max_ts");
        for (Shard s : shards) {
          WatermarkOrderer<InkMeta> t = s.tier1;
          pw.printf(Locale.ROOT, "green,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s,%d,%d%n",
              s.subtask, shards.size(), s.records, s.records - t.lateCount, t.lateCount,
              t.migratedCount, t.peakTier1, t.tier1Size(), t.insertNanos, t.migrationNanos,
              s.watermarks, s.maxWMinusTs == Long.MIN_VALUE ? "NA" : Long.toString(s.maxWMinusTs),
              s.minTs, s.maxTs);
          insert += t.insertNanos;
          migrate += t.migrationNanos;
          for (int i = 0; i < s.index.size(); i++) {
            InkMeta m = s.index.get(i);
            vars += m.totalVars();
            bytes += m.metadataBytes();
          }
        }
      }
      try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/insert_latency_green.csv"))) {
        pw.println("method,records,avg_ns,total_ms");
        pw.printf(Locale.ROOT, "green,%d,%.1f,%.3f%n", records, (double) (insert + migrate) / records,
            (insert + migrate) / 1e6);
      }
      try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/index_stats_green.csv"))) {
        pw.println("records,poly_vars,poly_bytes");
        pw.println(records + "," + vars + "," + bytes);
      }
    }
  }

  // helpers
  /** Same columns as the CAPS answer files: key, i, from, to, c0..c(M-1), defined, final. */
  private String answerHeader(String keyName) {
    StringBuilder h = new StringBuilder(keyName).append(",i,from,to");
    for (int c = 0; c < channels; c++)
      h.append(",c").append(c);
    return h.append(",defined,final").toString();
  }

  private static String answerRow(String key, int i, long[] q, long[] counts, int outputsInRange) {
    StringBuilder sb = new StringBuilder(key).append(',').append(i).append(',').append(q[0]).append(',').append(q[1]);
    for (long c : counts)
      sb.append(',').append(c);
    return sb.append(',').append(outputsInRange > 0 ? 1 : 0).append(",1").toString();
  }

  /** Fixed-length windows, uniformly random start, end = start + len (clamped to the stream span); as in the CAPS sinks. */
  static long[][] randomRanges(int n, long minTs, long maxTs, long lenMs, Random rnd) {
    long len = Math.min(lenMs, maxTs - minTs);
    long[][] ranges = new long[n][2];
    for (int i = 0; i < n; i++) {
      long start = minTs + (long) (rnd.nextDouble() * (maxTs - minTs - len));
      ranges[i][0] = start;
      ranges[i][1] = start + len;
    }
    return ranges;
  }
}
