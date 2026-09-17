package caps;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import temporalindex.ArrayStore;
import temporalindex.CapsIndex;

// Online Section-5 sink: builds {@link CapsIndex} live, reorders by slackMs, benchmarks at close.
// slackMs must cover max output disorder (one window for tumbling); too small makes append throw.
// Writes query_latency.csv, insert_latency.csv, index_stats.csv under benchDir.
public class IndexingSink<T> extends RichSinkFunction<T> {

  /** Extracts the event timestamp of a tuple. */
  public interface TsFn<T> extends Serializable {
    long ts(T tuple);
  }

  /** Extracts the how-much meta-data of a tuple. */
  public interface HmFn<T> extends Serializable {
    HowMuch hm(T tuple);
  }

  private static final long serialVersionUID = 1L;

  private final TsFn<T> tsFn;
  private final HmFn<T> hmFn;
  private final int channels;
  private final String benchDir;
  private final int numQueries;
  private final long slackMs;
  private final long queryLenMs;

  private transient CapsIndex index;
  private transient ArrayStore array;
  private transient PriorityQueue<Entry> reorder;
  private transient long maxSeenTs;
  private transient long indexInsertNanos;
  private transient long arrayInsertNanos;

  public IndexingSink(TsFn<T> tsFn, HmFn<T> hmFn, int channels,
                      String benchDir, int numQueries, long slackMs,
                      long queryLenMs) {
    this.tsFn = tsFn;
    this.hmFn = hmFn;
    this.channels = channels;
    this.benchDir = benchDir;
    this.numQueries = numQueries;
    this.slackMs = slackMs;
    this.queryLenMs = queryLenMs;
  }

  @Override
  public void open(Configuration parameters) {
    index = new CapsIndex(channels);
    array = new ArrayStore(channels);
    reorder = new PriorityQueue<>();
    maxSeenTs = Long.MIN_VALUE;
  }

  @Override
  public void invoke(T value, Context context) {
    // Clone counts becasue Flink object reuse would stomp the queued entry.
    long ts = tsFn.ts(value);
    reorder.add(new Entry(ts, hmFn.hm(value).counts.clone()));
    maxSeenTs = Math.max(maxSeenTs, ts);
    while (!reorder.isEmpty() && reorder.peek().ts <= maxSeenTs - slackMs) {
      release(reorder.poll());
    }
  }

  @Override
  public void close() throws Exception {
    while (!reorder.isEmpty()) {
      release(reorder.poll());
    }
    if (index.exactRecords() == 0) {
      throw new IllegalStateException("no records reached the indexing sink");
    }
    bench();
  }

  private void release(Entry e) {
    long t0 = System.nanoTime();
    index.append(e.ts, e.counts);
    long t1 = System.nanoTime();
    array.append(e.ts, e.counts);
    long t2 = System.nanoTime();
    indexInsertNanos += t1 - t0;
    arrayInsertNanos += t2 - t1;
  }

  /**
   * Query-latency experiment on the live, online-built structures: numQueries
   * temporal how-much queries (paper 5.1) and numQueries versioning queries
   * (paper Def. versioning-abs: deltaHP between two random windows), each
   * answered by both methods and timed separately. A versioning query on the
   * array store is two scans plus a channel-wise subtraction. Every query
   * window has the same fixed length (queryLenMs, ten window slides of the
   * dataflow) and a uniformly random start.
   */
  private void bench() throws Exception {
    long minTs = index.firstTs();
    long maxTs = maxSeenTs;
    // Seeds 42/43/44 give three independent query sets in one run.
    long[][] howMuchQ = randomRanges(numQueries, minTs, maxTs, queryLenMs, new Random(42));
    long[][] versionW1 = randomRanges(numQueries, minTs, maxTs, queryLenMs, new Random(43));
    long[][] versionW2 = randomRanges(numQueries, minTs, maxTs, queryLenMs, new Random(44));

    // Correctness cross-checks plus JIT warmup: array scan and prefix index
    // must agree on every query. Answers are kept and written to
    // query_answers.csv so verify.py can compare them with GeneaLog's.
    long[][] howMuchAns = new long[numQueries][];
    long[][] versionAns1 = new long[numQueries][];
    long[][] versionAns2 = new long[numQueries][];
    for (int i = 0; i < numQueries; i++) {
      long[] expected = array.howMuch(howMuchQ[i][0], howMuchQ[i][1]);
      double[] actual = index.howMuch(howMuchQ[i][0], howMuchQ[i][1]);
      for (int c = 0; c < channels; c++) {
        if (Math.round(actual[c]) != expected[c]) {
          throw new IllegalStateException("array/prefix mismatch on query " + i
              + " channel " + c + ": " + expected[c] + " vs " + actual[c]);
        }
      }
      howMuchAns[i] = expected;
      long[] a1 = array.howMuch(versionW1[i][0], versionW1[i][1]);
      long[] a2 = array.howMuch(versionW2[i][0], versionW2[i][1]);
      double[] actualDelta = index.deltaHowMuch(
          versionW1[i][0], versionW1[i][1], versionW2[i][0], versionW2[i][1]);
      for (int c = 0; c < channels; c++) {
        if (Math.round(actualDelta[c]) != a2[c] - a1[c]) {
          throw new IllegalStateException("array/prefix mismatch on versioning query "
              + i + " channel " + c + ": " + (a2[c] - a1[c]) + " vs " + actualDelta[c]);
        }
      }
      versionAns1[i] = a1;
      versionAns2[i] = a2;
    }

    // Touch answers so the JIT cannot erase the timed loops.
    long checksum = 0;

    long arrayStart = System.nanoTime();
    for (long[] q : howMuchQ) {
      checksum += array.howMuch(q[0], q[1])[0];
    }
    long arrayHowMuchNanos = System.nanoTime() - arrayStart;

    long indexStart = System.nanoTime();
    for (long[] q : howMuchQ) {
      checksum += (long) index.howMuch(q[0], q[1])[0];
    }
    long indexHowMuchNanos = System.nanoTime() - indexStart;

    long arrayVersionStart = System.nanoTime();
    for (int i = 0; i < numQueries; i++) {
      checksum += arrayDelta(versionW1[i], versionW2[i])[0];
    }
    long arrayVersionNanos = System.nanoTime() - arrayVersionStart;

    long indexVersionStart = System.nanoTime();
    for (int i = 0; i < numQueries; i++) {
      checksum += (long) index.deltaHowMuch(
          versionW1[i][0], versionW1[i][1], versionW2[i][0], versionW2[i][1])[0];
    }
    long indexVersionNanos = System.nanoTime() - indexVersionStart;

    int records = index.exactRecords();
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/query_latency.csv"))) {
      pw.println("method,query,records,queries,avg_us,total_ms");
      writeRow(pw, "array", "howmuch", records, arrayHowMuchNanos);
      writeRow(pw, "prefix", "howmuch", records, indexHowMuchNanos);
      writeRow(pw, "array", "versioning", records, arrayVersionNanos);
      writeRow(pw, "prefix", "versioning", records, indexVersionNanos);
    }
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/insert_latency.csv"))) {
      pw.println("method,records,avg_ns,total_ms");
      pw.printf(Locale.ROOT, "array,%d,%.1f,%.3f%n", records,
          (double) arrayInsertNanos / records, arrayInsertNanos / 1e6);
      pw.printf(Locale.ROOT, "prefix,%d,%.1f,%.3f%n", records,
          (double) indexInsertNanos / records, indexInsertNanos / 1e6);
    }
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/index_stats.csv"))) {
      pw.println("records,index_bytes");
      pw.println(records + "," + index.memoryBytes());
    }
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/query_answers.csv"))) {
      StringBuilder header = new StringBuilder("query,i,from,to");
      for (int c = 0; c < channels; c++) {
        header.append(",c").append(c);
      }
      pw.println(header);
      for (int i = 0; i < numQueries; i++) {
        writeAnswer(pw, "howmuch", i, howMuchQ[i], howMuchAns[i]);
        writeAnswer(pw, "versioning_w1", i, versionW1[i], versionAns1[i]);
        writeAnswer(pw, "versioning_w2", i, versionW2[i], versionAns2[i]);
      }
    }
    System.out.println("IndexingSink: " + records + " records indexed online, "
        + numQueries + " how-much + " + numQueries
        + " versioning queries benchmarked (checksum " + checksum + ")");
  }

  /** Fixed-length windows: uniformly random start, end = start + lenMs
   * (clamped to the stream span if lenMs exceeds it). */
  private static long[][] randomRanges(int n, long minTs, long maxTs, long lenMs, Random rnd) {
    long len = Math.min(lenMs, maxTs - minTs);
    long[][] ranges = new long[n][2];
    for (int i = 0; i < n; i++) {
      long start = minTs + (long) (rnd.nextDouble() * (maxTs - minTs - len));
      ranges[i][0] = start;
      ranges[i][1] = start + len;
    }
    return ranges;
  }

  /** Versioning on the array store: scan both windows, subtract channel-wise. */
  private long[] arrayDelta(long[] w1, long[] w2) {
    long[] a = array.howMuch(w1[0], w1[1]);
    long[] b = array.howMuch(w2[0], w2[1]);
    long[] delta = new long[channels];
    for (int c = 0; c < channels; c++) {
      delta[c] = b[c] - a[c];
    }
    return delta;
  }

  private void writeRow(PrintWriter pw, String method, String query, int records, long nanos) {
    pw.printf(Locale.ROOT, "%s,%s,%d,%d,%.3f,%.3f%n", method, query, records, numQueries,
        nanos / 1000.0 / numQueries, nanos / 1e6);
  }

  private static void writeAnswer(PrintWriter pw, String type, int i, long[] q, long[] answer) {
    StringBuilder sb = new StringBuilder();
    sb.append(type).append(',').append(i).append(',').append(q[0]).append(',').append(q[1]);
    for (long a : answer) {
      sb.append(',').append(a);
    }
    pw.println(sb);
  }

  private static final class Entry implements Comparable<Entry> {
    final long ts;
    final long[] counts;

    Entry(long ts, long[] counts) {
      this.ts = ts;
      this.counts = counts;
    }

    @Override
    public int compareTo(Entry other) {
      return Long.compare(ts, other.ts);
    }
  }
}
