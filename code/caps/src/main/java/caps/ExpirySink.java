package caps;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import temporalindex.CapsIndex;

public class ExpirySink<T> extends RichSinkFunction<T> {

  private static final long serialVersionUID = 1L;

  public static final long SAMPLE_EVERY_MS = 6 * 60 * 60 * 1000L;

  public static final int CHECK_QUERIES = 1000;

  private final IndexingSink.TsFn<T> tsFn;
  private final IndexingSink.HmFn<T> hmFn;
  private final int channels;
  private final String benchDir;
  private final long horizonMs;
  private final long slackMs;
  private final long queryLenMs;

  private transient CapsIndex full;
  private transient CapsIndex expiring;
  private transient PriorityQueue<Entry> reorder;
  private transient long maxSeenTs;
  private transient long recordsTotal;
  private transient long lastSampleTs;
  private transient List<long[]> samples;
  private transient long expiryCalls;
  private transient long expiryNanos;

  public ExpirySink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn, int channels,
                    String benchDir, long horizonMs, long slackMs, long queryLenMs) {
    this.tsFn = tsFn;
    this.hmFn = hmFn;
    this.channels = channels;
    this.benchDir = benchDir;
    this.horizonMs = horizonMs;
    this.slackMs = slackMs;
    this.queryLenMs = queryLenMs;
  }

  @Override
  public void open(Configuration parameters) {
    full = new CapsIndex(channels);
    expiring = new CapsIndex(channels);
    reorder = new PriorityQueue<>();
    maxSeenTs = Long.MIN_VALUE;
    lastSampleTs = Long.MIN_VALUE;
    samples = new ArrayList<>();
  }

  @Override
  public void invoke(T value, Context context) {
    long ts = tsFn.ts(value);
    reorder.add(new Entry(ts, hmFn.hm(value).counts.clone()));
    maxSeenTs = Math.max(maxSeenTs, ts);
    while (!reorder.isEmpty() && reorder.peek().ts <= maxSeenTs - slackMs) {
      release(reorder.poll());
    }
  }

  private void release(Entry e) {
    full.append(e.ts, e.counts);
    expiring.append(e.ts, e.counts);
    recordsTotal++;
    if (lastSampleTs == Long.MIN_VALUE || e.ts - lastSampleTs >= SAMPLE_EVERY_MS) {
      expire(e.ts - horizonMs);
      samples.add(new long[] {e.ts, full.memoryBytes(), expiring.memoryBytes()});
      lastSampleTs = e.ts;
    }
  }

  @Override
  public void close() throws Exception {
    while (!reorder.isEmpty()) {
      release(reorder.poll());
    }
    if (recordsTotal == 0) {
      throw new IllegalStateException("no records reached the expiry sink");
    }
    long cutoff = maxSeenTs - horizonMs;
    expire(cutoff);
    samples.add(new long[] {maxSeenTs, full.memoryBytes(), expiring.memoryBytes()});

    Random rnd = new Random(42);
    long len = Math.min(queryLenMs, maxSeenTs - cutoff);
    long[] qFrom = new long[CHECK_QUERIES];
    long[] qTo = new long[CHECK_QUERIES];
    for (int i = 0; i < CHECK_QUERIES; i++) {
      long start = cutoff + (long) (rnd.nextDouble() * (maxSeenTs - cutoff - len));
      qFrom[i] = start;
      qTo[i] = start + len;
    }
    int mismatches = 0;
    for (int i = 0; i < CHECK_QUERIES; i++) {
      double[] expected = full.howMuch(qFrom[i], qTo[i]);
      double[] actual = expiring.howMuch(qFrom[i], qTo[i]);
      for (int c = 0; c < channels; c++) {
        if (Math.round(expected[c]) != Math.round(actual[c])) {
          mismatches++;
        }
      }
    }
    if (mismatches > 0) {
      throw new IllegalStateException(
          mismatches + " query mismatches between full and expiring index");
    }
    long fullQueryNanos = 0;
    long expiringQueryNanos = 0;
    for (int i = 0; i < CHECK_QUERIES; i++) {
      long t0 = System.nanoTime();
      full.howMuch(qFrom[i], qTo[i]);
      long t1 = System.nanoTime();
      expiring.howMuch(qFrom[i], qTo[i]);
      long t2 = System.nanoTime();
      fullQueryNanos += t1 - t0;
      expiringQueryNanos += t2 - t1;
    }

    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/expiry_memory.csv"))) {
      pw.println("event_ts,full_bytes,expiring_bytes");
      for (long[] s : samples) {
        pw.println(s[0] + "," + s[1] + "," + s[2]);
      }
    }
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/expiry_stats.csv"))) {
      pw.println("horizon_ms,records_total,records_retained,full_bytes,"
          + "expiring_bytes,queries_checked,mismatches,expiry_calls,avg_expiry_us,"
          + "full_query_us,expiring_query_us");
      pw.printf(java.util.Locale.ROOT, "%d,%d,%d,%d,%d,%d,%d,%d,%.3f,%.3f,%.3f%n",
          horizonMs, recordsTotal, expiring.exactRecords(),
          full.memoryBytes(), expiring.memoryBytes(),
          CHECK_QUERIES, mismatches, expiryCalls,
          expiryNanos / 1000.0 / Math.max(1, expiryCalls),
          fullQueryNanos / 1000.0 / CHECK_QUERIES,
          expiringQueryNanos / 1000.0 / CHECK_QUERIES);
    }
    System.out.println("ExpirySink: " + recordsTotal + " records, "
        + expiring.exactRecords() + " retained after expiry (horizon "
        + horizonMs + " ms), " + CHECK_QUERIES + " in-horizon queries exact, "
        + expiryCalls + " expiry calls avg "
        + String.format(java.util.Locale.ROOT, "%.1f",
            expiryNanos / 1000.0 / Math.max(1, expiryCalls)) + " us");
  }

  private void expire(long cutoff) {
    long t0 = System.nanoTime();
    expiring.expireBefore(cutoff);
    expiryNanos += System.nanoTime() - t0;
    expiryCalls++;
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
