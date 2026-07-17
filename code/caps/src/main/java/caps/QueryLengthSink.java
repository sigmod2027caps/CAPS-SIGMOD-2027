package caps;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import temporalindex.ArrayStore;
import temporalindex.CapsIndex;

public class QueryLengthSink<T> extends RichSinkFunction<T> {

  private static final long serialVersionUID = 1L;

  private final IndexingSink.TsFn<T> tsFn;
  private final IndexingSink.HmFn<T> hmFn;
  private final int channels;
  private final String benchDir;
  private final int numQueries;
  private final long slackMs;
  private final long[] queryLensMs;

  private transient CapsIndex index;
  private transient ArrayStore array;
  private transient PriorityQueue<Entry> reorder;
  private transient long maxSeenTs;

  public QueryLengthSink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn, int channels,
                         String benchDir, int numQueries, long slackMs, long[] queryLensMs) {
    this.tsFn = tsFn;
    this.hmFn = hmFn;
    this.channels = channels;
    this.benchDir = benchDir;
    this.numQueries = numQueries;
    this.slackMs = slackMs;
    this.queryLensMs = queryLensMs;
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
    
    long ts = tsFn.ts(value);
    reorder.add(new Entry(ts, hmFn.hm(value).counts.clone()));
    maxSeenTs = Math.max(maxSeenTs, ts);
    while (!reorder.isEmpty() && reorder.peek().ts <= maxSeenTs - slackMs) {
      Entry e = reorder.poll();
      index.append(e.ts, e.counts);
      array.append(e.ts, e.counts);
    }
  }

  @Override
  public void close() throws Exception {
    while (!reorder.isEmpty()) {
      Entry e = reorder.poll();
      index.append(e.ts, e.counts);
      array.append(e.ts, e.counts);
    }
    if (index.exactRecords() == 0) {
      throw new IllegalStateException("no records reached the query-length sink");
    }
    bench();
  }

  private void bench() throws Exception {
    long minTs = index.firstTs();
    long maxTs = maxSeenTs;
    int records = index.exactRecords();

    try (PrintWriter latency = new PrintWriter(
            new FileWriter(benchDir + "/query_length_latency.csv"));
         PrintWriter answers = new PrintWriter(
            new FileWriter(benchDir + "/query_length_answers.csv"))) {
      latency.println("method,len_ms,records,queries,avg_us");
      StringBuilder header = new StringBuilder("len_ms,i,from,to");
      for (int c = 0; c < channels; c++) {
        header.append(",c").append(c);
      }
      answers.println(header);

      for (long lenMs : queryLensMs) {
        long[][] queries = randomRanges(numQueries, minTs, maxTs, lenMs, new Random(42));

        
        for (int i = 0; i < numQueries; i++) {
          long[] expected = array.howMuch(queries[i][0], queries[i][1]);
          double[] actual = index.howMuch(queries[i][0], queries[i][1]);
          StringBuilder row = new StringBuilder();
          row.append(lenMs).append(',').append(i).append(',')
              .append(queries[i][0]).append(',').append(queries[i][1]);
          for (int c = 0; c < channels; c++) {
            if (Math.round(actual[c]) != expected[c]) {
              throw new IllegalStateException("array/prefix mismatch on len " + lenMs
                  + " query " + i + " channel " + c + ": "
                  + expected[c] + " vs " + actual[c]);
            }
            row.append(',').append(expected[c]);
          }
          answers.println(row);
        }

        long checksum = 0;

        long arrayStart = System.nanoTime();
        for (long[] q : queries) {
          checksum += array.howMuch(q[0], q[1])[0];
        }
        long arrayNanos = System.nanoTime() - arrayStart;

        long indexStart = System.nanoTime();
        for (long[] q : queries) {
          checksum += (long) index.howMuch(q[0], q[1])[0];
        }
        long indexNanos = System.nanoTime() - indexStart;

        if (checksum == Long.MIN_VALUE) {
          throw new IllegalStateException("unreachable");  
        }

        latency.printf(Locale.ROOT, "array,%d,%d,%d,%.3f%n", lenMs, records, numQueries,
            arrayNanos / 1000.0 / numQueries);
        latency.printf(Locale.ROOT, "prefix,%d,%d,%d,%.3f%n", lenMs, records, numQueries,
            indexNanos / 1000.0 / numQueries);
      }
    }
    System.out.println("QueryLengthSink: " + records + " records indexed online, "
        + numQueries + " how-much queries benchmarked for " + queryLensMs.length
        + " window lengths");
  }

  
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
