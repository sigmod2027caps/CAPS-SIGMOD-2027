package caps;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Random;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class ProvenanceQuerySink<T> extends RichSinkFunction<T> {

  private static final long serialVersionUID = 1L;

  
  private static final int VERIFY_SAMPLES = 1000;

  private final IndexingSink.TsFn<T> tsFn;
  private final IndexingSink.HmFn<T> hmFn;
  private final int channels;
  private final String benchDir;
  private final double sampleProb;

  private transient ArrayList<Entry> entries;
  private transient Random coin;
  private transient int sampled;
  private transient long queryNanos;
  private transient long checksum;

  public ProvenanceQuerySink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn,
                             int channels, String benchDir, double sampleProb) {
    this.tsFn = tsFn;
    this.hmFn = hmFn;
    this.channels = channels;
    this.benchDir = benchDir;
    this.sampleProb = sampleProb;
  }

  @Override
  public void open(Configuration parameters) {
    entries = new ArrayList<>();
    coin = new Random(42);
    sampled = 0;
    queryNanos = 0;
    checksum = 0;
  }

  @Override
  public void invoke(T value, Context context) {
    
    Entry e = new Entry(tsFn.ts(value), hmFn.hm(value).counts.clone());
    entries.add(e);
    
    if (coin.nextDouble() < sampleProb) {
      long t0 = System.nanoTime();
      long answer = answer(e);
      queryNanos += System.nanoTime() - t0;
      sampled++;
      checksum += answer;
    }
  }

  @Override
  public void close() throws Exception {
    final int n = entries.size();
    if (n == 0) {
      throw new IllegalStateException("no records reached the provenance query sink");
    }
    if (sampled == 0) {
      throw new IllegalStateException("coin never landed heads: sampleProb too small");
    }
    final int samples = sampled;
    final long nanos = queryNanos;

    
    
    
    
    entries.sort(null);
    final int[] groupStart = groupStarts(entries);
    final int numGroups = groupStart.length - 1;
    final Random grnd = new Random(7);
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/provenance_answers.csv"))) {
      pw.println("i,ts,group_count,group_sum");
      for (int i = 0; i < VERIFY_SAMPLES; i++) {
        int g = grnd.nextInt(numGroups);
        long sum = 0;
        for (int k = groupStart[g]; k < groupStart[g + 1]; k++) {
          sum += answer(entries.get(k));
        }
        pw.println(i + "," + entries.get(groupStart[g]).ts + ","
            + (groupStart[g + 1] - groupStart[g]) + "," + sum);
      }
    }

    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/provenance_query.csv"))) {
      pw.println("method,outputs,samples,avg_ns,total_ms");
      pw.printf(Locale.ROOT, "caps,%d,%d,%.1f,%.3f%n", n, samples,
          (double) nanos / samples, nanos / 1e6);
    }
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/outputs_caps.txt"))) {
      pw.println(n);
    }
    System.out.println("ProvenanceQuerySink: " + n + " outputs kept, " + samples
        + " provenance queries answered online (coin p=" + sampleProb
        + ", checksum " + checksum + ")");
  }

  
  private long answer(Entry e) {
    long total = 0;
    for (int c = 0; c < channels; c++) {
      total += e.counts[c];
    }
    return total;
  }

  
  static int[] groupStarts(ArrayList<Entry> sorted) {
    int groups = 1;
    for (int i = 1; i < sorted.size(); i++) {
      if (sorted.get(i).ts != sorted.get(i - 1).ts) {
        groups++;
      }
    }
    int[] starts = new int[groups + 1];
    int g = 0;
    for (int i = 1; i < sorted.size(); i++) {
      if (sorted.get(i).ts != sorted.get(i - 1).ts) {
        starts[++g] = i;
      }
    }
    starts[groups] = sorted.size();
    return starts;
  }

  static final class Entry implements Comparable<Entry> {
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
