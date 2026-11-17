package realscale;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.FileWriter;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Real input with a synthetic dataflow on top (channel-scalability experiment on real data).
 *
 * Both datasets give one record per (ride) / (tweet, hashtag): (timestamp, key, channel, value).
 *   taxi    taxis.txt  taxi_id,ts_ms,passengers,zone,payment,fare,tip
 *           key = taxi id, channel = pickup zone mod M, value = fare in cents, window 1 h
 *   taxi_zone  the same input and window; key = pickup zone, channel = taxi id mod M (outputs combine many channels)
 *   tweets  twitter.txt  tweetId,ts_ms,hashtags(|),likes,retweets,isVerified,isBlueVerified,location
 *           one record per hashtag: key = hashtag, channel = hash(location) mod M (the empty
 *           location, "no location", is a value of its own), value = likes + retweets, window 30 min
 * The dataflow of every method: Source (assign channel) -> keyBy(key) -> tumbling window ->
 * Agg(count, sum) -> Stamp(window end) -> Sink. The average is sum / count.
 * Only the width of the annotation depends on M; input, window and outputs do not.
 */
public final class RealData {

  private RealData() {}

  public static long windowMs(String dataset) {
    if (dataset.equals("taxi") || dataset.equals("taxi_zone")) return 3_600_000L;
    if (dataset.equals("tweets")) return 1_800_000L;
    throw new IllegalArgumentException("dataset must be taxi, taxi_zone or tweets: " + dataset);
  }

  /** Called once per record of an input line. */
  public interface Emit {
    void record(long ts, String key, int channel, long value);
  }

  /** Parses one input line of the dataset and emits its records (taxi: one, tweets: one per hashtag). */
  public static void parse(String dataset, String line, int channels, Emit out) {
    if (dataset.equals("taxi")) {
      String[] f = line.split(",", 7);
      long fareCents = Math.round(Double.parseDouble(f[5]) * 100.0);
      out.record(Long.parseLong(f[1]), f[0], Integer.parseInt(f[3]) % channels, fareCents);
      return;
    }
    if (dataset.equals("taxi_zone")) {   // same input and window; key = pickup zone, channel = taxi id mod M
      String[] f = line.split(",", 7);
      long fareCents = Math.round(Double.parseDouble(f[5]) * 100.0);
      out.record(Long.parseLong(f[1]), f[3], Integer.parseInt(f[0]) % channels, fareCents);
      return;
    }
    String[] f = line.split(",", 8);
    String tags = f[2];
    if (tags.isEmpty()) return;
    long ts = Long.parseLong(f[1]);
    long score = Long.parseLong(f[3]) + Long.parseLong(f[4]);
    int channel = Math.floorMod(f[7].hashCode(), channels);   // String.hashCode of the location text
    int from = 0;
    while (from <= tags.length()) {
      int to = tags.indexOf('|', from);
      if (to < 0) to = tags.length();
      if (to > from) out.record(ts, tags.substring(from, to), channel, score);
      from = to + 1;
    }
  }

  /** Identity of an output, the same in every method: selects the outputs of the query fixture. */
  public static boolean sampled(String key, long ts, long count, long sum, long period) {
    long h = key.hashCode() * 1000003L + ts + count * 131 + sum * 17;
    return Math.floorMod(h, period) == 0;
  }

  // Records and outputs
  public static class Rec implements Serializable {
    public long seq;       // 0, 1, 2, ... in input order (the Green variable of the record)
    public long timestamp;
    public String key;
    public int channel;
    public long value;
  }

  /** Output of the window: key, window end - 1, count, sum (+ the annotation in the subclasses). */
  public abstract static class AggOut implements Serializable {
    public String key;
    public long timestamp;
    public long count;
    public long sum;

    /** The row of the verification file. */
    public abstract String row();

    /** The annotation kept in the query fixture, or null. */
    public Object sample() {
      return null;
    }

    protected String head() {
      return key + "," + timestamp + "," + count + "," + sum;
    }
  }

  // Source and watermarks
  /** Reads the input file; subclasses create the record type of their method. */
  public abstract static class LineSource<R extends Rec> extends RichSourceFunction<R> {
    private final String dataset;
    private final String path;
    private final int channels;
    private volatile boolean running = true;

    protected LineSource(String dataset, String path, int channels) {
      this.dataset = dataset;
      this.path = path;
      this.channels = channels;
    }

    /** A record with the annotation of the method; the caller sets the fields of Rec. */
    protected abstract R make(long seq, int channel);

    @Override
    public void run(final SourceContext<R> ctx) throws Exception {
      final long[] seq = {0};
      Emit emit = (ts, key, channel, value) -> {
        long s = seq[0]++;
        R r = make(s, channel);
        r.seq = s;
        r.timestamp = ts;
        r.key = key;
        r.channel = channel;
        r.value = value;
        ctx.collect(r);
      };
      try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8), 1 << 16)) {
        String line;
        while (running && (line = br.readLine()) != null)
          if (!line.isEmpty()) parse(dataset, line, channels, emit);
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  /**
   * Punctuated watermark ts - 1 after every record: the records of one tweet share a timestamp, and
   * ts - 1 keeps the later ones of the same tweet from being late. A window [s, e) closes with the
   * first record at or after e.
   */
  public static class Assigner<R extends Rec> implements AssignerWithPunctuatedWatermarks<R> {
    @Override
    public long extractTimestamp(R r, long prev) {
      return r.timestamp;
    }

    @Override
    public Watermark checkAndGetNextWatermark(R r, long ts) {
      return new Watermark(ts - 1);
    }
  }

  // Sinks
  /**
   * Verification sink: every output as one row, in arrival order. With a sample directory it also keeps the
   * annotation of the outputs whose identity hash is 0 mod period, the query fixture (sample.ser).
   */
  public static class FileSink<T extends AggOut> extends RichSinkFunction<T> {
    private final String path;
    private final String sampleDir;
    private final long period;
    private transient PrintWriter pw;
    private transient ArrayList<Object> kept;

    public FileSink(String path, String sampleDir, long period) {
      this.path = path;
      this.sampleDir = sampleDir;
      this.period = period;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      pw = new PrintWriter(new BufferedWriter(new FileWriter(path), 1 << 20));
      kept = new ArrayList<Object>();
    }

    @Override
    public void invoke(T value, Context context) {
      pw.println(value.row());
      if (sampleDir != null && sampled(value.key, value.timestamp, value.count, value.sum, period))
        kept.add(value.sample());
    }

    @Override
    public void close() throws Exception {
      if (pw == null) return;
      pw.print("--- OUTPUT END ---");
      pw.close();
      if (sampleDir != null) writeSample(sampleDir, kept);
    }
  }

  static void writeSample(String dir, ArrayList<Object> kept) throws Exception {
    new File(dir).mkdirs();
    try (ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(dir + "/sample.ser"))) {
      os.writeObject(kept.toArray(new Object[0]));
    }
  }

  /** Timing sink: counts the outputs and writes the count at the end. No per-output work. */
  public static class DiscardSink<T extends AggOut> extends RichSinkFunction<T> {
    private final String dir;
    private transient long outputs;

    public DiscardSink(String dir) {
      this.dir = dir;
    }

    @Override
    public void invoke(T value, Context context) {
      outputs++;
    }

    @Override
    public void close() throws Exception {
      new File(dir).mkdirs();
      try (PrintWriter pw = new PrintWriter(new FileWriter(dir + "/outputs.txt"))) {
        pw.println(outputs);
      }
    }
  }
}
