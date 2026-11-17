import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Objects;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import temporalindex.Disorder;
import temporalindex.DisorderConfig;
import temporalindex.DisorderSource;
import temporalindex.Zipf;
import temporalindex.StampDerivedTime;

/**
 * No-provenance (no-provenance) Twitter Verified Join dataflow.
 *
 * <pre>
 *   Source --+-- FlatMap -- Agg(30min/5min avg) ----------------+
 *            |                                                   Join(30min slide 5min) -- Sink
 *            +-- Filter(verified) -- FlatMap -- Agg(30min/5min avg) +
 * </pre>
 *
 * Usage:
 *   flink run --class TwitterVerifiedJoin twitter-1-1.0-REV-SNAPSHOT.jar
 *       &lt;inputFile&gt; &lt;outputFile&gt; [sampleProb]
 */
public class TwitterVerifiedJoin {

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: TwitterVerifiedJoin <inputFile> <outputFile> [sampleProb]");
      System.exit(1);
    }
    final String inputFile = args[0];
    final String outputFile = args[1];
    // Optional sampleProb: sink still hashes every output so the coin cost matches CAPS bench mode.
    final double sampleProb = args.length > 2 ? Double.parseDouble(args[2]) : 0.0;

    // Environment, parallel disordered source and watermarks (CAPS_* variables, see DisorderConfig). The
    // defaults are the ordered run: the periodic bounded-out-of-orderness watermark with delta = 0, no
    // disorder. Same wiring in the CAPS and the no-provenance job.
    final DisorderConfig cfg = DisorderConfig.fromEnv();
    final StreamExecutionEnvironment env = Disorder.environment(cfg);

    DataStream<Tweet> sourceStream = Disorder.source(env, cfg, "SOURCE", inputFile, new TweetParser(),
        r -> r.timestamp, Tweet.class);

    // Left path: all tweets
    DataStream<HashtagAvg> leftAggIn = sourceStream
        .flatMap(new ToHashtagScore())
        .name("MAP-ALL");
    OutputTag<HashtagAvg> lateAggAll = Disorder.lateTag("AGG-ALL", leftAggIn);
    SingleOutputStreamOperator<HashtagAvg> opAggAll = Disorder.lateOutput(leftAggIn
        .keyBy(h -> h.hashtag)
        .window(SlidingEventTimeWindows.of(Time.minutes(30), Time.minutes(5)))
        .trigger(Disorder.trigger(cfg, Time.minutes(5))), lateAggAll, cfg)
        .aggregate(new AvgAggregate())
        .name("AGG-ALL");
    Disorder.lateSink(opAggAll, lateAggAll, "AGG-ALL", cfg);
    DataStream<HashtagAvg> leftAgg = opAggAll
        .process(new StampDerivedTime<HashtagAvg>((t, ts) -> t.timestamp = ts))
        .returns(HashtagAvg.class)
        .name("STAMP-AGG-ALL");

    // Right path: verified tweets only
    DataStream<HashtagAvg> rightAggIn = sourceStream
        .filter(t -> t.verified())
        .name("FILTER-VERIFIED")
        .flatMap(new ToHashtagScore())
        .name("MAP-VERIFIED");
    OutputTag<HashtagAvg> lateAggVerified = Disorder.lateTag("AGG-VERIFIED", rightAggIn);
    SingleOutputStreamOperator<HashtagAvg> opAggVerified = Disorder.lateOutput(rightAggIn
        .keyBy(h -> h.hashtag)
        .window(SlidingEventTimeWindows.of(Time.minutes(30), Time.minutes(5)))
        .trigger(Disorder.trigger(cfg, Time.minutes(5))), lateAggVerified, cfg)
        .aggregate(new AvgAggregate())
        .name("AGG-VERIFIED");
    Disorder.lateSink(opAggVerified, lateAggVerified, "AGG-VERIFIED", cfg);
    DataStream<HashtagAvg> rightAgg = opAggVerified
        .process(new StampDerivedTime<HashtagAvg>((t, ts) -> t.timestamp = ts))
        .returns(HashtagAvg.class)
        .name("STAMP-AGG-VERIFIED");

    // Join on hashtag
    leftAgg
        .join(rightAgg)
        .where(h -> h.hashtag)
        .equalTo(h -> h.hashtag)
        .window(SlidingEventTimeWindows.of(Time.minutes(30), Time.minutes(5)))
        .trigger(Disorder.trigger(cfg, Time.minutes(5)))
        .with(new JoinAvgs())
        .name("JOIN")
        .process(new StampDerivedTime<HashtagAvg>((t, ts) -> t.timestamp = ts))
        .returns(HashtagAvg.class)
        .name("STAMP-JOIN")
        .addSink(new FileSink(outputFile, sampleProb))
        // The discard sink of the timing runs follows CAPS_SINK_PARALLELISM like the CAPS sinks; a file sink stays on one subtask.
        .setParallelism("none".equals(outputFile) ? cfg.sinkParallelism : 1)
        .name("SINK");

    Disorder.execute(env, "TwitterVerifiedJoin-No-provenance");
  }

  // Data types
  public static class Tweet implements Serializable {
    public long tweetId;
    public long timestamp;
    public String hashtags;
    public int likes;
    public int retweets;
    public boolean isVerified;
    public boolean isBlueVerified;
    public String location;

    public int score() { return likes + retweets; }
    public boolean verified() { return isVerified || isBlueVerified; }
  }

  public static class HashtagAvg implements Serializable {
    public String hashtag;
    public long timestamp;
    public double value1;
    public double value2;

    public HashtagAvg() {}

    public HashtagAvg(String hashtag, long timestamp, double value1, double value2) {
      this.hashtag = hashtag;
      this.timestamp = timestamp;
      this.value1 = value1;
      this.value2 = value2;
    }

    @Override
    public String toString() {
      return hashtag + "," + timestamp + "," + value1 + "," + value2;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof HashtagAvg)) return false;
      HashtagAvg that = (HashtagAvg) o;
      return timestamp == that.timestamp
          && Double.compare(that.value1, value1) == 0
          && Double.compare(that.value2, value2) == 0
          && Objects.equals(hashtag, that.hashtag);
    }

    @Override
    public int hashCode() {
      return Objects.hash(hashtag, timestamp, value1, value2);
    }
  }

  // Source
  /**
   * Parses a line of twitter.txt. With CAPS_ZIPF every hashtag (the key of the window aggregates) is replaced by
   * "t" and a Zipf-distributed rank, one draw per hashtag occurrence.
   */
  public static class TweetParser extends DisorderSource.Parser<Tweet> {
    private transient Zipf zipf;

    @Override
    public void open(DisorderConfig cfg) {
      zipf = cfg.zipf();
    }

    @Override
    public Tweet parse(String line, long lineNo) {
      String[] parts = line.split(",", 8);
      Tweet t = new Tweet();
      t.tweetId = Long.parseLong(parts[0]);
      t.timestamp = Long.parseLong(parts[1]);
      t.hashtags = zipf == null ? parts[2] : remap(parts[2], lineNo);
      t.likes = Integer.parseInt(parts[3]);
      t.retweets = Integer.parseInt(parts[4]);
      t.isVerified = Boolean.parseBoolean(parts[5]);
      t.isBlueVerified = Boolean.parseBoolean(parts[6]);
      t.location = parts.length > 7 ? parts[7] : "";
      return t;
    }

    private String remap(String hashtags, long lineNo) {
      if (hashtags.isEmpty()) return hashtags;
      StringBuilder sb = new StringBuilder();
      String[] tags = hashtags.split("\\|");
      for (int i = 0; i < tags.length; i++) {
        if (tags[i].isEmpty()) continue;
        if (sb.length() > 0) sb.append('|');
        sb.append('t').append(zipf.rank(lineNo, i));
      }
      return sb.toString();
    }
  }

  // Operators
  public static class ToHashtagScore implements FlatMapFunction<Tweet, HashtagAvg> {
    @Override
    public void flatMap(Tweet tweet, Collector<HashtagAvg> out) {
      if (tweet.hashtags == null || tweet.hashtags.isEmpty()) return;
      int score = tweet.score();
      for (String tag : tweet.hashtags.split("\\|")) {
        if (!tag.isEmpty()) {
          out.collect(new HashtagAvg(tag, tweet.timestamp, score, 0));
        }
      }
    }
  }

  public static class AvgAggregate
      implements AggregateFunction<HashtagAvg, AvgAggregate.Acc, HashtagAvg> {

    @Override
    public Acc createAccumulator() {
      return new Acc();
    }

    @Override
    public Acc add(HashtagAvg value, Acc acc) {
      if (acc.hashtag == null) acc.hashtag = value.hashtag;
      acc.sum += value.value1;
      acc.count++;
      return acc;
    }

    @Override
    public HashtagAvg getResult(Acc acc) {
      double avg = acc.count > 0 ? acc.sum / acc.count : 0;
      // Placeholder: StampDerivedTime sets the window end after the operator.
      return new HashtagAvg(acc.hashtag, 0, avg, 0);
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.sum += b.sum;
      a.count += b.count;
      if (a.hashtag == null) a.hashtag = b.hashtag;
      return a;
    }

    public static class Acc implements Serializable {
      String hashtag;
      double sum;
      int count;
    }
  }

  public static class JoinAvgs
      implements JoinFunction<HashtagAvg, HashtagAvg, HashtagAvg> {
    @Override
    public HashtagAvg join(HashtagAvg left, HashtagAvg right) {
      return new HashtagAvg(
          left.hashtag,
          // Placeholder: StampDerivedTime sets the window end after the join.
          0,
          left.value1,
          right.value1);
    }
  }

  // Sink
  public static class FileSink extends RichSinkFunction<HashtagAvg> {
    private final String path;
    private final double sampleProb;
    private transient PrintWriter pw;
    // Same Knuth 64-bit LCG as the three provenance sinks (seed 42), so
    // noprov pays the same per-output coin cost (~2 ns, no CAS) and the
    // runtime difference isolates provenance. The sample is unused;
    // coinAcc is reported at close so the JIT cannot DCE the multiply.
    //
    // The LCG is no longer stepped. Always-per-output work is now the
    // same logical-key hash + threshold as the provenance sinks.
    private transient temporalindex.LogicalSampler sampler;
    private transient temporalindex.SampleKeyFn<HashtagAvg> keyFn;
    private transient long predicateChecksum;
    private transient int selected;

    public FileSink(String path) {
      this(path, 0.0);
    }

    public FileSink(String path, double sampleProb) {
      this.path = path;
      this.sampleProb = sampleProb;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      // Output path "none": discard outputs (timing runs without file I/O).
      if (!"none".equals(path)) {
        pw = new PrintWriter(new FileWriter(path), true);
      }
      sampler = new temporalindex.LogicalSampler();
      keyFn = new temporalindex.SampleKeyFn<HashtagAvg>() {
        @Override
        public void writeKey(HashtagAvg v, java.io.DataOutput out)
            throws java.io.IOException {
          temporalindex.LogicalSampler.writeString(out, v.hashtag);
          out.writeLong(v.timestamp);
          temporalindex.LogicalSampler.writeDoubleBits(out, v.value1);
          temporalindex.LogicalSampler.writeDoubleBits(out, v.value2);
        }
      };
      predicateChecksum = 0L;
      selected = 0;
    }

    @Override
    public void invoke(HashtagAvg value, Context context) {
      long h = sampler.hashKey(keyFn, value);
      predicateChecksum += h;
      if (temporalindex.LogicalSampler.selected(h, sampleProb)) {
        selected++;
      }
      if (pw != null) {
        pw.println(value);
      }
    }

    @Override
    public void close() throws Exception {
      if (pw != null) {
        pw.print("--- OUTPUT END ---");
        pw.flush();
        pw.close();
      }
      System.out.println("FileSink: predicateChecksum=" + predicateChecksum
          + " selected=" + selected);
    }
  }
}
