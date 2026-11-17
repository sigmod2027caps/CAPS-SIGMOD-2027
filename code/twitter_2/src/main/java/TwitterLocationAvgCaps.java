import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import org.apache.flink.api.common.JobExecutionResult;
import java.util.Objects;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import caps.BenchSinks;
import caps.CapsCardinalityDiscardSink;
import caps.CapsMetadataProbe;
import caps.HowMuch;
import caps.IndexingSink;
import caps.OutputLogSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import temporalindex.Disorder;
import temporalindex.LiveState;
import temporalindex.LiveStateSink;
import temporalindex.DisorderConfig;
import temporalindex.DisorderSource;
import temporalindex.Zipf;
import temporalindex.StampDerivedTime;

/**
 * How-much provenance (virtual-source) Twitter Location Average dataflow.
 *
 * Same linear pipeline as vanilla, but each tuple carries a {@link HowMuch}
 * meta-data with one counter per virtual source:
 *   - HAS_LOC: tuples originating from tweets WITH a location
 *   - NO_LOC:  tuples originating from tweets WITHOUT a location
 *
 * <pre>
 *   Source(tag VS counters) -> FlatMap -> Agg(6h slide 1h, sum counters) -> Sink
 * </pre>
 *
 * Output: hashtag,timestamp,avgScore,countHasLoc,countNoLoc
 */
public class TwitterLocationAvgCaps {

  // How-much provenance channels: one per virtual source.
  public static final int HAS_LOC = 0;
  public static final int NO_LOC = 1;
  public static final int CHANNELS = 2;

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: TwitterLocationAvgCaps <inputFile> <outputFile>"
          + " [mode benchDir params...]   (mode: queries | expiry | summarize"
          + " | memory_volume, see BenchSinks / CapsMetadataProbe)");
      System.exit(1);
    }
    final String inputFile = args[0];
    final String outputFile = args[1];
    // Bench mode: instead of writing outputs to a file, insert them into the
    // online CapsIndex as they are produced and run one of the paper
    // Section 5 experiments at end of stream (see BenchSinks).
    final boolean memoryVolume = CapsMetadataProbe.enabled(args, 2);
    final boolean live = LiveState.enabled(args, 2);
    final boolean benchMode = args.length > 2 && !memoryVolume && !live;

    // Environment, parallel disordered source and watermarks (CAPS_* variables, see DisorderConfig). The
    // defaults are the ordered run: the periodic bounded-out-of-orderness watermark with delta = 0, no
    // disorder. Same wiring in the CAPS and the no-provenance job.
    final DisorderConfig cfg = DisorderConfig.fromEnv();
    final StreamExecutionEnvironment env = Disorder.environment(cfg);

    DataStream<Tweet> sourceStream = Disorder.source(env, cfg, "SOURCE", inputFile, new TweetParser(),
        r -> r.timestamp, Tweet.class);

    IndexingSink.HmFn<HashtagAvg> hm =
        (IndexingSink.HmFn<HashtagAvg>) h -> h.hm;

    DataStream<HashtagAvg> mapped = sourceStream
        .flatMap(new ToHashtagScore())
        .name("MAP");
    mapped = CapsMetadataProbe.attach(mapped, memoryVolume, "MAP", hm);
    OutputTag<HashtagAvg> lateAgg6h = Disorder.lateTag("AGG-6H", mapped);
    SingleOutputStreamOperator<HashtagAvg> opAgg6h = Disorder.lateOutput(mapped
        .keyBy(h -> h.hashtag)
        .window(SlidingEventTimeWindows.of(Time.hours(6), Time.hours(1)))
        .trigger(LiveState.trigger(Disorder.trigger(cfg, Time.hours(1)), "AGG-6H", 0, Time.hours(1), LiveState.constant(CHANNELS * 8L), null)), lateAgg6h, cfg)
        .aggregate(new AvgAggregate())
        .name("AGG-6H");
    Disorder.lateSink(opAgg6h, lateAgg6h, "AGG-6H", cfg);
    DataStream<HashtagAvg> aggregated = opAgg6h
        .process(new StampDerivedTime<HashtagAvg>((t, ts) -> t.timestamp = ts))
        .returns(HashtagAvg.class)
        .name("STAMP-AGG-6H");
    aggregated = CapsMetadataProbe.attach(aggregated, memoryVolume, "AGG-6H", hm);

    if (live) {
      // live_memory: discard sink that publishes the live-state registry when the stream ends
      aggregated
          .addSink(new LiveStateSink<HashtagAvg>(x -> x.hm.channels() * 8L))
          .name("SINK");
    } else if (memoryVolume) {
      aggregated
          .addSink(new CapsCardinalityDiscardSink<HashtagAvg>())
          .name("SINK");
    } else if (benchMode) {
      // Index sinks run at the job's parallelism, one shard per subtask, and receive the watermarks.
      BenchSinks.attach(aggregated, BenchSinks.create(args, 2,
              (IndexingSink.TsFn<HashtagAvg>) h -> h.timestamp,
              (IndexingSink.HmFn<HashtagAvg>) h -> h.hm,
              (temporalindex.SampleKeyFn<HashtagAvg>) (h, out) -> {
                temporalindex.LogicalSampler.writeString(out, h.hashtag);
                out.writeLong(h.timestamp);
                temporalindex.LogicalSampler.writeDoubleBits(out, h.avgScore);
              },
              CHANNELS),
          "SINK-INDEX");
      // E4 (recovery): with CAPS_OUTPUT_LOG=1 every output is also logged with its id (operator, window end, key).
      OutputLogSink.attachIfEnabled(aggregated, args, 2, "AGG-6H", c -> c.timestamp, c -> c.hashtag);
    } else {
      aggregated
          .addSink(new FileSink(outputFile))
          .setParallelism(1)
          .name("SINK");
    }

    JobExecutionResult result = Disorder.execute(env, "TwitterLocationAvg-HowMuch");
    CapsMetadataProbe.writeIfEnabled(result, args, 2, "twitter_2");
    LiveState.writeIfEnabled(result, args, 2, "twitter_2", "caps");
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
    public boolean hasLocation() { return location != null && !location.isEmpty(); }
  }

  public static class HashtagAvg implements Serializable {
    public String hashtag;
    public long timestamp;
    public double avgScore;
    public HowMuch hm;

    public HashtagAvg() {}

    public HashtagAvg(String hashtag, long timestamp, double avgScore, HowMuch hm) {
      this.hashtag = hashtag;
      this.timestamp = timestamp;
      this.avgScore = avgScore;
      this.hm = hm;
    }

    @Override
    public String toString() {
      return hashtag + "," + timestamp + "," + avgScore + "," + hm;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof HashtagAvg)) return false;
      HashtagAvg that = (HashtagAvg) o;
      return timestamp == that.timestamp
          && Double.compare(that.avgScore, avgScore) == 0
          && Objects.equals(hashtag, that.hashtag);
    }

    @Override
    public int hashCode() {
      return Objects.hash(hashtag, timestamp, avgScore);
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
      int channel = tweet.hasLocation() ? HAS_LOC : NO_LOC;
      int score = tweet.score();
      for (String tag : tweet.hashtags.split("\\|")) {
        if (!tag.isEmpty()) {
          out.collect(new HashtagAvg(tag, tweet.timestamp, score,
              HowMuch.one(channel, CHANNELS)));
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
      acc.sum += value.avgScore;
      acc.count++;
      acc.hm.mergeWith(value.hm);
      return acc;
    }

    @Override
    public HashtagAvg getResult(Acc acc) {
      double avg = acc.count > 0 ? acc.sum / acc.count : 0;
      // Placeholder: StampDerivedTime sets the window end after the operator.
      return new HashtagAvg(acc.hashtag, 0, avg, acc.hm.copy());
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.sum += b.sum;
      a.count += b.count;
      a.hm.mergeWith(b.hm);
      if (a.hashtag == null) a.hashtag = b.hashtag;
      return a;
    }

    public static class Acc implements Serializable {
      String hashtag;
      double sum;
      int count;
      HowMuch hm = new HowMuch(CHANNELS);
    }
  }

  // Sink
  public static class FileSink extends RichSinkFunction<HashtagAvg> {
    private final String path;
    private transient PrintWriter pw;

    public FileSink(String path) {
      this.path = path;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      pw = new PrintWriter(new FileWriter(path), true);
    }

    @Override
    public void invoke(HashtagAvg value, Context context) {
      pw.println(value);
    }

    @Override
    public void close() throws Exception {
      if (pw != null) {
        pw.print("--- OUTPUT END ---");
        pw.flush();
        pw.close();
      }
    }
  }
}
