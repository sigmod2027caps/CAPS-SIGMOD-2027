import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import org.apache.flink.api.common.JobExecutionResult;
import java.util.Objects;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.util.Collector;

import ink.InkBenchSinks;
import ink.InkCardinalityDiscardSink;
import ink.InkMeta;
import ink.InkMetadataProbe;
import ink.InkQuerySink;
import ink.MonomialBuilder;
import ink.VarSource;
import temporalindex.LiveState;
import temporalindex.Zipf;
import temporalindex.DisorderSource;
import temporalindex.DisorderConfig;
import temporalindex.Disorder;
import temporalindex.LiveStateSink;
import temporalindex.StampDerivedTime;

/**
 * How-much provenance (virtual-source) Twitter Location Average dataflow.
 *
 * Same linear pipeline as vanilla, but each tuple carries an {@link InkMeta}
 * polynomial annotation (green_polynomial baseline in the paper) whose how-much
 * folding is one counter per virtual source:
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

  // Reorder slack for the online index. Every windowed operator stamps its output with
  // the right boundary of its window (StampDerivedTime), so at parallelism 1 the sink
  // input is already in timestamp order and the reorder buffer releases at once.
  public static final long INDEX_SLACK_MS = 0;

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: TwitterLocationAvgCaps <inputFile> <outputFile>"
          + " [mode benchDir params...]   (mode: provenance | provenance_timer"
          + " | memory_volume, see InkBenchSinks / InkMetadataProbe)");
      System.exit(1);
    }
    final String inputFile = args[0];
    final String outputFile = args[1];
    // Bench mode: instead of writing outputs to a file, insert them into the
    // online query sink as they are produced (see InkBenchSinks).
    final boolean memoryVolume = InkMetadataProbe.enabled(args, 2);
    final boolean live = LiveState.enabled(args, 2);
    final boolean benchMode = args.length > 2 && !memoryVolume && !live;

    // Same environment, source and watermark assigner as the CAPS and no-provenance jobs (CAPS_* variables, see
    // DisorderConfig): the defaults are the ordered run with a periodic watermark, no disorder.
    final DisorderConfig cfg = DisorderConfig.fromEnv();
    final StreamExecutionEnvironment env = Disorder.environment(cfg);

    DataStream<Tweet> sourceStream = Disorder.source(env, cfg, "SOURCE", inputFile, new TweetParser(),
        r -> r.timestamp, Tweet.class);

    InkQuerySink.MetaFn<HashtagAvg> meta =
        (InkQuerySink.MetaFn<HashtagAvg>) h -> h.hm;

    DataStream<HashtagAvg> mapped = sourceStream
        .flatMap(new ToHashtagScore())
        .name("MAP");
    mapped = InkMetadataProbe.attach(mapped, memoryVolume, "MAP", meta);
    DataStream<HashtagAvg> aggregated = mapped
        .keyBy(h -> h.hashtag)
        .window(SlidingEventTimeWindows.of(Time.hours(6), Time.hours(1)))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "AGG-6H", 0, Time.hours(1), null, null))
        .aggregate(LiveState.on() ? new AvgAggregate.Live() : new AvgAggregate())
        .name("AGG-6H")
        .process(new StampDerivedTime<HashtagAvg>((t, ts) -> t.timestamp = ts))
        .returns(HashtagAvg.class)
        .name("STAMP-AGG-6H");
    aggregated = InkMetadataProbe.attach(aggregated, memoryVolume, "AGG-6H", meta);

    if (live) {
      // live_memory: discard sink that publishes the live-state registry when the stream ends
      aggregated
          .addSink(new LiveStateSink<HashtagAvg>(x -> x.hm.metadataBytes()))
          .name("SINK");
    } else if (memoryVolume) {
      aggregated
          .addSink(new InkCardinalityDiscardSink<HashtagAvg>())
          .name("SINK");
    } else if (benchMode) {
      InkBenchSinks.attach(aggregated, InkBenchSinks.create(args, 2,
              (InkQuerySink.TsFn<HashtagAvg>) h -> h.timestamp,
              (InkQuerySink.MetaFn<HashtagAvg>) h -> h.hm,
              (temporalindex.SampleKeyFn<HashtagAvg>) (h, out) -> {
                temporalindex.LogicalSampler.writeString(out, h.hashtag);
                out.writeLong(h.timestamp);
                temporalindex.LogicalSampler.writeDoubleBits(out, h.avgScore);
              },
              CHANNELS, INDEX_SLACK_MS),
          "SINK-INDEX");
    } else {
      aggregated
          .addSink(new FileSink(outputFile, CHANNELS))
          .setParallelism(1)
          .name("SINK");
    }

    JobExecutionResult result = env.execute("TwitterLocationAvg-HowMuch");
    InkMetadataProbe.writeIfEnabled(result, args, 2, "twitter_2");
    LiveState.writeIfEnabled(result, args, 2, "twitter_2", "green_polynomial");
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
    public InkMeta hm;

    public HashtagAvg() {}

    public HashtagAvg(String hashtag, long timestamp, double avgScore, InkMeta hm) {
      this.hashtag = hashtag;
      this.timestamp = timestamp;
      this.avgScore = avgScore;
      this.hm = hm;
    }

    @Override
    public String toString() {
      return hashtag + "," + timestamp + "," + avgScore;
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
  public static class ToHashtagScore extends RichFlatMapFunction<Tweet, HashtagAvg> {
    // Source-tuple variables of this operator instance, unique over its subtasks; see InkMeta, VarSource.
    private final VarSource vars = new VarSource(CHANNELS);

    @Override
    public void open(Configuration parameters) {
      vars.open(getRuntimeContext());
    }

    @Override
    public void flatMap(Tweet tweet, Collector<HashtagAvg> out) {
      if (tweet.hashtags == null || tweet.hashtags.isEmpty()) return;
      int channel = tweet.hasLocation() ? HAS_LOC : NO_LOC;
      int score = tweet.score();
      for (String tag : tweet.hashtags.split("\\|")) {
        if (!tag.isEmpty()) {
          out.collect(new HashtagAvg(tag, tweet.timestamp, score,
              vars.next(channel)));
        }
      }
    }
  }

  public static class AvgAggregate
      implements AggregateFunction<HashtagAvg, AvgAggregate.Acc, HashtagAvg> {

    /** Same aggregate; reports the growth of its accumulator's annotation (8 B per variable) to LiveState. Used in live_memory mode only. */
    public static class Live extends AvgAggregate {
      @Override
      public Acc add(HashtagAvg value, Acc acc) {
        long before = acc.hm.numVars();
        super.add(value, acc);
        LiveState.grow(8L * (acc.hm.numVars() - before));
        return acc;
      }
    }

    @Override
    public Acc createAccumulator() {
      return new Acc();
    }

    @Override
    public Acc add(HashtagAvg value, Acc acc) {
      if (acc.hashtag == null) acc.hashtag = value.hashtag;
      acc.sum += value.avgScore;
      acc.count++;
      acc.hm.mul(value.hm);
      return acc;
    }

    @Override
    public HashtagAvg getResult(Acc acc) {
      double avg = acc.count > 0 ? acc.sum / acc.count : 0;
      // Placeholder: StampDerivedTime sets the window end after the operator.
      return new HashtagAvg(acc.hashtag, 0, avg, acc.hm.toMeta());
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.sum += b.sum;
      a.count += b.count;
      a.hm.mul(b.hm);
      if (a.hashtag == null) a.hashtag = b.hashtag;
      return a;
    }

    public static class Acc implements Serializable {
      String hashtag;
      double sum;
      int count;
      MonomialBuilder hm = new MonomialBuilder();
    }
  }

  // Sink
  public static class FileSink extends RichSinkFunction<HashtagAvg> {
    private final String path;
    private final int channels;
    private transient PrintWriter pw;

    public FileSink(String path, int channels) {
      this.path = path;
      this.channels = channels;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      pw = new PrintWriter(new FileWriter(path), true);
    }

    @Override
    public void invoke(HashtagAvg value, Context context) {
      // The how-much query of this baseline: fold the polynomial into one counter per channel.
      pw.println(value + "," + value.hm.howMuchCsv(channels));
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
