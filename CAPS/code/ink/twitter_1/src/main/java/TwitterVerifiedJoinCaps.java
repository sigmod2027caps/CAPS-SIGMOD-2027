import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import org.apache.flink.api.common.JobExecutionResult;
import java.util.Objects;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
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
 * How-much provenance (path-based) Twitter Verified Join dataflow.
 *
 * Identical computation to the vanilla job, but each tuple carries an
 * {@link InkMeta} polynomial annotation (green_polynomial baseline in the paper)
 * whose how-much folding is one counter per path, tracking how many
 * source tweets contributed to it via the left (all-tweets, PL) and right
 * (verified-only, PR) paths.
 *
 * <pre>
 *   Source --+-- FlatMap(PL=1) -- Agg(30min/5min, sum counts) ----------+
 *            |                                                           Join(30min slide 5min, merge counts) -- Sink
 *            +-- Filter(verified) -- FlatMap(PR=1) -- Agg(30min/5min) ---+
 * </pre>
 *
 * Output per line: hashtag,timestamp,avgAll,avgVerified,countPL,countPR
 *
 * Setting the environment variable CAPS_GRANULARITY=source switches the
 * meta-data to one counter for the single source (1 channel instead of 2),
 * used by the paths experiment to show the capture cost does not depend on
 * the number of counters.
 */
public class TwitterVerifiedJoinCaps {

  // How-much provenance channels: one per path (the dataflow has a single
  // source, multiplexed into the all-tweets and verified-only paths).
  public static final int PL = 0;
  public static final int PR = 1;
  public static final int PATH_CHANNELS = 2;
  public static final int SOURCE_CHANNELS = 1;

  // Reorder slack for the online index. Every windowed operator stamps its output with
  // the right boundary of its window (StampDerivedTime), so at parallelism 1 the sink
  // input is already in timestamp order and the reorder buffer releases at once.
  public static final long INDEX_SLACK_MS = 0;

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: TwitterVerifiedJoinCaps <inputFile> <outputFile>"
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

    // Default: one counter per path (2). CAPS_GRANULARITY=source: one counter
    // for the single source (1), for the counter-count cost comparison.
    final boolean sourceGranularity = "source".equals(System.getenv("CAPS_GRANULARITY"));
    final int channels = sourceGranularity ? SOURCE_CHANNELS : PATH_CHANNELS;

    // Same environment, source and watermark assigner as the CAPS and no-provenance jobs (CAPS_* variables, see
    // DisorderConfig): the defaults are the ordered run with a periodic watermark, no disorder.
    final DisorderConfig cfg = DisorderConfig.fromEnv();
    final StreamExecutionEnvironment env = Disorder.environment(cfg);

    DataStream<Tweet> sourceStream = Disorder.source(env, cfg, "SOURCE", inputFile, new TweetParser(),
        r -> r.timestamp, Tweet.class);

    // Annotation bytes of one buffered tuple (live_memory): the monomials of its polynomial.
    final LiveState.Sizer<HashtagAvg> polyBytes = c -> c.hm.metadataBytes();

    InkQuerySink.MetaFn<HashtagAvg> meta =
        (InkQuerySink.MetaFn<HashtagAvg>) h -> h.hm;

    // Left path (PL): all tweets - tag countPL=1
    DataStream<HashtagAvg> leftMapped = sourceStream
        .flatMap(new ToHashtagScore(true, sourceGranularity, channels))
        .name("MAP-ALL");
    leftMapped = InkMetadataProbe.attach(leftMapped, memoryVolume, "MAP-ALL", meta);
    DataStream<HashtagAvg> leftAgg = leftMapped
        .keyBy(h -> h.hashtag)
        .window(SlidingEventTimeWindows.of(Time.minutes(30), Time.minutes(5)))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "AGG-ALL", 0, Time.minutes(5), null, null))
        .aggregate(LiveState.on() ? new AvgAggregate.Live(channels) : new AvgAggregate(channels))
        .name("AGG-ALL")
        .process(new StampDerivedTime<HashtagAvg>((t, ts) -> t.timestamp = ts))
        .returns(HashtagAvg.class)
        .name("STAMP-AGG-ALL");
    leftAgg = InkMetadataProbe.attach(leftAgg, memoryVolume, "AGG-ALL", meta);

    // Right path (PR): verified tweets only - tag countPR=1
    DataStream<HashtagAvg> rightMapped = sourceStream
        .filter(t -> t.verified())
        .name("FILTER-VERIFIED")
        .flatMap(new ToHashtagScore(false, sourceGranularity, channels))
        .name("MAP-VERIFIED");
    rightMapped = InkMetadataProbe.attach(rightMapped, memoryVolume, "MAP-VERIFIED", meta);
    DataStream<HashtagAvg> rightAgg = rightMapped
        .keyBy(h -> h.hashtag)
        .window(SlidingEventTimeWindows.of(Time.minutes(30), Time.minutes(5)))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "AGG-VERIFIED", 0, Time.minutes(5), null, null))
        .aggregate(LiveState.on() ? new AvgAggregate.Live(channels) : new AvgAggregate(channels))
        .name("AGG-VERIFIED")
        .process(new StampDerivedTime<HashtagAvg>((t, ts) -> t.timestamp = ts))
        .returns(HashtagAvg.class)
        .name("STAMP-AGG-VERIFIED");
    rightAgg = InkMetadataProbe.attach(rightAgg, memoryVolume, "AGG-VERIFIED", meta);

    // Join on hashtag - merge counts from both paths
    DataStream<HashtagAvg> joined = leftAgg
        .join(rightAgg)
        .where(h -> h.hashtag)
        .equalTo(h -> h.hashtag)
        .window(SlidingEventTimeWindows.of(Time.minutes(30), Time.minutes(5)))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "JOIN", 1, Time.minutes(5), null, LiveState.join(polyBytes, polyBytes)))
        .apply(new JoinAvgs())
        .process(new StampDerivedTime<HashtagAvg>((t, ts) -> t.timestamp = ts))
        .returns(HashtagAvg.class)
        .name("STAMP-JOIN");
    joined = InkMetadataProbe.attach(joined, memoryVolume, "JOIN", meta);

    if (live) {
      // live_memory: discard sink that publishes the live-state registry when the stream ends
      joined
          .addSink(new LiveStateSink<HashtagAvg>(x -> x.hm.metadataBytes()))
          .name("SINK");
    } else if (memoryVolume) {
      joined
          .addSink(new InkCardinalityDiscardSink<HashtagAvg>())
          .name("SINK");
    } else if (benchMode) {
      InkBenchSinks.attach(joined, InkBenchSinks.create(args, 2,
              (InkQuerySink.TsFn<HashtagAvg>) h -> h.timestamp,
              (InkQuerySink.MetaFn<HashtagAvg>) h -> h.hm,
              (temporalindex.SampleKeyFn<HashtagAvg>) (h, out) -> {
                temporalindex.LogicalSampler.writeString(out, h.hashtag);
                out.writeLong(h.timestamp);
                temporalindex.LogicalSampler.writeDoubleBits(out, h.value1);
                temporalindex.LogicalSampler.writeDoubleBits(out, h.value2);
              },
              channels, INDEX_SLACK_MS),
          "SINK-INDEX");
    } else {
      joined
          .addSink(new FileSink(outputFile, channels))
          .setParallelism(1)
          .name("SINK");
    }

    JobExecutionResult result = env.execute("TwitterVerifiedJoin-HowMuch");
    InkMetadataProbe.writeIfEnabled(result, args, 2, "twitter_1");
    LiveState.writeIfEnabled(result, args, 2, "twitter_1", "green_polynomial");
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
    public InkMeta hm;

    public HashtagAvg() {}

    public HashtagAvg(String hashtag, long timestamp, double value1, double value2,
                      InkMeta hm) {
      this.hashtag = hashtag;
      this.timestamp = timestamp;
      this.value1 = value1;
      this.value2 = value2;
      this.hm = hm;
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
  /**
   * Splits a tweet into (hashtag, score) pairs, tagging each with
   * the path-based how-much provenance count: channel PL for the
   * left path, channel PR for the right path. With source
   * granularity the single channel is the (only) source.
   */
  public static class ToHashtagScore extends RichFlatMapFunction<Tweet, HashtagAvg> {
    private final boolean leftPath;
    private final boolean sourceGranularity;
    private final int channels;
    // Source-tuple variables of this operator instance, unique over its subtasks; see InkMeta, VarSource.
    private final VarSource vars;

    public ToHashtagScore(boolean leftPath, boolean sourceGranularity, int channels) {
      this.leftPath = leftPath;
      this.sourceGranularity = sourceGranularity;
      this.channels = channels;
      this.vars = new VarSource(channels);
    }

    @Override
    public void open(Configuration parameters) {
      vars.open(getRuntimeContext());
    }

    @Override
    public void flatMap(Tweet tweet, Collector<HashtagAvg> out) {
      if (tweet.hashtags == null || tweet.hashtags.isEmpty()) return;
      int channel = sourceGranularity ? 0 : (leftPath ? PL : PR);
      int score = tweet.score();
      for (String tag : tweet.hashtags.split("\\|")) {
        if (!tag.isEmpty()) {
          out.collect(new HashtagAvg(tag, tweet.timestamp, score, 0,
              vars.next(channel)));
        }
      }
    }
  }

  /**
   * Windowed average that also sums path-based provenance counts.
   */
  public static class AvgAggregate
      implements AggregateFunction<HashtagAvg, AvgAggregate.Acc, HashtagAvg> {

    /** Same aggregate; reports the growth of its accumulator's annotation (8 B per variable) to LiveState. Used in live_memory mode only. */
    public static class Live extends AvgAggregate {
      public Live(int channels) {
        super(channels);
      }

      @Override
      public Acc add(HashtagAvg value, Acc acc) {
        long before = acc.hm.numVars();
        super.add(value, acc);
        LiveState.grow(8L * (acc.hm.numVars() - before));
        return acc;
      }
    }

    private final int channels;

    public AvgAggregate(int channels) {
      this.channels = channels;
    }

    @Override
    public Acc createAccumulator() {
      return new Acc(channels);
    }

    @Override
    public Acc add(HashtagAvg value, Acc acc) {
      if (acc.hashtag == null) acc.hashtag = value.hashtag;
      acc.sum += value.value1;
      acc.count++;
      acc.hm.mul(value.hm);
      return acc;
    }

    @Override
    public HashtagAvg getResult(Acc acc) {
      double avg = acc.count > 0 ? acc.sum / acc.count : 0;
      // Placeholder: StampDerivedTime sets the window end after the operator.
      return new HashtagAvg(acc.hashtag, 0, avg, 0, acc.hm.toMeta());
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
      MonomialBuilder hm;

      public Acc() {}

      public Acc(int channels) {
        this.hm = new MonomialBuilder();
      }
    }
  }

  /**
   * Joins left (all-tweets) and right (verified) aggregated averages,
   * merging the how-much provenance counts from both sides.
   */
  public static class JoinAvgs
      implements JoinFunction<HashtagAvg, HashtagAvg, HashtagAvg> {
    @Override
    public HashtagAvg join(HashtagAvg left, HashtagAvg right) {
      return new HashtagAvg(
          left.hashtag,
          // Placeholder: StampDerivedTime sets the window end after the join.
          0,
          left.value1,
          right.value1,
          left.hm.times(right.hm));
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
