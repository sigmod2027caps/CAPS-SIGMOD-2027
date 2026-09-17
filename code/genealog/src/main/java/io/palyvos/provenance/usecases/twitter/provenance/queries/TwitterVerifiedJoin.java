package io.palyvos.provenance.usecases.twitter.provenance.queries;

import static io.palyvos.provenance.usecases.twitter.TwitterConstants.AGG_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.twitter.TwitterConstants.AGG_WINDOW_SLIDE;
import static io.palyvos.provenance.usecases.twitter.TwitterConstants.JOIN_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.twitter.TwitterConstants.JOIN_WINDOW_SLIDE;

import io.palyvos.provenance.genealog.GenealogCardinalityDiscardSink;
import io.palyvos.provenance.genealog.GenealogIndexingSink;
import io.palyvos.provenance.genealog.GenealogMetadataProbe;
import io.palyvos.provenance.genealog.GenealogTraversalSink;
import io.palyvos.provenance.genealog.GenealogJoinHelper;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.usecases.twitter.provenance.HashtagAvgAggregateFunction;
import io.palyvos.provenance.usecases.twitter.provenance.HashtagScoreFlatMap;
import io.palyvos.provenance.usecases.twitter.provenance.TwitterHashtagTupleGL;
import io.palyvos.provenance.usecases.twitter.provenance.TwitterSourceGL;
import io.palyvos.provenance.usecases.twitter.provenance.TwitterTweetTupleGL;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.ProvenanceActivator;
import io.palyvos.provenance.util.TimestampConverter;
import java.util.Arrays;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

/**
 * Twitter Verified Join workflow with GeneaLog provenance.
 *
 * <pre>
 *   Source ─── Mu ──┬── FlatMap(M) ── Agg(A, 5s avg) ──────────────────┐
 *                   │                                                    Join(J, 30min) ── Sink
 *                   └── Filter(F, verified) ── FlatMap(M) ── Agg(A) ───┘
 * </pre>
 *
 * Output: (hashtag, avgScore_all, avgScore_verified)
 */
public class TwitterVerifiedJoin {

  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final TimestampConverter timestampConverter = ts -> ts;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setMaxParallelism(settings.maxParallelism());

    FlinkSerializerActivator.PROVENANCE_OPTIMIZED.activate(env, settings);
    final boolean metadataVolume = GenealogMetadataProbe.enabled(settings);

    SingleOutputStreamOperator<TwitterTweetTupleGL> sourced =
        env.addSource(new TwitterSourceGL(settings))
            .name("SOURCE")
            .assignTimestampsAndWatermarks(
                new AssignerWithPunctuatedWatermarks<TwitterTweetTupleGL>() {
                  @Override
                  public long extractTimestamp(TwitterTweetTupleGL t, long prev) {
                    return t.getTimestamp();
                  }
                  @Override
                  public Watermark checkAndGetNextWatermark(TwitterTweetTupleGL t, long ts) {
                    return new Watermark(ts);
                  }
                })
            .setParallelism(1)
            .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
            .returns(TwitterTweetTupleGL.class)
            .setParallelism(env.getParallelism());
    DataStream<TwitterTweetTupleGL> sourceStream =
        GenealogMetadataProbe.attach(sourced, metadataVolume, "SOURCE");

    // --- Left path (PL): all tweets ---
    DataStream<TwitterHashtagTupleGL> leftMapped = sourceStream
        .flatMap(new HashtagScoreFlatMap())
        .name("MAP-ALL");
    leftMapped = GenealogMetadataProbe.attach(leftMapped, metadataVolume, "MAP-ALL");
    DataStream<TwitterHashtagTupleGL> leftAgg = leftMapped
        .keyBy(TwitterHashtagTupleGL::getHashtag)
        .window(SlidingEventTimeWindows.of(AGG_WINDOW_SIZE, AGG_WINDOW_SLIDE))
        .aggregate(new HashtagAvgAggregateFunction(settings.aggregateStrategySupplier()))
        .name("AGG-ALL");
    leftAgg = GenealogMetadataProbe.attach(leftAgg, metadataVolume, "AGG-ALL");

    // --- Right path (PR): verified tweets only ---
    DataStream<TwitterTweetTupleGL> verified = sourceStream
        .filter(TwitterTweetTupleGL::isVerified)
        .name("FILTER-VERIFIED");
    verified = GenealogMetadataProbe.attach(verified, metadataVolume, "FILTER-VERIFIED");
    DataStream<TwitterHashtagTupleGL> rightMapped = verified
        .flatMap(new HashtagScoreFlatMap())
        .name("MAP-VERIFIED");
    rightMapped = GenealogMetadataProbe.attach(rightMapped, metadataVolume, "MAP-VERIFIED");
    DataStream<TwitterHashtagTupleGL> rightAgg = rightMapped
        .keyBy(TwitterHashtagTupleGL::getHashtag)
        .window(SlidingEventTimeWindows.of(AGG_WINDOW_SIZE, AGG_WINDOW_SLIDE))
        .aggregate(new HashtagAvgAggregateFunction(settings.aggregateStrategySupplier()))
        .name("AGG-VERIFIED");
    rightAgg = GenealogMetadataProbe.attach(rightAgg, metadataVolume, "AGG-VERIFIED");

    // --- Join on hashtag ---
    DataStream<TwitterHashtagTupleGL> joined = leftAgg
        .join(rightAgg)
        .where(TwitterHashtagTupleGL::getHashtag)
        .equalTo(TwitterHashtagTupleGL::getHashtag)
        .window(SlidingEventTimeWindows.of(JOIN_WINDOW_SIZE, JOIN_WINDOW_SLIDE))
        .apply(new VerifiedJoinFunction());
    joined = GenealogMetadataProbe.attach(joined, metadataVolume, "JOIN");

    if (metadataVolume) {
      joined
          .addSink(new GenealogCardinalityDiscardSink<TwitterHashtagTupleGL>())
          .setParallelism(1)
          .name("SINK");
    } else if (settings.indexBenchDir() != null) {
      // Temporal-query bench: store outputs in the GenealogIndex and answer
      // time-range queries with the provenance-graph BFS at end of stream.
      joined
          .addSink(new GenealogIndexingSink<>(settings,
              2 * JOIN_WINDOW_SIZE.toMilliseconds()))
          .setParallelism(1)
          .name("SINK-INDEX");
    } else if (settings.traversalBenchDir() != null) {
      // Per-tuple provenance bench: keep outputs in memory (no file I/O),
      // sample some at end of stream and time their graph traversal.
      joined
          .addSink(new GenealogTraversalSink<>(settings,
              (t, out) -> {
                temporalindex.LogicalSampler.writeString(out, t.getHashtag());
                out.writeLong(t.getTimestamp());
                temporalindex.LogicalSampler.writeDoubleBits(out, t.getValue1());
                temporalindex.LogicalSampler.writeDoubleBits(out, t.getValue2());
              }))
          .setParallelism(1)
          .name("SINK-TRAVERSAL");
    } else {
      settings
          .genealogActivator()
          .activate(
              Arrays.asList(ProvenanceActivator.convert(joined)),
              Arrays.asList("JOINED"),
              settings,
              AGG_WINDOW_SIZE.toMilliseconds() + JOIN_WINDOW_SIZE.toMilliseconds(),
              timestampConverter);
    }

    GenealogMetadataProbe.writeIfEnabled(env.execute("TwitterVerifiedJoin"),
        settings, "twitter_1");
  }

  /**
   * Combines the left (all tweets) and right (verified tweets) aggregated averages
   * into a single output tuple, annotating it with GeneaLog JOIN provenance.
   */
  public static class VerifiedJoinFunction
      implements JoinFunction<TwitterHashtagTupleGL, TwitterHashtagTupleGL, TwitterHashtagTupleGL> {

    @Override
    public TwitterHashtagTupleGL join(TwitterHashtagTupleGL left, TwitterHashtagTupleGL right) {
      TwitterHashtagTupleGL result = new TwitterHashtagTupleGL(
          left.getHashtag(),
          Math.max(left.getTimestamp(), right.getTimestamp()),
          Math.max(left.getStimulus(), right.getStimulus()),
          left.getValue1(),
          right.getValue1());
      GenealogJoinHelper.INSTANCE.annotateResult(left, right, result);
      return result;
    }
  }
}
