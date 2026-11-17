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
import io.palyvos.provenance.genealog.GenealogLiveSink;
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
import temporalindex.Disorder;
import temporalindex.DisorderConfig;
import temporalindex.LiveState;
import temporalindex.StampDerivedTime;
import java.util.Arrays;
import org.apache.flink.api.common.functions.JoinFunction;
import java.io.File;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

/**
 * Twitter Verified Join workflow with GeneaLog provenance.
 *
 * <pre>
 *   Source --- Mu --+-- FlatMap(M) -- Agg(A, 30min/5min avg) ----------+
 *                   |                                                    Join(J, 30min/5min) -- Sink
 *                   +-- Filter(F, verified) -- FlatMap(M) -- Agg(A) ---+
 * </pre>
 *
 * Output: (hashtag, avgScore_all, avgScore_verified)
 */
public class TwitterVerifiedJoin {

  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    // The same environment, reader and watermark assigner as the CAPS, Green and no-provenance jobs (CAPS_* variables).
    final DisorderConfig cfg = DisorderConfig.fromEnv();
    final StreamExecutionEnvironment env = Disorder.environment(cfg);
    final TimestampConverter timestampConverter = ts -> ts;
    env.setMaxParallelism(settings.maxParallelism());

    FlinkSerializerActivator.PROVENANCE_OPTIMIZED.activate(env, settings);
    // live_memory: CAPS_LIVE_STATE=1 and --metadataVolumeDir <dir>; the dedicated sink publishes the live state.
    final boolean live = LiveState.on() && settings.metadataVolumeDir() != null;
    final boolean metadataVolume = GenealogMetadataProbe.enabled(settings) && !live;

    SingleOutputStreamOperator<TwitterTweetTupleGL> sourced =
        TwitterSourceGL.read(env, cfg, settings)
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
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "AGG-ALL", 0, AGG_WINDOW_SLIDE, LiveState.constant(GenealogLiveSink.AGGREGATE_WINDOW_BYTES), null,
            GenealogLiveSink.TUPLE_SIZER, null))
        .aggregate(new HashtagAvgAggregateFunction(settings.aggregateStrategySupplier()))
        .name("AGG-ALL")
        .process(new StampDerivedTime<TwitterHashtagTupleGL>((t, ts) -> t.setTimestamp(ts)))
        .returns(TwitterHashtagTupleGL.class)
        .name("STAMP-AGG-ALL");
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
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "AGG-VERIFIED", 0, AGG_WINDOW_SLIDE, LiveState.constant(GenealogLiveSink.AGGREGATE_WINDOW_BYTES), null,
            GenealogLiveSink.TUPLE_SIZER, null))
        .aggregate(new HashtagAvgAggregateFunction(settings.aggregateStrategySupplier()))
        .name("AGG-VERIFIED")
        .process(new StampDerivedTime<TwitterHashtagTupleGL>((t, ts) -> t.setTimestamp(ts)))
        .returns(TwitterHashtagTupleGL.class)
        .name("STAMP-AGG-VERIFIED");
    rightAgg = GenealogMetadataProbe.attach(rightAgg, metadataVolume, "AGG-VERIFIED");

    // --- Join on hashtag ---
    DataStream<TwitterHashtagTupleGL> joined = leftAgg
        .join(rightAgg)
        .where(TwitterHashtagTupleGL::getHashtag)
        .equalTo(TwitterHashtagTupleGL::getHashtag)
        .window(SlidingEventTimeWindows.of(JOIN_WINDOW_SIZE, JOIN_WINDOW_SLIDE))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "JOIN", 1, JOIN_WINDOW_SLIDE, null, GenealogLiveSink.TUPLE_ONLY, null, GenealogLiveSink.REMOTE_SET))
        .apply(new VerifiedJoinFunction())
        .process(new StampDerivedTime<TwitterHashtagTupleGL>((t, ts) -> t.setTimestamp(ts)))
        .returns(TwitterHashtagTupleGL.class)
        .name("STAMP-JOIN");
    joined = GenealogMetadataProbe.attach(joined, metadataVolume, "JOIN");

    if (live) {
      joined
          .addSink(new GenealogLiveSink<TwitterHashtagTupleGL>(settings))
          .name("SINK");
    } else if (metadataVolume) {
      joined
          .addSink(new GenealogCardinalityDiscardSink<TwitterHashtagTupleGL>())
          .setParallelism(1)
          .name("SINK");
    } else if (settings.indexBenchDir() != null) {
      // Temporal-query bench: store outputs in the GenealogIndex and answer
      // time-range queries with the provenance-graph BFS at end of stream.
      // Watermark-driven Tier 1 -> ordered append (as in CAPS), one shard per sink subtask.
      GenealogIndexingSink.attach(joined, new GenealogIndexingSink<>(settings), "SINK-INDEX");
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

    JobExecutionResult result = env.execute("TwitterVerifiedJoin");
    if (live)
      LiveState.write(result.getAllAccumulatorResults(), new File(settings.metadataVolumeDir()), "twitter_1", "genealog");
    else
      GenealogMetadataProbe.writeIfEnabled(result, settings, "twitter_1");
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
          // Placeholder: StampDerivedTime sets the window end after the join.
          0,
          Math.max(left.getStimulus(), right.getStimulus()),
          left.getValue1(),
          right.getValue1());
      GenealogJoinHelper.INSTANCE.annotateResult(left, right, result);
      return result;
    }
  }
}
