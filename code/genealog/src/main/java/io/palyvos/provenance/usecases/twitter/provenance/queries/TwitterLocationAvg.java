package io.palyvos.provenance.usecases.twitter.provenance.queries;

import io.palyvos.provenance.genealog.GenealogCardinalityDiscardSink;
import io.palyvos.provenance.genealog.GenealogIndexingSink;
import io.palyvos.provenance.genealog.GenealogLiveSink;
import io.palyvos.provenance.genealog.GenealogMetadataProbe;
import io.palyvos.provenance.genealog.GenealogTraversalSink;
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
import java.io.File;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Twitter Location Average workflow with GeneaLog provenance.
 *
 * Linear pipeline: Source -> FlatMap(hashtag,score) -> Agg(6h avg) -> Sink
 *
 * Uses the same source/tuple as twitter_1 (reads twitter.txt with 8 fields).
 */
public class TwitterLocationAvg {

  private static final Time AGG_WINDOW = Time.hours(6);
  private static final Time AGG_SLIDE = Time.hours(1);

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

    DataStream<TwitterHashtagTupleGL> mapped = sourceStream
        .flatMap(new HashtagScoreFlatMap())
        .name("MAP");
    mapped = GenealogMetadataProbe.attach(mapped, metadataVolume, "MAP");
    DataStream<TwitterHashtagTupleGL> aggregated = mapped
        .keyBy(TwitterHashtagTupleGL::getHashtag)
        .window(SlidingEventTimeWindows.of(AGG_WINDOW, AGG_SLIDE))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "AGG-6H", 0, AGG_SLIDE, LiveState.constant(GenealogLiveSink.AGGREGATE_WINDOW_BYTES), null,
            GenealogLiveSink.TUPLE_SIZER, null))
        .aggregate(new HashtagAvgAggregateFunction(settings.aggregateStrategySupplier()))
        .name("AGG-6H")
        .process(new StampDerivedTime<TwitterHashtagTupleGL>((t, ts) -> t.setTimestamp(ts)))
        .returns(TwitterHashtagTupleGL.class)
        .name("STAMP-AGG-6H");
    aggregated = GenealogMetadataProbe.attach(aggregated, metadataVolume, "AGG-6H");

    if (live) {
      aggregated
          .addSink(new GenealogLiveSink<TwitterHashtagTupleGL>(settings))
          .name("SINK");
    } else if (metadataVolume) {
      aggregated
          .addSink(new GenealogCardinalityDiscardSink<TwitterHashtagTupleGL>())
          .setParallelism(1)
          .name("SINK");
    } else if (settings.indexBenchDir() != null) {
      // Temporal-query bench: store outputs in the GenealogIndex and answer
      // time-range queries with the provenance-graph BFS at end of stream.
      // Watermark-driven Tier 1 -> ordered append (as in CAPS), one shard per sink subtask.
      GenealogIndexingSink.attach(aggregated, new GenealogIndexingSink<>(settings), "SINK-INDEX");
    } else if (settings.traversalBenchDir() != null) {
      // Per-tuple provenance bench: keep outputs in memory (no file I/O),
      // sample some at end of stream and time their graph traversal.
      aggregated
          .addSink(new GenealogTraversalSink<>(settings,
              (t, out) -> {
                temporalindex.LogicalSampler.writeString(out, t.getHashtag());
                out.writeLong(t.getTimestamp());
                temporalindex.LogicalSampler.writeDoubleBits(out, t.getValue1());
              }))
          .setParallelism(1)
          .name("SINK-TRAVERSAL");
    } else {
      settings
          .genealogActivator()
          .activate(
              Arrays.asList(ProvenanceActivator.convert(aggregated)),
              Arrays.asList("AGG"),
              settings,
              AGG_WINDOW.toMilliseconds(),
              timestampConverter);
    }

    JobExecutionResult result = env.execute("TwitterLocationAvg");
    if (live)
      LiveState.write(result.getAllAccumulatorResults(), new File(settings.metadataVolumeDir()), "twitter_2", "genealog");
    else
      GenealogMetadataProbe.writeIfEnabled(result, settings, "twitter_2");
  }
}
