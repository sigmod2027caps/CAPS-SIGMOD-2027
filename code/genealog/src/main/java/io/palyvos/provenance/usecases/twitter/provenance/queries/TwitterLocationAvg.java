package io.palyvos.provenance.usecases.twitter.provenance.queries;

import io.palyvos.provenance.genealog.GenealogCardinalityDiscardSink;
import io.palyvos.provenance.genealog.GenealogIndexingSink;
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
import java.util.Arrays;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Twitter Location Average workflow with GeneaLog provenance.
 *
 * Linear pipeline: Source → FlatMap(hashtag,score) → Agg(6h avg) → Sink
 *
 * Uses the same source/tuple as twitter_1 (reads twitter.txt with 8 fields).
 */
public class TwitterLocationAvg {

  private static final Time AGG_WINDOW = Time.hours(6);
  private static final Time AGG_SLIDE = Time.hours(1);

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

    DataStream<TwitterHashtagTupleGL> mapped = sourceStream
        .flatMap(new HashtagScoreFlatMap())
        .name("MAP");
    mapped = GenealogMetadataProbe.attach(mapped, metadataVolume, "MAP");
    DataStream<TwitterHashtagTupleGL> aggregated = mapped
        .keyBy(TwitterHashtagTupleGL::getHashtag)
        .window(SlidingEventTimeWindows.of(AGG_WINDOW, AGG_SLIDE))
        .aggregate(new HashtagAvgAggregateFunction(settings.aggregateStrategySupplier()))
        .name("AGG-6H");
    aggregated = GenealogMetadataProbe.attach(aggregated, metadataVolume, "AGG-6H");

    if (metadataVolume) {
      aggregated
          .addSink(new GenealogCardinalityDiscardSink<TwitterHashtagTupleGL>())
          .setParallelism(1)
          .name("SINK");
    } else if (settings.indexBenchDir() != null) {
      // Temporal-query bench: store outputs in the GenealogIndex and answer
      // time-range queries with the provenance-graph BFS at end of stream.
      aggregated
          .addSink(new GenealogIndexingSink<>(settings, 2 * AGG_WINDOW.toMilliseconds()))
          .setParallelism(1)
          .name("SINK-INDEX");
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

    GenealogMetadataProbe.writeIfEnabled(env.execute("TwitterLocationAvg"),
        settings, "twitter_2");
  }
}
