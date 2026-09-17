package io.palyvos.provenance.usecases.nexmark.provenance.queries;

import static io.palyvos.provenance.usecases.nexmark.NexmarkConstants.WINDOW_SIZE;

import io.palyvos.provenance.genealog.GenealogCardinalityDiscardSink;
import io.palyvos.provenance.genealog.GenealogIndexingSink;
import io.palyvos.provenance.genealog.GenealogMetadataProbe;
import io.palyvos.provenance.genealog.GenealogTraversalSink;
import io.palyvos.provenance.genealog.GenealogJoinHelper;
import io.palyvos.provenance.usecases.nexmark.provenance.NexmarkCountAggregateFunction;
import io.palyvos.provenance.usecases.nexmark.provenance.NexmarkEventTupleGL;
import io.palyvos.provenance.usecases.nexmark.provenance.NexmarkPersonCountTupleGL;
import io.palyvos.provenance.usecases.nexmark.provenance.NexmarkSourceGL;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.ProvenanceActivator;
import io.palyvos.provenance.util.TimestampConverter;
import java.util.Arrays;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;

/**
 * Nexmark Q8 Monitor New Users workflow (nexmark_1) with GeneaLog provenance:
 * per 12-hour tumbling window, the users who registered and opened at least
 * one auction within the window, with their auction count.
 *
 * <pre>
 *   Persons  ── Map ──┐
 *                      Join(person.id = auction.seller, 12h tumbling)
 *   Auctions ── Map ──┘        │
 *                      Filter(auction.ts >= person.ts) ── Agg(count/person, 12h) ── Sink
 * </pre>
 *
 * Output: (personId, nAuctions)
 */
public class NexmarkMonitorNewUsers {

  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final TimestampConverter timestampConverter = ts -> ts;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setMaxParallelism(settings.maxParallelism());

    FlinkSerializerActivator.PROVENANCE_OPTIMIZED.activate(env, settings);
    final boolean metadataVolume = GenealogMetadataProbe.enabled(settings);

    DataStream<NexmarkEventTupleGL> persons =
        source(env, settings, true, 0, metadataVolume);
    DataStream<NexmarkEventTupleGL> auctions =
        source(env, settings, false, 1, metadataVolume);

    // Join persons with the auctions they opened in the same 12h window,
    // keeping only auctions opened at or after the registration.
    DataStream<NexmarkPersonCountTupleGL> newUserAuctions = persons
        .join(auctions)
        .where(NexmarkEventTupleGL::getPersonId)
        .equalTo(NexmarkEventTupleGL::getPersonId)
        .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
        .apply(new NewUserJoin());
    newUserAuctions = GenealogMetadataProbe.attach(newUserAuctions, metadataVolume, "JOIN");

    // Count the auctions of each new user inside the window
    DataStream<NexmarkPersonCountTupleGL> counted = newUserAuctions
        .keyBy(NexmarkPersonCountTupleGL::getPersonId)
        .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
        .aggregate(new NexmarkCountAggregateFunction(settings.aggregateStrategySupplier()))
        .name("AGG-COUNT");
    counted = GenealogMetadataProbe.attach(counted, metadataVolume, "AGG-COUNT");

    if (metadataVolume) {
      counted
          .addSink(new GenealogCardinalityDiscardSink<NexmarkPersonCountTupleGL>())
          .setParallelism(1)
          .name("SINK");
    } else if (settings.indexBenchDir() != null) {
      // Temporal-query bench: store outputs in the GenealogIndex and answer
      // time-range queries with the provenance-graph BFS at end of stream.
      counted
          .addSink(new GenealogIndexingSink<>(settings,
              2 * WINDOW_SIZE.toMilliseconds()))
          .setParallelism(1)
          .name("SINK-INDEX");
    } else if (settings.traversalBenchDir() != null) {
      // Per-tuple provenance bench: keep outputs in memory (no file I/O),
      // sample some at end of stream and time their graph traversal.
      counted
          .addSink(new GenealogTraversalSink<>(settings,
              (t, out) -> {
                out.writeLong(t.getPersonId());
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
              Arrays.asList(ProvenanceActivator.convert(counted)),
              Arrays.asList("JOINED"),
              settings,
              2 * WINDOW_SIZE.toMilliseconds(),
              timestampConverter);
    }

    GenealogMetadataProbe.writeIfEnabled(env.execute("NexmarkMonitorNewUsers"),
        settings, "nexmark_1");
  }

  private static DataStream<NexmarkEventTupleGL> source(
      StreamExecutionEnvironment env, ExperimentSettings settings,
      boolean personStream, int componentIndex, boolean metadataVolume) {
    DataStream<NexmarkEventTupleGL> sourced = env
        .addSource(new NexmarkSourceGL(settings, personStream))
        .name(personStream ? "SOURCE-PERSONS" : "SOURCE-AUCTIONS")
        .assignTimestampsAndWatermarks(
            new AssignerWithPunctuatedWatermarks<NexmarkEventTupleGL>() {
              @Override
              public long extractTimestamp(NexmarkEventTupleGL t, long prev) {
                return t.getTimestamp();
              }
              @Override
              public Watermark checkAndGetNextWatermark(NexmarkEventTupleGL t, long ts) {
                return new Watermark(ts);
              }
            })
        .setParallelism(1)
        .map(settings.genealogActivator()
            .uidAssigner(componentIndex, settings.maxParallelism()))
        .returns(NexmarkEventTupleGL.class)
        .setParallelism(env.getParallelism());
    return GenealogMetadataProbe.attach(sourced, metadataVolume,
        personStream ? "SOURCE-PERSONS" : "SOURCE-AUCTIONS");
  }

  /**
   * Emits one pair per (person, auction) of the window where the auction was
   * opened at or after the registration, annotated with GeneaLog JOIN
   * provenance.
   */
  public static class NewUserJoin
      implements FlatJoinFunction<NexmarkEventTupleGL, NexmarkEventTupleGL, NexmarkPersonCountTupleGL> {

    @Override
    public void join(NexmarkEventTupleGL person, NexmarkEventTupleGL auction,
        Collector<NexmarkPersonCountTupleGL> out) {
      if (auction.getTimestamp() >= person.getTimestamp()) {
        NexmarkPersonCountTupleGL result = new NexmarkPersonCountTupleGL(
            person.getPersonId(),
            Math.max(person.getTimestamp(), auction.getTimestamp()),
            Math.max(person.getStimulus(), auction.getStimulus()),
            1,
            0);
        GenealogJoinHelper.INSTANCE.annotateResult(person, auction, result);
        out.collect(result);
      }
    }
  }
}
