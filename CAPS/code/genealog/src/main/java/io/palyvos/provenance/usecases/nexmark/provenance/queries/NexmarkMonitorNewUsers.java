package io.palyvos.provenance.usecases.nexmark.provenance.queries;

import static io.palyvos.provenance.usecases.nexmark.NexmarkConstants.WINDOW_SIZE;

import io.palyvos.provenance.genealog.GenealogCardinalityDiscardSink;
import io.palyvos.provenance.genealog.GenealogIndexingSink;
import io.palyvos.provenance.genealog.GenealogMetadataProbe;
import io.palyvos.provenance.genealog.GenealogTraversalSink;
import io.palyvos.provenance.genealog.GenealogJoinHelper;
import io.palyvos.provenance.genealog.GenealogLiveSink;
import io.palyvos.provenance.usecases.nexmark.provenance.NexmarkCountAggregateFunction;
import io.palyvos.provenance.usecases.nexmark.provenance.NexmarkEventTupleGL;
import io.palyvos.provenance.usecases.nexmark.provenance.NexmarkPersonCountTupleGL;
import io.palyvos.provenance.usecases.nexmark.provenance.NexmarkSourceGL;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.ProvenanceActivator;
import io.palyvos.provenance.util.TimestampConverter;
import temporalindex.Disorder;
import temporalindex.DisorderConfig;
import temporalindex.LiveState;
import temporalindex.StampDerivedTime;
import java.util.Arrays;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import java.io.File;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;

/**
 * Nexmark Q8 Monitor New Users workflow (nexmark_1) with GeneaLog provenance:
 * per 12-hour tumbling window, the users who registered and opened at least
 * one auction within the window, with their auction count.
 *
 * <pre>
 *   Persons  -- Map --+
 *                      Join(person.id = auction.seller, 12h tumbling)
 *   Auctions -- Map --+        |
 *                      Filter(auction.ts >= person.ts) -- Agg(count/person, 12h) -- Sink
 * </pre>
 *
 * Output: (personId, nAuctions)
 */
public class NexmarkMonitorNewUsers {

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

    DataStream<NexmarkEventTupleGL> persons =
        source(env, cfg, settings, true, 0, metadataVolume);
    DataStream<NexmarkEventTupleGL> auctions =
        source(env, cfg, settings, false, 1, metadataVolume);

    // Join persons with the auctions they opened in the same 12h window,
    // keeping only auctions opened at or after the registration.
    DataStream<NexmarkPersonCountTupleGL> newUserAuctions = persons
        .join(auctions)
        .where(NexmarkEventTupleGL::getPersonId)
        .equalTo(NexmarkEventTupleGL::getPersonId)
        .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "JOIN", 0, WINDOW_SIZE, null, GenealogLiveSink.TUPLE_ONLY, null, GenealogLiveSink.REMOTE_SET))
        .apply(new NewUserJoin())
        .process(new StampDerivedTime<NexmarkPersonCountTupleGL>((t, ts) -> t.setTimestamp(ts)))
        .returns(NexmarkPersonCountTupleGL.class)
        .name("STAMP-JOIN");
    newUserAuctions = GenealogMetadataProbe.attach(newUserAuctions, metadataVolume, "JOIN");

    // Count the auctions of each new user inside the window
    DataStream<NexmarkPersonCountTupleGL> counted = newUserAuctions
        .keyBy(NexmarkPersonCountTupleGL::getPersonId)
        .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "AGG-COUNT", 1, WINDOW_SIZE, LiveState.constant(GenealogLiveSink.AGGREGATE_WINDOW_BYTES), null,
            GenealogLiveSink.TUPLE_SIZER, null))
        .aggregate(new NexmarkCountAggregateFunction(settings.aggregateStrategySupplier()))
        .name("AGG-COUNT")
        .process(new StampDerivedTime<NexmarkPersonCountTupleGL>((t, ts) -> t.setTimestamp(ts)))
        .returns(NexmarkPersonCountTupleGL.class)
        .name("STAMP-AGG-COUNT");
    counted = GenealogMetadataProbe.attach(counted, metadataVolume, "AGG-COUNT");

    if (live) {
      counted
          .addSink(new GenealogLiveSink<NexmarkPersonCountTupleGL>(settings))
          .name("SINK");
    } else if (metadataVolume) {
      counted
          .addSink(new GenealogCardinalityDiscardSink<NexmarkPersonCountTupleGL>())
          .setParallelism(1)
          .name("SINK");
    } else if (settings.indexBenchDir() != null) {
      // Temporal-query bench: store outputs in the GenealogIndex and answer
      // time-range queries with the provenance-graph BFS at end of stream.
      // Watermark-driven Tier 1 -> ordered append (as in CAPS), one shard per sink subtask.
      GenealogIndexingSink.attach(counted, new GenealogIndexingSink<>(settings), "SINK-INDEX");
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

    JobExecutionResult result = env.execute("NexmarkMonitorNewUsers");
    if (live)
      LiveState.write(result.getAllAccumulatorResults(), new File(settings.metadataVolumeDir()), "nexmark_1", "genealog");
    else
      GenealogMetadataProbe.writeIfEnabled(result, settings, "nexmark_1");
  }

  private static DataStream<NexmarkEventTupleGL> source(
      StreamExecutionEnvironment env, DisorderConfig cfg, ExperimentSettings settings,
      boolean personStream, int componentIndex, boolean metadataVolume) {
    DataStream<NexmarkEventTupleGL> sourced = NexmarkSourceGL.read(env, cfg, settings, personStream)
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
            // Placeholder: StampDerivedTime sets the window end after the join.
            0,
            Math.max(person.getStimulus(), auction.getStimulus()),
            1,
            0);
        GenealogJoinHelper.INSTANCE.annotateResult(person, auction, result);
        out.collect(result);
      }
    }
  }
}
