package io.palyvos.provenance.usecases.taxi.provenance.queries;

import static io.palyvos.provenance.usecases.taxi.TaxiConstants.WINDOW_SIZE;
import static io.palyvos.provenance.usecases.taxi.TaxiConstants.WINDOW_SLIDE;

import io.palyvos.provenance.usecases.taxi.TaxiBoroughs;

import io.palyvos.provenance.genealog.GenealogCardinalityDiscardSink;
import io.palyvos.provenance.genealog.GenealogIndexingSink;
import io.palyvos.provenance.genealog.GenealogMetadataProbe;
import io.palyvos.provenance.genealog.GenealogTraversalSink;
import io.palyvos.provenance.genealog.GenealogJoinHelper;
import io.palyvos.provenance.genealog.GenealogLiveSink;
import io.palyvos.provenance.usecases.taxi.provenance.RideToTaxiCountMap;
import io.palyvos.provenance.usecases.taxi.provenance.TaxiCountAggregateFunction;
import io.palyvos.provenance.usecases.taxi.provenance.TaxiCountTupleGL;
import io.palyvos.provenance.usecases.taxi.provenance.TaxiRideTupleGL;
import io.palyvos.provenance.usecases.taxi.provenance.TaxiSourceGL;
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
 * Taxi Passenger Join workflow (taxi_1, the paper's introduction figure) with
 * GeneaLog provenance: per 1-hour window, the taxis with exactly one
 * 1-passenger ride and at least two rides with more than two passengers.
 * The stream is the multiplex of two virtual sources, Manhattan pickups (S1)
 * and Queens pickups (S2); rides outside both boroughs are dropped before
 * entering the dataflow ({@link TaxiBoroughs}).
 *
 * <pre>
 *   S1 (Manhattan) --+    +-- Filter(pax=1) -- Map(M) -- Count/taxi(1h) -- Filter(c=1)  --+
 *                    Mux -+                                                                Join(taxiID) -- Sink
 *   S2 (Queens)    --+    +-- Filter(pax>2) -- Map(M) -- Count/taxi(1h) -- Filter(c>=2) --+
 * </pre>
 *
 * Output: (taxiId, soloCount, crowdedCount)
 */
public class TaxiPassengerJoin {

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

    SingleOutputStreamOperator<TaxiRideTupleGL> sourced =
        TaxiSourceGL.read(env, cfg, settings)
            // Virtual-source multiplex: keep Manhattan (S1) / Queens (S2)
            // pickups only, before any provenance meta-data is attached.
            .filter(r -> TaxiBoroughs.borough(r.getZone()) >= 0)
            .name("MUX-BOROUGH")
            .setParallelism(1)
            .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
            .returns(TaxiRideTupleGL.class)
            .setParallelism(env.getParallelism());
    DataStream<TaxiRideTupleGL> sourceStream =
        GenealogMetadataProbe.attach(sourced, metadataVolume, "SOURCE");

    // --- Left path (PL): solo (1-passenger) rides ---
    DataStream<TaxiRideTupleGL> soloRides = sourceStream
        .filter(r -> r.getPassengers() == 1)
        .name("FILTER-SOLO");
    soloRides = GenealogMetadataProbe.attach(soloRides, metadataVolume, "FILTER-SOLO");
    DataStream<TaxiCountTupleGL> soloMapped = soloRides
        .map(new RideToTaxiCountMap())
        .name("MAP-SOLO");
    soloMapped = GenealogMetadataProbe.attach(soloMapped, metadataVolume, "MAP-SOLO");
    DataStream<TaxiCountTupleGL> soloAgg = soloMapped
        .keyBy(TaxiCountTupleGL::getTaxiId)
        .window(SlidingEventTimeWindows.of(WINDOW_SIZE, WINDOW_SLIDE))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "AGG-SOLO", 0, WINDOW_SLIDE, LiveState.constant(GenealogLiveSink.AGGREGATE_WINDOW_BYTES), null,
            GenealogLiveSink.TUPLE_SIZER, null))
        .aggregate(new TaxiCountAggregateFunction(settings.aggregateStrategySupplier()))
        .name("AGG-SOLO")
        .process(new StampDerivedTime<TaxiCountTupleGL>((t, ts) -> t.setTimestamp(ts)))
        .returns(TaxiCountTupleGL.class)
        .name("STAMP-AGG-SOLO");
    soloAgg = GenealogMetadataProbe.attach(soloAgg, metadataVolume, "AGG-SOLO");
    DataStream<TaxiCountTupleGL> solo = soloAgg
        .filter(c -> c.getValue1() == 1)
        .name("FILTER-COUNT-SOLO");
    solo = GenealogMetadataProbe.attach(solo, metadataVolume, "FILTER-COUNT-SOLO");

    // --- Right path (PR): crowded (more than 2 passengers) rides ---
    DataStream<TaxiRideTupleGL> crowdedRides = sourceStream
        .filter(r -> r.getPassengers() > 2)
        .name("FILTER-CROWDED");
    crowdedRides = GenealogMetadataProbe.attach(crowdedRides, metadataVolume, "FILTER-CROWDED");
    DataStream<TaxiCountTupleGL> crowdedMapped = crowdedRides
        .map(new RideToTaxiCountMap())
        .name("MAP-CROWDED");
    crowdedMapped = GenealogMetadataProbe.attach(crowdedMapped, metadataVolume, "MAP-CROWDED");
    DataStream<TaxiCountTupleGL> crowdedAgg = crowdedMapped
        .keyBy(TaxiCountTupleGL::getTaxiId)
        .window(SlidingEventTimeWindows.of(WINDOW_SIZE, WINDOW_SLIDE))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "AGG-CROWDED", 0, WINDOW_SLIDE, LiveState.constant(GenealogLiveSink.AGGREGATE_WINDOW_BYTES), null,
            GenealogLiveSink.TUPLE_SIZER, null))
        .aggregate(new TaxiCountAggregateFunction(settings.aggregateStrategySupplier()))
        .name("AGG-CROWDED")
        .process(new StampDerivedTime<TaxiCountTupleGL>((t, ts) -> t.setTimestamp(ts)))
        .returns(TaxiCountTupleGL.class)
        .name("STAMP-AGG-CROWDED");
    crowdedAgg = GenealogMetadataProbe.attach(crowdedAgg, metadataVolume, "AGG-CROWDED");
    DataStream<TaxiCountTupleGL> crowded = crowdedAgg
        .filter(c -> c.getValue1() >= 2)
        .name("FILTER-COUNT-CROWDED");
    crowded = GenealogMetadataProbe.attach(crowded, metadataVolume, "FILTER-COUNT-CROWDED");

    // --- Join on taxiId ---
    DataStream<TaxiCountTupleGL> joined = solo
        .join(crowded)
        .where(TaxiCountTupleGL::getTaxiId)
        .equalTo(TaxiCountTupleGL::getTaxiId)
        .window(SlidingEventTimeWindows.of(WINDOW_SIZE, WINDOW_SLIDE))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "JOIN", 1, WINDOW_SLIDE, null, GenealogLiveSink.TUPLE_ONLY, null, GenealogLiveSink.REMOTE_SET))
        .apply(new PassengerJoinFunction())
        .process(new StampDerivedTime<TaxiCountTupleGL>((t, ts) -> t.setTimestamp(ts)))
        .returns(TaxiCountTupleGL.class)
        .name("STAMP-JOIN");
    joined = GenealogMetadataProbe.attach(joined, metadataVolume, "JOIN");

    if (live) {
      joined
          .addSink(new GenealogLiveSink<TaxiCountTupleGL>(settings))
          .name("SINK");
    } else if (metadataVolume) {
      joined
          .addSink(new GenealogCardinalityDiscardSink<TaxiCountTupleGL>())
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
                out.writeLong(t.getTaxiId());
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
              2 * WINDOW_SIZE.toMilliseconds(),
              timestampConverter);
    }

    JobExecutionResult result = env.execute("TaxiPassengerJoin");
    if (live)
      LiveState.write(result.getAllAccumulatorResults(), new File(settings.metadataVolumeDir()), "taxi_1", "genealog");
    else
      GenealogMetadataProbe.writeIfEnabled(result, settings, "taxi_1");
  }

  /**
   * Combines the solo and crowded per-taxi counts into a single output tuple,
   * annotating it with GeneaLog JOIN provenance.
   */
  public static class PassengerJoinFunction
      implements JoinFunction<TaxiCountTupleGL, TaxiCountTupleGL, TaxiCountTupleGL> {

    @Override
    public TaxiCountTupleGL join(TaxiCountTupleGL solo, TaxiCountTupleGL crowded) {
      TaxiCountTupleGL result = new TaxiCountTupleGL(
          solo.getTaxiId(),
          // Placeholder: StampDerivedTime sets the window end after the join.
          0,
          Math.max(solo.getStimulus(), crowded.getStimulus()),
          solo.getValue1(),
          crowded.getValue1());
      GenealogJoinHelper.INSTANCE.annotateResult(solo, crowded, result);
      return result;
    }
  }
}
