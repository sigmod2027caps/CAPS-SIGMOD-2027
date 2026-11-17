package io.palyvos.provenance.usecases.taxi.provenance.queries;

import static io.palyvos.provenance.usecases.taxi.TaxiConstants.WINDOW_SIZE;
import static io.palyvos.provenance.usecases.taxi.TaxiConstants.WINDOW_SLIDE;

import io.palyvos.provenance.genealog.GenealogCardinalityDiscardSink;
import io.palyvos.provenance.genealog.GenealogIndexingSink;
import io.palyvos.provenance.genealog.GenealogMetadataProbe;
import io.palyvos.provenance.genealog.GenealogTraversalSink;
import io.palyvos.provenance.genealog.GenealogJoinHelper;
import io.palyvos.provenance.genealog.GenealogLiveSink;
import io.palyvos.provenance.usecases.taxi.provenance.RideToZoneValueMap;
import io.palyvos.provenance.usecases.taxi.provenance.TaxiRideTupleGL;
import io.palyvos.provenance.usecases.taxi.provenance.TaxiSourceGL;
import io.palyvos.provenance.usecases.taxi.provenance.TaxiZoneTupleGL;
import io.palyvos.provenance.usecases.taxi.provenance.ZoneAvgAggregateFunction;
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
 * Taxi Payment Average workflow (taxi_2) with GeneaLog provenance: card and
 * cash rides act as two virtual sources; the card branch computes the average
 * tip and the cash branch the average fare, per pickup zone and 1-hour window,
 * and the two averages are joined on the zone.
 *
 * <pre>
 *   Source --+-- Filter(CRD) -- Map(M) -- AvgTip/zone(1h)  --+
 *            |                                                Join(zone) -- Sink
 *            +-- Filter(CSH) -- Map(M) -- AvgFare/zone(1h) --+
 * </pre>
 *
 * Output: (zone, avgCardTip, avgCashFare)
 */
public class TaxiPaymentAvg {

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
            .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
            .returns(TaxiRideTupleGL.class)
            .setParallelism(env.getParallelism());
    DataStream<TaxiRideTupleGL> sourceStream =
        GenealogMetadataProbe.attach(sourced, metadataVolume, "SOURCE");

    // --- Card virtual source (SC): average tip per zone ---
    DataStream<TaxiRideTupleGL> cardRides = sourceStream
        .filter(TaxiRideTupleGL::isCard)
        .name("FILTER-CARD");
    cardRides = GenealogMetadataProbe.attach(cardRides, metadataVolume, "FILTER-CARD");
    DataStream<TaxiZoneTupleGL> cardMapped = cardRides
        .map(new RideToZoneValueMap(true))
        .name("MAP-CARD");
    cardMapped = GenealogMetadataProbe.attach(cardMapped, metadataVolume, "MAP-CARD");
    DataStream<TaxiZoneTupleGL> card = cardMapped
        .keyBy(TaxiZoneTupleGL::getZone)
        .window(SlidingEventTimeWindows.of(WINDOW_SIZE, WINDOW_SLIDE))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "AGG-CARD", 0, WINDOW_SLIDE, LiveState.constant(GenealogLiveSink.AGGREGATE_WINDOW_BYTES), null,
            GenealogLiveSink.TUPLE_SIZER, null))
        .aggregate(new ZoneAvgAggregateFunction(settings.aggregateStrategySupplier()))
        .name("AGG-CARD")
        .process(new StampDerivedTime<TaxiZoneTupleGL>((t, ts) -> t.setTimestamp(ts)))
        .returns(TaxiZoneTupleGL.class)
        .name("STAMP-AGG-CARD");
    card = GenealogMetadataProbe.attach(card, metadataVolume, "AGG-CARD");

    // --- Cash virtual source (SK): average fare per zone ---
    DataStream<TaxiRideTupleGL> cashRides = sourceStream
        .filter(TaxiRideTupleGL::isCash)
        .name("FILTER-CASH");
    cashRides = GenealogMetadataProbe.attach(cashRides, metadataVolume, "FILTER-CASH");
    DataStream<TaxiZoneTupleGL> cashMapped = cashRides
        .map(new RideToZoneValueMap(false))
        .name("MAP-CASH");
    cashMapped = GenealogMetadataProbe.attach(cashMapped, metadataVolume, "MAP-CASH");
    DataStream<TaxiZoneTupleGL> cash = cashMapped
        .keyBy(TaxiZoneTupleGL::getZone)
        .window(SlidingEventTimeWindows.of(WINDOW_SIZE, WINDOW_SLIDE))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "AGG-CASH", 0, WINDOW_SLIDE, LiveState.constant(GenealogLiveSink.AGGREGATE_WINDOW_BYTES), null,
            GenealogLiveSink.TUPLE_SIZER, null))
        .aggregate(new ZoneAvgAggregateFunction(settings.aggregateStrategySupplier()))
        .name("AGG-CASH")
        .process(new StampDerivedTime<TaxiZoneTupleGL>((t, ts) -> t.setTimestamp(ts)))
        .returns(TaxiZoneTupleGL.class)
        .name("STAMP-AGG-CASH");
    cash = GenealogMetadataProbe.attach(cash, metadataVolume, "AGG-CASH");

    // --- Join on zone ---
    DataStream<TaxiZoneTupleGL> joined = card
        .join(cash)
        .where(TaxiZoneTupleGL::getZone)
        .equalTo(TaxiZoneTupleGL::getZone)
        .window(SlidingEventTimeWindows.of(WINDOW_SIZE, WINDOW_SLIDE))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "JOIN", 1, WINDOW_SLIDE, null, GenealogLiveSink.TUPLE_ONLY, null, GenealogLiveSink.REMOTE_SET))
        .apply(new PaymentJoinFunction())
        .process(new StampDerivedTime<TaxiZoneTupleGL>((t, ts) -> t.setTimestamp(ts)))
        .returns(TaxiZoneTupleGL.class)
        .name("STAMP-JOIN");
    joined = GenealogMetadataProbe.attach(joined, metadataVolume, "JOIN");

    if (live) {
      joined
          .addSink(new GenealogLiveSink<TaxiZoneTupleGL>(settings))
          .name("SINK");
    } else if (metadataVolume) {
      joined
          .addSink(new GenealogCardinalityDiscardSink<TaxiZoneTupleGL>())
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
                out.writeInt(t.getZone());
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

    JobExecutionResult result = env.execute("TaxiPaymentAvg");
    if (live)
      LiveState.write(result.getAllAccumulatorResults(), new File(settings.metadataVolumeDir()), "taxi_2", "genealog");
    else
      GenealogMetadataProbe.writeIfEnabled(result, settings, "taxi_2");
  }

  /**
   * Combines the card (avg tip) and cash (avg fare) zone averages into a
   * single output tuple, annotating it with GeneaLog JOIN provenance.
   */
  public static class PaymentJoinFunction
      implements JoinFunction<TaxiZoneTupleGL, TaxiZoneTupleGL, TaxiZoneTupleGL> {

    @Override
    public TaxiZoneTupleGL join(TaxiZoneTupleGL card, TaxiZoneTupleGL cash) {
      TaxiZoneTupleGL result = new TaxiZoneTupleGL(
          card.getZone(),
          // Placeholder: StampDerivedTime sets the window end after the join.
          0,
          Math.max(card.getStimulus(), cash.getStimulus()),
          card.getValue1(),
          cash.getValue1());
      GenealogJoinHelper.INSTANCE.annotateResult(card, cash, result);
      return result;
    }
  }
}
