package io.palyvos.provenance.usecases.taxi.provenance.queries;

import static io.palyvos.provenance.usecases.taxi.TaxiConstants.WINDOW_SIZE;
import static io.palyvos.provenance.usecases.taxi.TaxiConstants.WINDOW_SLIDE;

import io.palyvos.provenance.genealog.GenealogCardinalityDiscardSink;
import io.palyvos.provenance.genealog.GenealogIndexingSink;
import io.palyvos.provenance.genealog.GenealogMetadataProbe;
import io.palyvos.provenance.genealog.GenealogTraversalSink;
import io.palyvos.provenance.genealog.GenealogJoinHelper;
import io.palyvos.provenance.usecases.taxi.provenance.RideToZoneValueMap;
import io.palyvos.provenance.usecases.taxi.provenance.TaxiRideTupleGL;
import io.palyvos.provenance.usecases.taxi.provenance.TaxiSourceGL;
import io.palyvos.provenance.usecases.taxi.provenance.TaxiZoneTupleGL;
import io.palyvos.provenance.usecases.taxi.provenance.ZoneAvgAggregateFunction;
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
 * Taxi Payment Average workflow (taxi_2) with GeneaLog provenance: card and
 * cash rides act as two virtual sources; the card branch computes the average
 * tip and the cash branch the average fare, per pickup zone and 1-hour window,
 * and the two averages are joined on the zone.
 *
 * <pre>
 *   Source ──┬── Filter(CRD) ── Map(M) ── AvgTip/zone(1h)  ──┐
 *            │                                                Join(zone) ── Sink
 *            └── Filter(CSH) ── Map(M) ── AvgFare/zone(1h) ──┘
 * </pre>
 *
 * Output: (zone, avgCardTip, avgCashFare)
 */
public class TaxiPaymentAvg {

  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final TimestampConverter timestampConverter = ts -> ts;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setMaxParallelism(settings.maxParallelism());

    FlinkSerializerActivator.PROVENANCE_OPTIMIZED.activate(env, settings);
    final boolean metadataVolume = GenealogMetadataProbe.enabled(settings);

    SingleOutputStreamOperator<TaxiRideTupleGL> sourced =
        env.addSource(new TaxiSourceGL(settings))
            .name("SOURCE")
            .assignTimestampsAndWatermarks(
                new AssignerWithPunctuatedWatermarks<TaxiRideTupleGL>() {
                  @Override
                  public long extractTimestamp(TaxiRideTupleGL t, long prev) {
                    return t.getTimestamp();
                  }
                  @Override
                  public Watermark checkAndGetNextWatermark(TaxiRideTupleGL t, long ts) {
                    return new Watermark(ts);
                  }
                })
            .setParallelism(1)
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
        .aggregate(new ZoneAvgAggregateFunction(settings.aggregateStrategySupplier()))
        .name("AGG-CARD");
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
        .aggregate(new ZoneAvgAggregateFunction(settings.aggregateStrategySupplier()))
        .name("AGG-CASH");
    cash = GenealogMetadataProbe.attach(cash, metadataVolume, "AGG-CASH");

    // --- Join on zone ---
    DataStream<TaxiZoneTupleGL> joined = card
        .join(cash)
        .where(TaxiZoneTupleGL::getZone)
        .equalTo(TaxiZoneTupleGL::getZone)
        .window(SlidingEventTimeWindows.of(WINDOW_SIZE, WINDOW_SLIDE))
        .apply(new PaymentJoinFunction());
    joined = GenealogMetadataProbe.attach(joined, metadataVolume, "JOIN");

    if (metadataVolume) {
      joined
          .addSink(new GenealogCardinalityDiscardSink<TaxiZoneTupleGL>())
          .setParallelism(1)
          .name("SINK");
    } else if (settings.indexBenchDir() != null) {
      // Temporal-query bench: store outputs in the GenealogIndex and answer
      // time-range queries with the provenance-graph BFS at end of stream.
      joined
          .addSink(new GenealogIndexingSink<>(settings,
              2 * WINDOW_SIZE.toMilliseconds()))
          .setParallelism(1)
          .name("SINK-INDEX");
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

    GenealogMetadataProbe.writeIfEnabled(env.execute("TaxiPaymentAvg"),
        settings, "taxi_2");
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
          Math.max(card.getTimestamp(), cash.getTimestamp()),
          Math.max(card.getStimulus(), cash.getStimulus()),
          card.getValue1(),
          cash.getValue1());
      GenealogJoinHelper.INSTANCE.annotateResult(card, cash, result);
      return result;
    }
  }
}
