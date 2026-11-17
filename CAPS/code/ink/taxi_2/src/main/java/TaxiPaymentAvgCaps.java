import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import org.apache.flink.api.common.JobExecutionResult;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;

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
 * How-much provenance (virtual-source) Taxi Payment Average dataflow.
 *
 * Identical computation to the vanilla job, but each tuple carries an
 * {@link InkMeta} polynomial annotation (green_polynomial baseline in the paper)
 * whose how-much folding is one counter per virtual source: SC counts
 * the card (CRD) rides and SK the cash (CSH) rides that contributed to the
 * output (docs/workflows taxi_q4 virtual sources).
 *
 * <pre>
 *   Source --+-- Filter(CRD) -- Map(SC=1) -- AvgTip/zone(1h)  --+
 *            |                                                   Join(zone, merge counts) -- Sink
 *            +-- Filter(CSH) -- Map(SK=1) -- AvgFare/zone(1h) --+
 * </pre>
 *
 * Output per line: zone,timestamp,avgCardTip,avgCashFare,countSC,countSK
 */
public class TaxiPaymentAvgCaps {

  // How-much provenance channels: one per virtual source.
  public static final int SC = 0;  // card rides
  public static final int SK = 1;  // cash rides
  public static final int CHANNELS = 2;

  // Reorder slack for the online index. Every windowed operator stamps its output with
  // the right boundary of its window (StampDerivedTime), so at parallelism 1 the sink
  // input is already in timestamp order and the reorder buffer releases at once.
  public static final long INDEX_SLACK_MS = 0;

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: TaxiPaymentAvgCaps <inputFile> <outputFile>"
          + " [mode benchDir params...]   (mode: provenance | provenance_timer"
          + " | memory_volume, see InkBenchSinks / InkMetadataProbe)");
      System.exit(1);
    }
    final String inputFile = args[0];
    final String outputFile = args[1];
    final boolean memoryVolume = InkMetadataProbe.enabled(args, 2);
    final boolean live = LiveState.enabled(args, 2);
    final boolean benchMode = args.length > 2 && !memoryVolume && !live;

    // Same environment, source and watermark assigner as the CAPS and no-provenance jobs (CAPS_* variables, see
    // DisorderConfig): the defaults are the ordered run with a periodic watermark, no disorder.
    final DisorderConfig cfg = DisorderConfig.fromEnv();
    final StreamExecutionEnvironment env = Disorder.environment(cfg);

    DataStream<Ride> sourceStream = Disorder.source(env, cfg, "SOURCE", inputFile, new RideParser(),
        r -> r.timestamp, Ride.class);

    // Annotation bytes of one buffered tuple (live_memory): the monomials of its polynomial.
    final LiveState.Sizer<ZoneAvg> polyBytes = c -> c.hm.metadataBytes();

    InkQuerySink.MetaFn<ZoneAvg> meta =
        (InkQuerySink.MetaFn<ZoneAvg>) z -> z.hm;

    // Card virtual source (SC): average tip per zone
    DataStream<ZoneAvg> cardMapped = sourceStream
        .filter(r -> "CRD".equals(r.payment))
        .name("FILTER-CARD")
        .map(new ToZoneValue(true))
        .name("MAP-CARD");
    cardMapped = InkMetadataProbe.attach(cardMapped, memoryVolume, "MAP-CARD", meta);
    DataStream<ZoneAvg> card = cardMapped
        .keyBy(z -> z.zone)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "AGG-CARD", 0, Time.minutes(10), null, null))
        .aggregate(LiveState.on() ? new AvgAggregate.Live() : new AvgAggregate())
        .name("AGG-CARD")
        .process(new StampDerivedTime<ZoneAvg>((t, ts) -> t.timestamp = ts))
        .returns(ZoneAvg.class)
        .name("STAMP-AGG-CARD");
    card = InkMetadataProbe.attach(card, memoryVolume, "AGG-CARD", meta);

    // Cash virtual source (SK): average fare per zone
    DataStream<ZoneAvg> cashMapped = sourceStream
        .filter(r -> "CSH".equals(r.payment))
        .name("FILTER-CASH")
        .map(new ToZoneValue(false))
        .name("MAP-CASH");
    cashMapped = InkMetadataProbe.attach(cashMapped, memoryVolume, "MAP-CASH", meta);
    DataStream<ZoneAvg> cash = cashMapped
        .keyBy(z -> z.zone)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "AGG-CASH", 0, Time.minutes(10), null, null))
        .aggregate(LiveState.on() ? new AvgAggregate.Live() : new AvgAggregate())
        .name("AGG-CASH")
        .process(new StampDerivedTime<ZoneAvg>((t, ts) -> t.timestamp = ts))
        .returns(ZoneAvg.class)
        .name("STAMP-AGG-CASH");
    cash = InkMetadataProbe.attach(cash, memoryVolume, "AGG-CASH", meta);

    // Join on zone - merge counts from both virtual sources
    DataStream<ZoneAvg> joined = card
        .join(cash)
        .where(z -> z.zone)
        .equalTo(z -> z.zone)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "JOIN", 1, Time.minutes(10), null, LiveState.join(polyBytes, polyBytes)))
        .apply(new JoinAvgs())
        .process(new StampDerivedTime<ZoneAvg>((t, ts) -> t.timestamp = ts))
        .returns(ZoneAvg.class)
        .name("STAMP-JOIN");
    joined = InkMetadataProbe.attach(joined, memoryVolume, "JOIN", meta);

    if (live) {
      // live_memory: discard sink that publishes the live-state registry when the stream ends
      joined
          .addSink(new LiveStateSink<ZoneAvg>(x -> x.hm.metadataBytes()))
          .name("SINK");
    } else if (memoryVolume) {
      joined
          .addSink(new InkCardinalityDiscardSink<ZoneAvg>())
          .name("SINK");
    } else if (benchMode) {
      InkBenchSinks.attach(joined, InkBenchSinks.create(args, 2,
              (InkQuerySink.TsFn<ZoneAvg>) z -> z.timestamp,
              (InkQuerySink.MetaFn<ZoneAvg>) z -> z.hm,
              (temporalindex.SampleKeyFn<ZoneAvg>) (z, out) -> {
                out.writeInt(z.zone);
                out.writeLong(z.timestamp);
                temporalindex.LogicalSampler.writeDoubleBits(out, z.value1);
                temporalindex.LogicalSampler.writeDoubleBits(out, z.value2);
              },
              CHANNELS, INDEX_SLACK_MS),
          "SINK-INDEX");
    } else {
      joined
          .addSink(new FileSink(outputFile, CHANNELS))
          .setParallelism(1)
          .name("SINK");
    }

    JobExecutionResult result = env.execute("TaxiPaymentAvg-HowMuch");
    InkMetadataProbe.writeIfEnabled(result, args, 2, "taxi_2");
    LiveState.writeIfEnabled(result, args, 2, "taxi_2", "green_polynomial");
  }

  // Data types
  public static class Ride implements Serializable {
    public long taxiId;
    public long timestamp;
    public int passengers;
    public int zone;
    public String payment;
    public double fare;
    public double tip;
  }

  public static class ZoneAvg implements Serializable {
    public int zone;
    public long timestamp;
    public double value1;
    public double value2;
    public InkMeta hm;

    public ZoneAvg() {}

    public ZoneAvg(int zone, long timestamp, double value1, double value2,
                   InkMeta hm) {
      this.zone = zone;
      this.timestamp = timestamp;
      this.value1 = value1;
      this.value2 = value2;
      this.hm = hm;
    }

    @Override
    public String toString() {
      return zone + "," + timestamp + "," + value1 + "," + value2;
    }
  }

  // Source
  /**
   * Parses a line of taxis.txt: taxi_id,ts_ms,passengers,zone,payment,fare,tip. With CAPS_ZIPF the zone (the
   * key of both window aggregates and of the join) is replaced by a Zipf-distributed rank.
   */
  public static class RideParser extends DisorderSource.Parser<Ride> {
    private transient Zipf zipf;

    @Override
    public void open(DisorderConfig cfg) {
      zipf = cfg.zipf();
    }

    @Override
    public Ride parse(String line, long lineNo) {
      String[] parts = line.split(",", 7);
      Ride r = new Ride();
      r.taxiId = Long.parseLong(parts[0]);
      r.timestamp = Long.parseLong(parts[1]);
      r.passengers = Integer.parseInt(parts[2]);
      r.zone = zipf == null ? Integer.parseInt(parts[3]) : zipf.rank(lineNo, 0);
      r.payment = parts[4];
      r.fare = Double.parseDouble(parts[5]);
      r.tip = Double.parseDouble(parts[6]);
      return r;
    }
  }


  // Operators
  /**
   * Projects a ride to (zone, value): tip for the card branch, fare for cash.
   * Tags the tuple with the virtual-source how-much provenance count:
   * channel SC for card rides, SK for cash rides.
   */
  public static class ToZoneValue extends RichMapFunction<Ride, ZoneAvg> {
    private final boolean card;
    // Source-tuple variables of this operator instance, unique over its subtasks; see InkMeta, VarSource.
    private final VarSource vars = new VarSource(CHANNELS);

    public ToZoneValue(boolean card) {
      this.card = card;
    }

    @Override
    public void open(Configuration parameters) {
      vars.open(getRuntimeContext());
    }

    @Override
    public ZoneAvg map(Ride r) {
      int channel = card ? SC : SK;
      return new ZoneAvg(r.zone, r.timestamp, card ? r.tip : r.fare, 0,
          vars.next(channel));
    }
  }

  /** Windowed average that also sums the virtual-source provenance counts. */
  public static class AvgAggregate
      implements AggregateFunction<ZoneAvg, AvgAggregate.Acc, ZoneAvg> {

    /** Same aggregate; reports the growth of its accumulator's annotation (8 B per variable) to LiveState. Used in live_memory mode only. */
    public static class Live extends AvgAggregate {
      @Override
      public Acc add(ZoneAvg value, Acc acc) {
        long before = acc.hm.numVars();
        super.add(value, acc);
        LiveState.grow(8L * (acc.hm.numVars() - before));
        return acc;
      }
    }

    @Override
    public Acc createAccumulator() {
      return new Acc();
    }

    @Override
    public Acc add(ZoneAvg value, Acc acc) {
      acc.zone = value.zone;
      acc.sum += value.value1;
      acc.count++;
      // Average over values; provenance still multiplies becasue every ride must have contributed.
      acc.hm.mul(value.hm);
      return acc;
    }

    @Override
    public ZoneAvg getResult(Acc acc) {
      double avg = acc.count > 0 ? acc.sum / acc.count : 0;
      // Placeholder: StampDerivedTime sets the window end after the operator.
      return new ZoneAvg(acc.zone, 0, avg, 0, acc.hm.toMeta());
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.zone = b.zone;
      a.sum += b.sum;
      a.count += b.count;
      a.hm.mul(b.hm);
      return a;
    }

    public static class Acc implements Serializable {
      int zone;
      double sum;
      long count;
      MonomialBuilder hm = new MonomialBuilder();
    }
  }

  /** Joins the card and cash averages, merging both virtual sources' counts. */
  public static class JoinAvgs
      implements JoinFunction<ZoneAvg, ZoneAvg, ZoneAvg> {
    @Override
    public ZoneAvg join(ZoneAvg card, ZoneAvg cash) {
      return new ZoneAvg(
          card.zone,
          // Placeholder: StampDerivedTime sets the window end after the join.
          0,
          card.value1,
          cash.value1,
          card.hm.times(cash.hm));
    }
  }

  // Sink
  public static class FileSink extends RichSinkFunction<ZoneAvg> {
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
    public void invoke(ZoneAvg value, Context context) {
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
