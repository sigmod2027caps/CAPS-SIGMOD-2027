import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import ink.InkBenchSinks;
import ink.InkCardinalityDiscardSink;
import ink.InkMeta;
import ink.InkMetadataProbe;
import ink.InkQuerySink;
import ink.MonomialBuilder;

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
 *   Source ──┬── Filter(CRD) ── Map(SC=1) ── AvgTip/zone(1h)  ──┐
 *            │                                                   Join(zone, merge counts) ── Sink
 *            └── Filter(CSH) ── Map(SK=1) ── AvgFare/zone(1h) ──┘
 * </pre>
 *
 * Output per line: zone,timestamp,avgCardTip,avgCashFare,countSC,countSK
 */
public class TaxiPaymentAvgCaps {

  // How-much provenance channels: one per virtual source.
  public static final int SC = 0;  // card rides
  public static final int SK = 1;  // cash rides
  public static final int CHANNELS = 2;

  // Reorder slack for the online index: output timestamp disorder is bounded
  // by the 1-hour window; doubled for safety.
  public static final long INDEX_SLACK_MS = 2 * 60 * 60 * 1000L;

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
    final boolean benchMode = args.length > 2 && !memoryVolume;

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setParallelism(1);

    DataStream<Ride> sourceStream = env
        .addSource(new RideSource(inputFile))
        .name("SOURCE")
        .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Ride>() {
          @Override
          public long extractTimestamp(Ride r, long prev) {
            return r.timestamp;
          }
          @Override
          public Watermark checkAndGetNextWatermark(Ride r, long ts) {
            return new Watermark(ts);
          }
        });

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
        .aggregate(new AvgAggregate())
        .name("AGG-CARD");
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
        .aggregate(new AvgAggregate())
        .name("AGG-CASH");
    cash = InkMetadataProbe.attach(cash, memoryVolume, "AGG-CASH", meta);

    // Join on zone — merge counts from both virtual sources
    DataStream<ZoneAvg> joined = card
        .join(cash)
        .where(z -> z.zone)
        .equalTo(z -> z.zone)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .apply(new JoinAvgs());
    joined = InkMetadataProbe.attach(joined, memoryVolume, "JOIN", meta);

    if (memoryVolume) {
      joined
          .addSink(new InkCardinalityDiscardSink<ZoneAvg>())
          .name("SINK");
    } else if (benchMode) {
      joined
          .addSink(InkBenchSinks.create(args, 2,
              (InkQuerySink.TsFn<ZoneAvg>) z -> z.timestamp,
              (InkQuerySink.MetaFn<ZoneAvg>) z -> z.hm,
              (temporalindex.SampleKeyFn<ZoneAvg>) (z, out) -> {
                out.writeInt(z.zone);
                out.writeLong(z.timestamp);
                temporalindex.LogicalSampler.writeDoubleBits(out, z.value1);
                temporalindex.LogicalSampler.writeDoubleBits(out, z.value2);
              },
              CHANNELS, INDEX_SLACK_MS))
          .name("SINK-INDEX");
    } else {
      joined
          .addSink(new FileSink(outputFile))
          .name("SINK");
    }

    InkMetadataProbe.writeIfEnabled(env.execute("TaxiPaymentAvg-HowMuch"),
        args, 2, "taxi_2");
  }

  // ── Data types ──────────────────────────────────────────────

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
      return zone + "," + timestamp + "," + value1 + "," + value2 + "," + hm;
    }
  }

  // ── Source ──────────────────────────────────────────────────

  /** Reads taxis.txt: taxi_id,ts_ms,passengers,zone,payment,fare,tip */
  public static class RideSource extends RichSourceFunction<Ride> {
    private final String path;
    private volatile boolean running = true;

    public RideSource(String path) {
      this.path = path;
    }

    @Override
    public void run(SourceContext<Ride> ctx) throws Exception {
      try (BufferedReader br = new BufferedReader(new FileReader(path))) {
        String line;
        while (running && (line = br.readLine()) != null) {
          String trimmed = line.trim();
          if (trimmed.isEmpty()) continue;
          String[] parts = trimmed.split(",", 7);
          Ride r = new Ride();
          r.taxiId = Long.parseLong(parts[0]);
          r.timestamp = Long.parseLong(parts[1]);
          r.passengers = Integer.parseInt(parts[2]);
          r.zone = Integer.parseInt(parts[3]);
          r.payment = parts[4];
          r.fare = Double.parseDouble(parts[5]);
          r.tip = Double.parseDouble(parts[6]);
          ctx.collect(r);
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  // ── Operators ──────────────────────────────────────────────

  /**
   * Projects a ride to (zone, value): tip for the card branch, fare for cash.
   * Tags the tuple with the virtual-source how-much provenance count:
   * channel SC for card rides, SK for cash rides.
   */
  public static class ToZoneValue implements MapFunction<Ride, ZoneAvg> {
    private final boolean card;
    // Per-operator-instance source-tuple counter; see InkMeta.source.
    private transient long seq;

    public ToZoneValue(boolean card) {
      this.card = card;
    }

    @Override
    public ZoneAvg map(Ride r) {
      int channel = card ? SC : SK;
      return new ZoneAvg(r.zone, r.timestamp, card ? r.tip : r.fare, 0,
          InkMeta.source(channel, CHANNELS, seq++));
    }
  }

  /** Windowed average that also sums the virtual-source provenance counts. */
  public static class AvgAggregate
      implements AggregateFunction<ZoneAvg, AvgAggregate.Acc, ZoneAvg> {

    @Override
    public Acc createAccumulator() {
      return new Acc();
    }

    @Override
    public Acc add(ZoneAvg value, Acc acc) {
      acc.zone = value.zone;
      acc.sum += value.value1;
      acc.count++;
      acc.maxTs = Math.max(acc.maxTs, value.timestamp);
      // Average over values; provenance still multiplies becasue every ride must have contributed.
      acc.hm.mul(value.hm);
      return acc;
    }

    @Override
    public ZoneAvg getResult(Acc acc) {
      double avg = acc.count > 0 ? acc.sum / acc.count : 0;
      return new ZoneAvg(acc.zone, acc.maxTs, avg, 0, acc.hm.toMeta());
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.zone = b.zone;
      a.sum += b.sum;
      a.count += b.count;
      a.maxTs = Math.max(a.maxTs, b.maxTs);
      a.hm.mul(b.hm);
      return a;
    }

    public static class Acc implements Serializable {
      int zone;
      double sum;
      long count;
      long maxTs;
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
          Math.max(card.timestamp, cash.timestamp),
          card.value1,
          cash.value1,
          card.hm.times(cash.hm));
    }
  }

  // ── Sink ───────────────────────────────────────────────────

  public static class FileSink extends RichSinkFunction<ZoneAvg> {
    private final String path;
    private transient PrintWriter pw;

    public FileSink(String path) {
      this.path = path;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      pw = new PrintWriter(new FileWriter(path), true);
    }

    @Override
    public void invoke(ZoneAvg value, Context context) {
      pw.println(value);
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
