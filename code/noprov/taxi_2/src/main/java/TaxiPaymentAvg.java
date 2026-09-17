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

/**
 * No-provenance (no-provenance) Taxi Payment Average dataflow (docs/workflows
 * taxi_q4): card and cash rides act as two virtual sources; the card branch
 * computes the average tip and the cash branch the average fare, per pickup
 * zone and 1-hour window, and the two averages are joined on the zone.
 *
 * <pre>
 *   Source ──┬── Filter(CRD) ── AvgTip/zone(1h)  ──┐
 *            │                                      Join(zone) ── Sink
 *            └── Filter(CSH) ── AvgFare/zone(1h) ──┘
 * </pre>
 *
 * Output per line: zone,timestamp,avgCardTip,avgCashFare
 *
 * Usage:
 *   flink run --class TaxiPaymentAvg taxi-2-1.0-SNAPSHOT.jar
 *       &lt;inputFile&gt; &lt;outputFile&gt; [memoryFile]
 */
public class TaxiPaymentAvg {

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: TaxiPaymentAvg <inputFile> <outputFile> [memoryFile]");
      System.exit(1);
    }
    final String inputFile = args[0];
    final String outputFile = args[1];
    final double sampleProb = args.length > 2 ? Double.parseDouble(args[2]) : 0.0;

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    // Mirrors TaxiPaymentAvgCaps: CRD/CSH split, 1h/10min avgs, zone join — provenance not inluded.
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

    // Card branch: average tip per zone
    DataStream<ZoneAvg> card = sourceStream
        .filter(r -> "CRD".equals(r.payment))
        .name("FILTER-CARD")
        .map(new ToZoneValue(true))
        .name("MAP-CARD")
        .keyBy(z -> z.zone)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .aggregate(new AvgAggregate())
        .name("AGG-CARD");

    // Cash branch: average fare per zone
    DataStream<ZoneAvg> cash = sourceStream
        .filter(r -> "CSH".equals(r.payment))
        .name("FILTER-CASH")
        .map(new ToZoneValue(false))
        .name("MAP-CASH")
        .keyBy(z -> z.zone)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .aggregate(new AvgAggregate())
        .name("AGG-CASH");

    // Join on zone
    card
        .join(cash)
        .where(z -> z.zone)
        .equalTo(z -> z.zone)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .apply(new JoinAvgs())
        // Sampling coin still runs so the gap is provenance only, not coin cost.
        .addSink(new FileSink(outputFile, sampleProb))
        .name("SINK");

    env.execute("TaxiPaymentAvg-No-provenance");
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

    public ZoneAvg() {}

    public ZoneAvg(int zone, long timestamp, double value1, double value2) {
      this.zone = zone;
      this.timestamp = timestamp;
      this.value1 = value1;
      this.value2 = value2;
    }

    @Override
    public String toString() {
      return zone + "," + timestamp + "," + value1 + "," + value2;
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

  /** Projects a ride to (zone, value): tip for the card branch, fare for cash. */
  public static class ToZoneValue implements MapFunction<Ride, ZoneAvg> {
    private final boolean card;

    public ToZoneValue(boolean card) {
      this.card = card;
    }

    @Override
    public ZoneAvg map(Ride r) {
      return new ZoneAvg(r.zone, r.timestamp, card ? r.tip : r.fare, 0);
    }
  }

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
      return acc;
    }

    @Override
    public ZoneAvg getResult(Acc acc) {
      double avg = acc.count > 0 ? acc.sum / acc.count : 0;
      return new ZoneAvg(acc.zone, acc.maxTs, avg, 0);
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.zone = b.zone;
      a.sum += b.sum;
      a.count += b.count;
      a.maxTs = Math.max(a.maxTs, b.maxTs);
      return a;
    }

    public static class Acc implements Serializable {
      int zone;
      double sum;
      long count;
      long maxTs;
    }
  }

  public static class JoinAvgs
      implements JoinFunction<ZoneAvg, ZoneAvg, ZoneAvg> {
    @Override
    public ZoneAvg join(ZoneAvg card, ZoneAvg cash) {
      return new ZoneAvg(
          card.zone,
          Math.max(card.timestamp, cash.timestamp),
          card.value1,
          cash.value1);
    }
  }

  // ── Sink ───────────────────────────────────────────────────

  public static class FileSink extends RichSinkFunction<ZoneAvg> {
    private final String path;
    private final double sampleProb;
    private transient PrintWriter pw;
    // Same Knuth 64-bit LCG as the three provenance sinks (seed 42), so
    // noprov pays the same per-output coin cost (~2 ns, no CAS) and the
    // runtime difference isolates provenance. The sample is unused;
    // coinAcc is reported at close so the JIT cannot DCE the multiply.
    //
    // The LCG is no longer stepped. Always-per-output work is now the
    // same logical-key hash + threshold as the provenance sinks.
    private transient temporalindex.LogicalSampler sampler;
    private transient temporalindex.SampleKeyFn<ZoneAvg> keyFn;
    private transient long predicateChecksum;
    private transient int selected;

    public FileSink(String path) {
      this(path, 0.0);
    }

    public FileSink(String path, double sampleProb) {
      this.path = path;
      this.sampleProb = sampleProb;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      // Output path "none": discard outputs (timing runs without file I/O).
      if (!"none".equals(path)) {
        pw = new PrintWriter(new FileWriter(path), true);
      }
      sampler = new temporalindex.LogicalSampler();
      keyFn = new temporalindex.SampleKeyFn<ZoneAvg>() {
        @Override
        public void writeKey(ZoneAvg v, java.io.DataOutput out)
            throws java.io.IOException {
          out.writeInt(v.zone);
          out.writeLong(v.timestamp);
          temporalindex.LogicalSampler.writeDoubleBits(out, v.value1);
          temporalindex.LogicalSampler.writeDoubleBits(out, v.value2);
        }
      };
      predicateChecksum = 0L;
      selected = 0;
    }

    @Override
    public void invoke(ZoneAvg value, Context context) {
      long h = sampler.hashKey(keyFn, value);
      predicateChecksum += h;
      if (temporalindex.LogicalSampler.selected(h, sampleProb)) {
        selected++;
      }
      if (pw != null) {
        pw.println(value);
      }
    }

    @Override
    public void close() throws Exception {
      if (pw != null) {
        pw.print("--- OUTPUT END ---");
        pw.flush();
        pw.close();
      }
      System.out.println("FileSink: predicateChecksum=" + predicateChecksum
          + " selected=" + selected);
    }
  }
}
