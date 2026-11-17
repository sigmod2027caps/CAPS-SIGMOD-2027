import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import temporalindex.Disorder;
import temporalindex.DisorderConfig;
import temporalindex.DisorderSource;
import temporalindex.Zipf;
import temporalindex.StampDerivedTime;

/**
 * No-provenance (no-provenance) Taxi Payment Average dataflow (docs/workflows
 * taxi_q4): card and cash rides act as two virtual sources; the card branch
 * computes the average tip and the cash branch the average fare, per pickup
 * zone and 1-hour window, and the two averages are joined on the zone.
 *
 * <pre>
 *   Source --+-- Filter(CRD) -- AvgTip/zone(1h)  --+
 *            |                                      Join(zone) -- Sink
 *            +-- Filter(CSH) -- AvgFare/zone(1h) --+
 * </pre>
 *
 * Output per line: zone,timestamp,avgCardTip,avgCashFare
 *
 * Usage:
 *   flink run --class TaxiPaymentAvg taxi-2-1.0-REV-SNAPSHOT.jar
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

    // Environment, parallel disordered source and watermarks (CAPS_* variables, see DisorderConfig). The
    // defaults are the ordered run: the periodic bounded-out-of-orderness watermark with delta = 0, no
    // disorder. Same wiring in the CAPS and the no-provenance job.
    final DisorderConfig cfg = DisorderConfig.fromEnv();
    final StreamExecutionEnvironment env = Disorder.environment(cfg);

    DataStream<Ride> sourceStream = Disorder.source(env, cfg, "SOURCE", inputFile, new RideParser(),
        r -> r.timestamp, Ride.class);

    // Card branch: average tip per zone
    DataStream<ZoneAvg> cardIn = sourceStream
        .filter(r -> "CRD".equals(r.payment))
        .name("FILTER-CARD")
        .map(new ToZoneValue(true))
        .name("MAP-CARD");
    OutputTag<ZoneAvg> lateAggCard = Disorder.lateTag("AGG-CARD", cardIn);
    SingleOutputStreamOperator<ZoneAvg> opAggCard = Disorder.lateOutput(cardIn
        .keyBy(z -> z.zone)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .trigger(Disorder.trigger(cfg, Time.minutes(10))), lateAggCard, cfg)
        .aggregate(new AvgAggregate())
        .name("AGG-CARD");
    Disorder.lateSink(opAggCard, lateAggCard, "AGG-CARD", cfg);
    DataStream<ZoneAvg> card = opAggCard
        .process(new StampDerivedTime<ZoneAvg>((t, ts) -> t.timestamp = ts))
        .returns(ZoneAvg.class)
        .name("STAMP-AGG-CARD");

    // Cash branch: average fare per zone
    DataStream<ZoneAvg> cashIn = sourceStream
        .filter(r -> "CSH".equals(r.payment))
        .name("FILTER-CASH")
        .map(new ToZoneValue(false))
        .name("MAP-CASH");
    OutputTag<ZoneAvg> lateAggCash = Disorder.lateTag("AGG-CASH", cashIn);
    SingleOutputStreamOperator<ZoneAvg> opAggCash = Disorder.lateOutput(cashIn
        .keyBy(z -> z.zone)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .trigger(Disorder.trigger(cfg, Time.minutes(10))), lateAggCash, cfg)
        .aggregate(new AvgAggregate())
        .name("AGG-CASH");
    Disorder.lateSink(opAggCash, lateAggCash, "AGG-CASH", cfg);
    DataStream<ZoneAvg> cash = opAggCash
        .process(new StampDerivedTime<ZoneAvg>((t, ts) -> t.timestamp = ts))
        .returns(ZoneAvg.class)
        .name("STAMP-AGG-CASH");

    // Join on zone
    card
        .join(cash)
        .where(z -> z.zone)
        .equalTo(z -> z.zone)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .trigger(Disorder.trigger(cfg, Time.minutes(10)))
        .with(new JoinAvgs())
        .name("JOIN")
        .process(new StampDerivedTime<ZoneAvg>((t, ts) -> t.timestamp = ts))
        .returns(ZoneAvg.class)
        .name("STAMP-JOIN")
        // Sampling coin still runs so the gap is provenance only, not coin cost.
        .addSink(new FileSink(outputFile, sampleProb))
        // The discard sink of the timing runs follows CAPS_SINK_PARALLELISM like the CAPS sinks; a file sink stays on one subtask.
        .setParallelism("none".equals(outputFile) ? cfg.sinkParallelism : 1)
        .name("SINK");

    Disorder.execute(env, "TaxiPaymentAvg-No-provenance");
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
      return acc;
    }

    @Override
    public ZoneAvg getResult(Acc acc) {
      double avg = acc.count > 0 ? acc.sum / acc.count : 0;
      // Placeholder: StampDerivedTime sets the window end after the operator.
      return new ZoneAvg(acc.zone, 0, avg, 0);
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.zone = b.zone;
      a.sum += b.sum;
      a.count += b.count;
      return a;
    }

    public static class Acc implements Serializable {
      int zone;
      double sum;
      long count;
    }
  }

  public static class JoinAvgs
      implements JoinFunction<ZoneAvg, ZoneAvg, ZoneAvg> {
    @Override
    public ZoneAvg join(ZoneAvg card, ZoneAvg cash) {
      return new ZoneAvg(
          card.zone,
          // Placeholder: StampDerivedTime sets the window end after the join.
          0,
          card.value1,
          cash.value1);
    }
  }

  // Sink
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
