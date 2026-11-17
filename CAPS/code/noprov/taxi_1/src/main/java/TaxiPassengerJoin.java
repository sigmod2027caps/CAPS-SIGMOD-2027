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
 * No-provenance Taxi Passenger Join dataflow (the paper's introduction
 * figure): per 1-hour window, the taxis with exactly one 1-passenger ride
 * and at least two rides with more than two passengers. The stream is the
 * multiplex of two virtual sources, Manhattan pickups (S1) and Queens
 * pickups (S2); rides picked up outside both boroughs are dropped at the
 * source.
 *
 * <pre>
 *   S1 (Manhattan) --+    +-- Filter(pax=1) -- Count/taxi(1h) -- Filter(c=1)  --+
 *                    Mux -+                                                      Join(taxiID) -- Sink
 *   S2 (Queens)    --+    +-- Filter(pax>2) -- Count/taxi(1h) -- Filter(c>=2) --+
 * </pre>
 *
 * Output per line: taxiId,timestamp,soloCount,crowdedCount
 *
 * Usage:
 *   flink run --class TaxiPassengerJoin taxi-1-1.0-REV-SNAPSHOT.jar
 *       &lt;inputFile&gt; &lt;outputFile&gt; [memoryFile]
 */
public class TaxiPassengerJoin {

  public static final int MANHATTAN = 0;
  public static final int QUEENS = 1;

  // taxis.txt grid geometry (make_taxis_txt.py): 40x25 cells over the NYC box.
  private static final double LON_MIN = -74.05, LON_MAX = -73.75;
  private static final double LAT_MIN = 40.58, LAT_MAX = 40.92;
  private static final int LON_CELLS = 40, LAT_CELLS = 25;

  /**
   * Virtual source of a pickup zone: {@link #MANHATTAN}, {@link #QUEENS}, or
   * -1 for zones outside both boroughs (approximated by disjoint bounding
   * boxes over the grid-cell centers).
   */
  public static int borough(int zone) {
    double lon = LON_MIN + (zone % LON_CELLS + 0.5) * (LON_MAX - LON_MIN) / LON_CELLS;
    double lat = LAT_MIN + (zone / LON_CELLS + 0.5) * (LAT_MAX - LAT_MIN) / LAT_CELLS;
    if (lon >= -74.02 && lon < -73.92 && lat >= 40.70 && lat < 40.88) {
      return MANHATTAN;
    }
    if (lon >= -73.92 && lat < 40.80) {
      return QUEENS;
    }
    return -1;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: TaxiPassengerJoin <inputFile> <outputFile> [memoryFile]");
      System.exit(1);
    }
    final String inputFile = args[0];
    final String outputFile = args[1];
    // sampleProb drives the sink coin; change it only together with the CAPS bench args.
    final double sampleProb = args.length > 2 ? Double.parseDouble(args[2]) : 0.0;

    // Environment, parallel disordered source and watermarks (CAPS_* variables, see DisorderConfig). The
    // defaults are the ordered run: the periodic bounded-out-of-orderness watermark with delta = 0, no
    // disorder. Same wiring in the CAPS and the no-provenance job.
    final DisorderConfig cfg = DisorderConfig.fromEnv();
    final StreamExecutionEnvironment env = Disorder.environment(cfg);

    DataStream<Ride> sourceStream = Disorder.source(env, cfg, "SOURCE", inputFile, new RideParser(),
        r -> r.timestamp, Ride.class);

    // Left path: solo (1-passenger) rides, count per taxi, keep taxis with exactly one
    DataStream<TaxiCount> soloIn = sourceStream
        .filter(r -> r.passengers == 1)
        .name("FILTER-SOLO")
        .map(new ToTaxiCount())
        .name("MAP-SOLO");
    OutputTag<TaxiCount> lateAggSolo = Disorder.lateTag("AGG-SOLO", soloIn);
    SingleOutputStreamOperator<TaxiCount> opAggSolo = Disorder.lateOutput(soloIn
        .keyBy(c -> c.taxiId)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .trigger(Disorder.trigger(cfg, Time.minutes(10))), lateAggSolo, cfg)
        .aggregate(new CountAggregate())
        .name("AGG-SOLO");
    Disorder.lateSink(opAggSolo, lateAggSolo, "AGG-SOLO", cfg);
    DataStream<TaxiCount> solo = opAggSolo
        .process(new StampDerivedTime<TaxiCount>((t, ts) -> t.timestamp = ts))
        .returns(TaxiCount.class)
        .name("STAMP-AGG-SOLO")
        .filter(c -> c.value1 == 1)
        .name("FILTER-COUNT-SOLO");

    // Right path: crowded (>2 passengers) rides, count per taxi, keep taxis with two or more
    DataStream<TaxiCount> crowdedIn = sourceStream
        .filter(r -> r.passengers > 2)
        .name("FILTER-CROWDED")
        .map(new ToTaxiCount())
        .name("MAP-CROWDED");
    OutputTag<TaxiCount> lateAggCrowded = Disorder.lateTag("AGG-CROWDED", crowdedIn);
    SingleOutputStreamOperator<TaxiCount> opAggCrowded = Disorder.lateOutput(crowdedIn
        .keyBy(c -> c.taxiId)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .trigger(Disorder.trigger(cfg, Time.minutes(10))), lateAggCrowded, cfg)
        .aggregate(new CountAggregate())
        .name("AGG-CROWDED");
    Disorder.lateSink(opAggCrowded, lateAggCrowded, "AGG-CROWDED", cfg);
    DataStream<TaxiCount> crowded = opAggCrowded
        .process(new StampDerivedTime<TaxiCount>((t, ts) -> t.timestamp = ts))
        .returns(TaxiCount.class)
        .name("STAMP-AGG-CROWDED")
        .filter(c -> c.value1 >= 2)
        .name("FILTER-COUNT-CROWDED");

    // Join on taxiId - same window as CAPS taxi_1.
    solo
        .join(crowded)
        .where(c -> c.taxiId)
        .equalTo(c -> c.taxiId)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .trigger(Disorder.trigger(cfg, Time.minutes(10)))
        .with(new JoinCounts())
        .name("JOIN")
        .process(new StampDerivedTime<TaxiCount>((t, ts) -> t.timestamp = ts))
        .returns(TaxiCount.class)
        .name("STAMP-JOIN")
        .addSink(new FileSink(outputFile, sampleProb))
        // The discard sink of the timing runs follows CAPS_SINK_PARALLELISM like the CAPS sinks; a file sink stays on one subtask.
        .setParallelism("none".equals(outputFile) ? cfg.sinkParallelism : 1)
        .name("SINK");

    Disorder.execute(env, "TaxiPassengerJoin-No-provenance");
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

  public static class TaxiCount implements Serializable {
    public long taxiId;
    public long timestamp;
    public double value1;
    public double value2;

    public TaxiCount() {}

    public TaxiCount(long taxiId, long timestamp, double value1, double value2) {
      this.taxiId = taxiId;
      this.timestamp = timestamp;
      this.value1 = value1;
      this.value2 = value2;
    }

    @Override
    public String toString() {
      return taxiId + "," + timestamp + "," + value1 + "," + value2;
    }
  }

  // Source
  /**
   * Parses a line of taxis.txt (taxi_id,ts_ms,passengers,zone,payment,fare,tip) and, like the released source,
   * keeps only Manhattan and Queens pickups. With CAPS_ZIPF the taxi id (the key of both window aggregates and
   * of the join) is replaced by a Zipf-distributed rank; the borough of the pickup is unchanged.
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
      r.taxiId = zipf == null ? Long.parseLong(parts[0]) : zipf.rank(lineNo, 0);
      r.timestamp = Long.parseLong(parts[1]);
      r.passengers = Integer.parseInt(parts[2]);
      r.zone = Integer.parseInt(parts[3]);
      r.payment = parts[4];
      r.fare = Double.parseDouble(parts[5]);
      r.tip = Double.parseDouble(parts[6]);
      if (borough(r.zone) < 0) return null;
      return r;
    }
  }

  // Operators
  public static class ToTaxiCount implements MapFunction<Ride, TaxiCount> {
    @Override
    public TaxiCount map(Ride r) {
      return new TaxiCount(r.taxiId, r.timestamp, 1, 0);
    }
  }

  /** Counts the rides of a taxi inside the window. */
  public static class CountAggregate
      implements AggregateFunction<TaxiCount, CountAggregate.Acc, TaxiCount> {

    @Override
    public Acc createAccumulator() {
      return new Acc();
    }

    @Override
    public Acc add(TaxiCount value, Acc acc) {
      acc.taxiId = value.taxiId;
      acc.count++;
      return acc;
    }

    @Override
    public TaxiCount getResult(Acc acc) {
      // Placeholder: StampDerivedTime sets the window end after the operator.
      return new TaxiCount(acc.taxiId, 0, acc.count, 0);
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.taxiId = b.taxiId;
      a.count += b.count;
      return a;
    }

    public static class Acc implements Serializable {
      long taxiId;
      long count;
    }
  }

  public static class JoinCounts
      implements JoinFunction<TaxiCount, TaxiCount, TaxiCount> {
    @Override
    public TaxiCount join(TaxiCount solo, TaxiCount crowded) {
      return new TaxiCount(
          solo.taxiId,
          // Placeholder: StampDerivedTime sets the window end after the join.
          0,
          solo.value1,
          crowded.value1);
    }
  }

  // Sink
  public static class FileSink extends RichSinkFunction<TaxiCount> {
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
    private transient temporalindex.SampleKeyFn<TaxiCount> keyFn;
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
      keyFn = new temporalindex.SampleKeyFn<TaxiCount>() {
        @Override
        public void writeKey(TaxiCount v, java.io.DataOutput out)
            throws java.io.IOException {
          out.writeLong(v.taxiId);
          out.writeLong(v.timestamp);
          temporalindex.LogicalSampler.writeDoubleBits(out, v.value1);
          temporalindex.LogicalSampler.writeDoubleBits(out, v.value2);
        }
      };
      predicateChecksum = 0L;
      selected = 0;
    }

    @Override
    public void invoke(TaxiCount value, Context context) {
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
