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
 * No-provenance Taxi Passenger Join dataflow (the paper's introduction
 * figure): per 1-hour window, the taxis with exactly one 1-passenger ride
 * and at least two rides with more than two passengers. The stream is the
 * multiplex of two virtual sources, Manhattan pickups (S1) and Queens
 * pickups (S2); rides picked up outside both boroughs are dropped at the
 * source.
 *
 * <pre>
 *   S1 (Manhattan) ──┐    ┌── Filter(pax=1) ── Count/taxi(1h) ── Filter(c=1)  ──┐
 *                    Mux ─┤                                                      Join(taxiID) ── Sink
 *   S2 (Queens)    ──┘    └── Filter(pax>2) ── Count/taxi(1h) ── Filter(c>=2) ──┘
 * </pre>
 *
 * Output per line: taxiId,timestamp,soloCount,crowdedCount
 *
 * Usage:
 *   flink run --class TaxiPassengerJoin taxi-1-1.0-SNAPSHOT.jar
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

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    // Same branches, borough mux, and 1h/10min windows as TaxiPassengerJoinCaps (intro figure).
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

    // Left path: solo (1-passenger) rides, count per taxi, keep taxis with exactly one
    DataStream<TaxiCount> solo = sourceStream
        .filter(r -> r.passengers == 1)
        .name("FILTER-SOLO")
        .map(new ToTaxiCount())
        .name("MAP-SOLO")
        .keyBy(c -> c.taxiId)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .aggregate(new CountAggregate())
        .name("AGG-SOLO")
        .filter(c -> c.value1 == 1)
        .name("FILTER-COUNT-SOLO");

    // Right path: crowded (>2 passengers) rides, count per taxi, keep taxis with two or more
    DataStream<TaxiCount> crowded = sourceStream
        .filter(r -> r.passengers > 2)
        .name("FILTER-CROWDED")
        .map(new ToTaxiCount())
        .name("MAP-CROWDED")
        .keyBy(c -> c.taxiId)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .aggregate(new CountAggregate())
        .name("AGG-CROWDED")
        .filter(c -> c.value1 >= 2)
        .name("FILTER-COUNT-CROWDED");

    // Join on taxiId — same window as CAPS taxi_1.
    solo
        .join(crowded)
        .where(c -> c.taxiId)
        .equalTo(c -> c.taxiId)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .apply(new JoinCounts())
        .addSink(new FileSink(outputFile, sampleProb))
        .name("SINK");

    env.execute("TaxiPassengerJoin-No-provenance");
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

  // ── Source ──────────────────────────────────────────────────

  /**
   * Reads taxis.txt (taxi_id,ts_ms,passengers,zone,payment,fare,tip) and
   * multiplexes the two virtual sources: only Manhattan and Queens pickups
   * are emitted.
   */
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
          if (borough(r.zone) < 0) continue;
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
      acc.maxTs = Math.max(acc.maxTs, value.timestamp);
      return acc;
    }

    @Override
    public TaxiCount getResult(Acc acc) {
      return new TaxiCount(acc.taxiId, acc.maxTs, acc.count, 0);
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.taxiId = b.taxiId;
      a.count += b.count;
      a.maxTs = Math.max(a.maxTs, b.maxTs);
      return a;
    }

    public static class Acc implements Serializable {
      long taxiId;
      long count;
      long maxTs;
    }
  }

  public static class JoinCounts
      implements JoinFunction<TaxiCount, TaxiCount, TaxiCount> {
    @Override
    public TaxiCount join(TaxiCount solo, TaxiCount crowded) {
      return new TaxiCount(
          solo.taxiId,
          Math.max(solo.timestamp, crowded.timestamp),
          solo.value1,
          crowded.value1);
    }
  }

  // ── Sink ───────────────────────────────────────────────────

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
