import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import org.apache.flink.api.common.JobExecutionResult;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import caps.BenchSinks;
import caps.CapsCardinalityDiscardSink;
import caps.CapsMetadataProbe;
import caps.HowMuch;
import caps.IndexingSink;
import caps.OutputLogSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import temporalindex.Disorder;
import temporalindex.LiveState;
import temporalindex.LiveStateSink;
import temporalindex.DisorderConfig;
import temporalindex.DisorderSource;
import temporalindex.Zipf;
import temporalindex.StampDerivedTime;

/**
 * How-much provenance (path-based) Taxi Passenger Join dataflow (the paper's
 * introduction figure).
 *
 * Identical computation to the no-provenance job. The stream is the multiplex
 * of two virtual sources, Manhattan pickups (S1) and Queens pickups (S2);
 * rides outside both boroughs are dropped at the source. Each tuple carries a
 * {@link HowMuch} meta-data with one counter per (virtual source, branch)
 * path: P1 = S1 via solo, P2 = S2 via solo, P3 = S1 via crowded,
 * P4 = S2 via crowded.
 *
 * <pre>
 *   S1 (Manhattan) --+    +-- Filter(pax=1) -- Map(P1/P2=1) -- Count/taxi(1h) -- Filter(c=1)  --+
 *                    Mux -+                                                                      Join(taxiID, merge counts) -- Sink
 *   S2 (Queens)    --+    +-- Filter(pax>2) -- Map(P3/P4=1) -- Count/taxi(1h) -- Filter(c>=2) --+
 * </pre>
 *
 * Output per line: taxiId,timestamp,soloCount,crowdedCount,countP1..countP4
 *
 * Setting the environment variable CAPS_GRANULARITY=source switches the
 * meta-data to one counter per virtual source (2 channels instead of 4),
 * used by the paths experiment to show the capture cost does not depend on
 * the number of counters.
 */
public class TaxiPassengerJoinCaps {

  public static final int MANHATTAN = 0;
  public static final int QUEENS = 1;

  // How-much provenance channels: one per (virtual source, branch) path.
  // channel = branch * 2 + borough (branch: 0 = solo, 1 = crowded).
  public static final int PATH_CHANNELS = 4;
  public static final int SOURCE_CHANNELS = 2;

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
      System.err.println("Usage: TaxiPassengerJoinCaps <inputFile> <outputFile>"
          + " [mode benchDir params...]   (mode: queries | expiry | summarize"
          + " | memory_volume, see BenchSinks / CapsMetadataProbe)");
      System.exit(1);
    }
    final String inputFile = args[0];
    final String outputFile = args[1];
    final boolean memoryVolume = CapsMetadataProbe.enabled(args, 2);
    final boolean live = LiveState.enabled(args, 2);
    final boolean benchMode = args.length > 2 && !memoryVolume && !live;

    // Default: one counter per path (4). CAPS_GRANULARITY=source: one counter
    // per virtual source (2), for the counter-count cost comparison.
    final boolean sourceGranularity = "source".equals(System.getenv("CAPS_GRANULARITY"));
    final int channels = sourceGranularity ? SOURCE_CHANNELS : PATH_CHANNELS;

    // Environment, parallel disordered source and watermarks (CAPS_* variables, see DisorderConfig). The
    // defaults are the ordered run: the periodic bounded-out-of-orderness watermark with delta = 0, no
    // disorder. Same wiring in the CAPS and the no-provenance job.
    final DisorderConfig cfg = DisorderConfig.fromEnv();
    final StreamExecutionEnvironment env = Disorder.environment(cfg);

    DataStream<Ride> sourceStream = Disorder.source(env, cfg, "SOURCE", inputFile, new RideParser(),
        r -> r.timestamp, Ride.class);

    IndexingSink.HmFn<TaxiCount> hm =
        (IndexingSink.HmFn<TaxiCount>) c -> c.hm;

    // Solo branch: paths P1 (Manhattan) / P2 (Queens). 1h/10min slide matches noprov and the intro figure.
    DataStream<TaxiCount> soloMapped = sourceStream
        .filter(r -> r.passengers == 1)
        .name("FILTER-SOLO")
        .map(new ToTaxiCount(true, sourceGranularity, channels))
        .name("MAP-SOLO");
    soloMapped = CapsMetadataProbe.attach(soloMapped, memoryVolume, "MAP-SOLO", hm);
    OutputTag<TaxiCount> lateAggSolo = Disorder.lateTag("AGG-SOLO", soloMapped);
    SingleOutputStreamOperator<TaxiCount> opAggSolo = Disorder.lateOutput(soloMapped
        .keyBy(c -> c.taxiId)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .trigger(LiveState.trigger(Disorder.trigger(cfg, Time.minutes(10)), "AGG-SOLO", 0, Time.minutes(10), LiveState.constant(channels * 8L), null)), lateAggSolo, cfg)
        .aggregate(new CountAggregate(channels))
        .name("AGG-SOLO");
    Disorder.lateSink(opAggSolo, lateAggSolo, "AGG-SOLO", cfg);
    DataStream<TaxiCount> soloAgg = opAggSolo
        .process(new StampDerivedTime<TaxiCount>((t, ts) -> t.timestamp = ts))
        .returns(TaxiCount.class)
        .name("STAMP-AGG-SOLO");
    soloAgg = CapsMetadataProbe.attach(soloAgg, memoryVolume, "AGG-SOLO", hm);
    DataStream<TaxiCount> solo = soloAgg
        .filter(c -> c.value1 == 1)
        .name("FILTER-COUNT-SOLO");
    solo = CapsMetadataProbe.attach(solo, memoryVolume, "FILTER-COUNT-SOLO", hm);

    // Crowded branch: paths P3 (Manhattan) / P4 (Queens)
    DataStream<TaxiCount> crowdedMapped = sourceStream
        .filter(r -> r.passengers > 2)
        .name("FILTER-CROWDED")
        .map(new ToTaxiCount(false, sourceGranularity, channels))
        .name("MAP-CROWDED");
    crowdedMapped = CapsMetadataProbe.attach(crowdedMapped, memoryVolume, "MAP-CROWDED", hm);
    OutputTag<TaxiCount> lateAggCrowded = Disorder.lateTag("AGG-CROWDED", crowdedMapped);
    SingleOutputStreamOperator<TaxiCount> opAggCrowded = Disorder.lateOutput(crowdedMapped
        .keyBy(c -> c.taxiId)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .trigger(LiveState.trigger(Disorder.trigger(cfg, Time.minutes(10)), "AGG-CROWDED", 0, Time.minutes(10), LiveState.constant(channels * 8L), null)), lateAggCrowded, cfg)
        .aggregate(new CountAggregate(channels))
        .name("AGG-CROWDED");
    Disorder.lateSink(opAggCrowded, lateAggCrowded, "AGG-CROWDED", cfg);
    DataStream<TaxiCount> crowdedAgg = opAggCrowded
        .process(new StampDerivedTime<TaxiCount>((t, ts) -> t.timestamp = ts))
        .returns(TaxiCount.class)
        .name("STAMP-AGG-CROWDED");
    crowdedAgg = CapsMetadataProbe.attach(crowdedAgg, memoryVolume, "AGG-CROWDED", hm);
    DataStream<TaxiCount> crowded = crowdedAgg
        .filter(c -> c.value1 >= 2)
        .name("FILTER-COUNT-CROWDED");
    crowded = CapsMetadataProbe.attach(crowded, memoryVolume, "FILTER-COUNT-CROWDED", hm);

    // Join on taxiId - merge counts from both paths (paper intro figure).
    DataStream<TaxiCount> joined = solo
        .join(crowded)
        .where(c -> c.taxiId)
        .equalTo(c -> c.taxiId)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .trigger(LiveState.trigger(Disorder.trigger(cfg, Time.minutes(10)), "JOIN", 1, Time.minutes(10), null, LiveState.joined(channels * 8L, channels * 8L)))
        .with(new JoinCounts())
        .name("JOIN")
        .process(new StampDerivedTime<TaxiCount>((t, ts) -> t.timestamp = ts))
        .returns(TaxiCount.class)
        .name("STAMP-JOIN");
    joined = CapsMetadataProbe.attach(joined, memoryVolume, "JOIN", hm);

    if (live) {
      // live_memory: discard sink that publishes the live-state registry when the stream ends
      joined
          .addSink(new LiveStateSink<TaxiCount>(x -> x.hm.channels() * 8L))
          .name("SINK");
    } else if (memoryVolume) {
      joined
          .addSink(new CapsCardinalityDiscardSink<TaxiCount>())
          .name("SINK");
    } else if (benchMode) {
      // Index sinks run at the job's parallelism, one shard per subtask, and receive the watermarks.
      BenchSinks.attach(joined, BenchSinks.create(args, 2,
              (IndexingSink.TsFn<TaxiCount>) c -> c.timestamp,
              (IndexingSink.HmFn<TaxiCount>) c -> c.hm,
              (temporalindex.SampleKeyFn<TaxiCount>) (c, out) -> {
                out.writeLong(c.taxiId);
                out.writeLong(c.timestamp);
                temporalindex.LogicalSampler.writeDoubleBits(out, c.value1);
                temporalindex.LogicalSampler.writeDoubleBits(out, c.value2);
              },
              channels),
          "SINK-INDEX");
      // E4 (recovery): with CAPS_OUTPUT_LOG=1 every output is also logged with its id (operator, window end, key).
      OutputLogSink.attachIfEnabled(joined, args, 2, "JOIN", c -> c.timestamp, c -> Long.toString(c.taxiId));
    } else {
      joined
          .addSink(new FileSink(outputFile))
          .setParallelism(1)
          .name("SINK");
    }

    JobExecutionResult result = Disorder.execute(env, "TaxiPassengerJoin-HowMuch");
    CapsMetadataProbe.writeIfEnabled(result, args, 2, "taxi_1");
    LiveState.writeIfEnabled(result, args, 2, "taxi_1", "caps");
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
    public HowMuch hm;

    public TaxiCount() {}

    public TaxiCount(long taxiId, long timestamp, double value1, double value2,
                     HowMuch hm) {
      this.taxiId = taxiId;
      this.timestamp = timestamp;
      this.value1 = value1;
      this.value2 = value2;
      this.hm = hm;
    }

    @Override
    public String toString() {
      return taxiId + "," + timestamp + "," + value1 + "," + value2 + "," + hm;
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
  /**
   * Projects a ride to a per-taxi count of 1, tagging it with the how-much
   * provenance count of its path: channel = branch * 2 + borough
   * (P1 = Manhattan solo, P2 = Queens solo, P3 = Manhattan crowded,
   * P4 = Queens crowded). With source granularity the branch is ignored and
   * the channel is just the virtual source (borough).
   */
  public static class ToTaxiCount implements MapFunction<Ride, TaxiCount> {
    // Provenance channel is chosen here; agg and join only merge the seperate path counts.
    private final boolean soloBranch;
    private final boolean sourceGranularity;
    private final int channels;

    public ToTaxiCount(boolean soloBranch, boolean sourceGranularity, int channels) {
      this.soloBranch = soloBranch;
      this.sourceGranularity = sourceGranularity;
      this.channels = channels;
    }

    @Override
    public TaxiCount map(Ride r) {
      int source = borough(r.zone);
      int channel = sourceGranularity ? source : (soloBranch ? 0 : 2) + source;
      return new TaxiCount(r.taxiId, r.timestamp, 1, 0,
          HowMuch.one(channel, channels));
    }
  }

  /** Counts the rides of a taxi inside the window, merging provenance counts. */
  public static class CountAggregate
      implements AggregateFunction<TaxiCount, CountAggregate.Acc, TaxiCount> {

    private final int channels;

    public CountAggregate(int channels) {
      this.channels = channels;
    }

    @Override
    public Acc createAccumulator() {
      return new Acc(channels);
    }

    @Override
    public Acc add(TaxiCount value, Acc acc) {
      acc.taxiId = value.taxiId;
      acc.count++;
      acc.hm.mergeWith(value.hm);
      return acc;
    }

    @Override
    public TaxiCount getResult(Acc acc) {
      // Placeholder: StampDerivedTime sets the window end after the operator.
      return new TaxiCount(acc.taxiId, 0, acc.count, 0, acc.hm.copy());
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.taxiId = b.taxiId;
      a.count += b.count;
      a.hm.mergeWith(b.hm);
      return a;
    }

    public static class Acc implements Serializable {
      long taxiId;
      long count;
      HowMuch hm;

      public Acc() {}

      public Acc(int channels) {
        this.hm = new HowMuch(channels);
      }
    }
  }

  /** Joins the solo and crowded counts, merging the provenance of both paths. */
  public static class JoinCounts
      implements JoinFunction<TaxiCount, TaxiCount, TaxiCount> {
    @Override
    public TaxiCount join(TaxiCount solo, TaxiCount crowded) {
      return new TaxiCount(
          solo.taxiId,
          // Placeholder: StampDerivedTime sets the window end after the join.
          0,
          solo.value1,
          crowded.value1,
          HowMuch.merge(solo.hm, crowded.hm));
    }
  }

  // Sink
  public static class FileSink extends RichSinkFunction<TaxiCount> {
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
    public void invoke(TaxiCount value, Context context) {
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
