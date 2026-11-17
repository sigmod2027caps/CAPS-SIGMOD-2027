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
 * How-much provenance (path-based) Taxi Passenger Join dataflow (the paper's
 * introduction figure).
 *
 * Identical computation to the no-provenance job. The stream is the multiplex
 * of two virtual sources, Manhattan pickups (S1) and Queens pickups (S2);
 * rides outside both boroughs are dropped at the source. Each tuple carries an
 * {@link InkMeta} polynomial annotation (green_polynomial baseline in the paper)
 * whose how-much folding is one counter per (virtual source, branch)
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

  // Reorder slack for the online index. Every windowed operator stamps its output with
  // the right boundary of its window (StampDerivedTime), so at parallelism 1 the sink
  // input is already in timestamp order and the reorder buffer releases at once.
  public static final long INDEX_SLACK_MS = 0;

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: TaxiPassengerJoinCaps <inputFile> <outputFile>"
          + " [mode benchDir params...]   (mode: provenance | provenance_timer"
          + " | memory_volume, see InkBenchSinks / InkMetadataProbe)");
      System.exit(1);
    }
    final String inputFile = args[0];
    final String outputFile = args[1];
    final boolean memoryVolume = InkMetadataProbe.enabled(args, 2);
    final boolean live = LiveState.enabled(args, 2);
    final boolean benchMode = args.length > 2 && !memoryVolume && !live;

    // Default: one counter per path (4). CAPS_GRANULARITY=source: one counter
    // per virtual source (2), for the counter-count cost comparison.
    // CAPS_GRANULARITY=source: fewer channels, same polynomial charge per tuple.
    final boolean sourceGranularity = "source".equals(System.getenv("CAPS_GRANULARITY"));
    final int channels = sourceGranularity ? SOURCE_CHANNELS : PATH_CHANNELS;

    // Same environment, source and watermark assigner as the CAPS and no-provenance jobs (CAPS_* variables, see
    // DisorderConfig): the defaults are the ordered run with a periodic watermark, no disorder.
    final DisorderConfig cfg = DisorderConfig.fromEnv();
    final StreamExecutionEnvironment env = Disorder.environment(cfg);

    DataStream<Ride> sourceStream = Disorder.source(env, cfg, "SOURCE", inputFile, new RideParser(),
        r -> r.timestamp, Ride.class);

    // Annotation bytes of one buffered tuple (live_memory): the monomials of its polynomial.
    final LiveState.Sizer<TaxiCount> polyBytes = c -> c.hm.metadataBytes();

    InkQuerySink.MetaFn<TaxiCount> meta =
        (InkQuerySink.MetaFn<TaxiCount>) c -> c.hm;

    // Solo branch: paths P1 (Manhattan) / P2 (Queens)
    DataStream<TaxiCount> soloMapped = sourceStream
        .filter(r -> r.passengers == 1)
        .name("FILTER-SOLO")
        .map(new ToTaxiCount(true, sourceGranularity, channels))
        .name("MAP-SOLO");
    soloMapped = InkMetadataProbe.attach(soloMapped, memoryVolume, "MAP-SOLO", meta);
    DataStream<TaxiCount> soloAgg = soloMapped
        .keyBy(c -> c.taxiId)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "AGG-SOLO", 0, Time.minutes(10), null, null))
        .aggregate(LiveState.on() ? new CountAggregate.Live(channels) : new CountAggregate(channels))
        .name("AGG-SOLO")
        .process(new StampDerivedTime<TaxiCount>((t, ts) -> t.timestamp = ts))
        .returns(TaxiCount.class)
        .name("STAMP-AGG-SOLO");
    soloAgg = InkMetadataProbe.attach(soloAgg, memoryVolume, "AGG-SOLO", meta);
    DataStream<TaxiCount> solo = soloAgg
        .filter(c -> c.value1 == 1)
        .name("FILTER-COUNT-SOLO");
    solo = InkMetadataProbe.attach(solo, memoryVolume, "FILTER-COUNT-SOLO", meta);

    // Crowded branch: paths P3 (Manhattan) / P4 (Queens)
    DataStream<TaxiCount> crowdedMapped = sourceStream
        .filter(r -> r.passengers > 2)
        .name("FILTER-CROWDED")
        .map(new ToTaxiCount(false, sourceGranularity, channels))
        .name("MAP-CROWDED");
    crowdedMapped = InkMetadataProbe.attach(crowdedMapped, memoryVolume, "MAP-CROWDED", meta);
    DataStream<TaxiCount> crowdedAgg = crowdedMapped
        .keyBy(c -> c.taxiId)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "AGG-CROWDED", 0, Time.minutes(10), null, null))
        .aggregate(LiveState.on() ? new CountAggregate.Live(channels) : new CountAggregate(channels))
        .name("AGG-CROWDED")
        .process(new StampDerivedTime<TaxiCount>((t, ts) -> t.timestamp = ts))
        .returns(TaxiCount.class)
        .name("STAMP-AGG-CROWDED");
    crowdedAgg = InkMetadataProbe.attach(crowdedAgg, memoryVolume, "AGG-CROWDED", meta);
    DataStream<TaxiCount> crowded = crowdedAgg
        .filter(c -> c.value1 >= 2)
        .name("FILTER-COUNT-CROWDED");
    crowded = InkMetadataProbe.attach(crowded, memoryVolume, "FILTER-COUNT-CROWDED", meta);

    // Join on taxiId - merge counts from both paths
    DataStream<TaxiCount> joined = solo
        .join(crowded)
        .where(c -> c.taxiId)
        .equalTo(c -> c.taxiId)
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "JOIN", 1, Time.minutes(10), null, LiveState.join(polyBytes, polyBytes)))
        .apply(new JoinCounts())
        .process(new StampDerivedTime<TaxiCount>((t, ts) -> t.timestamp = ts))
        .returns(TaxiCount.class)
        .name("STAMP-JOIN");
    joined = InkMetadataProbe.attach(joined, memoryVolume, "JOIN", meta);

    if (live) {
      // live_memory: discard sink that publishes the live-state registry when the stream ends
      joined
          .addSink(new LiveStateSink<TaxiCount>(x -> x.hm.metadataBytes()))
          .name("SINK");
    } else if (memoryVolume) {
      joined
          .addSink(new InkCardinalityDiscardSink<TaxiCount>())
          .name("SINK");
    } else if (benchMode) {
      InkBenchSinks.attach(joined, InkBenchSinks.create(args, 2,
              (InkQuerySink.TsFn<TaxiCount>) c -> c.timestamp,
              (InkQuerySink.MetaFn<TaxiCount>) c -> c.hm,
              (temporalindex.SampleKeyFn<TaxiCount>) (c, out) -> {
                out.writeLong(c.taxiId);
                out.writeLong(c.timestamp);
                temporalindex.LogicalSampler.writeDoubleBits(out, c.value1);
                temporalindex.LogicalSampler.writeDoubleBits(out, c.value2);
              },
              channels, INDEX_SLACK_MS),
          "SINK-INDEX");
    } else {
      joined
          .addSink(new FileSink(outputFile, channels))
          .setParallelism(1)
          .name("SINK");
    }

    JobExecutionResult result = env.execute("TaxiPassengerJoin-HowMuch");
    InkMetadataProbe.writeIfEnabled(result, args, 2, "taxi_1");
    LiveState.writeIfEnabled(result, args, 2, "taxi_1", "green_polynomial");
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
    public InkMeta hm;

    public TaxiCount() {}

    public TaxiCount(long taxiId, long timestamp, double value1, double value2,
                     InkMeta hm) {
      this.taxiId = taxiId;
      this.timestamp = timestamp;
      this.value1 = value1;
      this.value2 = value2;
      this.hm = hm;
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
  /**
   * Projects a ride to a per-taxi count of 1, tagging it with the how-much
   * provenance count of its path: channel = branch * 2 + borough
   * (P1 = Manhattan solo, P2 = Queens solo, P3 = Manhattan crowded,
   * P4 = Queens crowded). With source granularity the branch is ignored and
   * the channel is just the virtual source (borough).
   */
  public static class ToTaxiCount extends RichMapFunction<Ride, TaxiCount> {
    private final boolean soloBranch;
    private final boolean sourceGranularity;
    private final int channels;
    // Source-tuple variables of this operator instance, unique over its subtasks; see InkMeta, VarSource.
    private final VarSource vars;

    public ToTaxiCount(boolean soloBranch, boolean sourceGranularity, int channels) {
      this.soloBranch = soloBranch;
      this.sourceGranularity = sourceGranularity;
      this.channels = channels;
      this.vars = new VarSource(channels);
    }

    @Override
    public void open(Configuration parameters) {
      vars.open(getRuntimeContext());
    }

    @Override
    public TaxiCount map(Ride r) {
      int source = borough(r.zone);
      int channel = sourceGranularity ? source : (soloBranch ? 0 : 2) + source;
      return new TaxiCount(r.taxiId, r.timestamp, 1, 0,
          vars.next(channel));
    }
  }

  /** Counts the rides of a taxi inside the window, merging provenance counts. */
  public static class CountAggregate
      implements AggregateFunction<TaxiCount, CountAggregate.Acc, TaxiCount> {

    /** Same aggregate; reports the growth of its accumulator's annotation (8 B per variable) to LiveState. Used in live_memory mode only. */
    public static class Live extends CountAggregate {
      public Live(int channels) {
        super(channels);
      }

      @Override
      public Acc add(TaxiCount value, Acc acc) {
        long before = acc.hm.numVars();
        super.add(value, acc);
        LiveState.grow(8L * (acc.hm.numVars() - before));
        return acc;
      }
    }

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
      // Window count multiplies monomials - degree tracks how many rides contributed.
      acc.hm.mul(value.hm);
      return acc;
    }

    @Override
    public TaxiCount getResult(Acc acc) {
      // Placeholder: StampDerivedTime sets the window end after the operator.
      return new TaxiCount(acc.taxiId, 0, acc.count, 0, acc.hm.toMeta());
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.taxiId = b.taxiId;
      a.count += b.count;
      a.hm.mul(b.hm);
      return a;
    }

    public static class Acc implements Serializable {
      long taxiId;
      long count;
      MonomialBuilder hm;

      public Acc() {}

      public Acc(int channels) {
        this.hm = new MonomialBuilder();
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
          solo.hm.times(crowded.hm));
    }
  }

  // Sink
  public static class FileSink extends RichSinkFunction<TaxiCount> {
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
    public void invoke(TaxiCount value, Context context) {
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
