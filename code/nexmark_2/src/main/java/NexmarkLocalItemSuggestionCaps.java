import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import org.apache.flink.api.common.JobExecutionResult;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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
 * How-much provenance Nexmark Q3 Local Item Suggestion dataflow.
 *
 * Identical computation to the vanilla job, but each tuple carries a
 * {@link HowMuch} meta-data with one counter per source: PP counts the person
 * registrations and PA the opened auctions that contributed to the output
 * (docs/workflows nexmark_q3 paths).
 *
 * <pre>
 *   Persons(PP=1)  -- Filter(state in {OR,ID,CA}) -- Map --+
 *                                                          Join(12h tumbling, merge counts) -- Sink
 *   Auctions(PA=1) -- Filter(category = 10) -------- Map --+
 * </pre>
 *
 * Output per line: personId,timestamp,name,city,state,auctionId,countPP,countPA
 */
public class NexmarkLocalItemSuggestionCaps {

  // How-much provenance channels: one per source.
  public static final int PP = 0;  // person stream
  public static final int PA = 1;  // auction stream
  public static final int CHANNELS = 2;

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: NexmarkLocalItemSuggestionCaps <personsFile> <auctionsFile>"
          + " <outputFile> [mode benchDir params...]   (mode: queries | expiry |"
          + " summarize | memory_volume, see BenchSinks / CapsMetadataProbe)");
      System.exit(1);
    }
    final String personsFile = args[0];
    final String auctionsFile = args[1];
    final String outputFile = args[2];
    final boolean memoryVolume = CapsMetadataProbe.enabled(args, 3);
    final boolean live = LiveState.enabled(args, 3);
    final boolean benchMode = args.length > 3 && !memoryVolume && !live;

    // Environment, parallel disordered source and watermarks (CAPS_* variables, see DisorderConfig). The
    // defaults are the ordered run: the periodic bounded-out-of-orderness watermark with delta = 0, no
    // disorder. Same wiring in the CAPS and the no-provenance job.
    final DisorderConfig cfg = DisorderConfig.fromEnv();
    final StreamExecutionEnvironment env = Disorder.environment(cfg);

    DataStream<Event> persons = Disorder.source(env, cfg, "SOURCE-PERSONS", personsFile,
        new EventParser(personsFile, true), e -> e.timestamp, Event.class)
        // Standard Nexmark Q3 state filter; do not change or outputs diverge from the baseline.
        .filter(e -> "OR".equals(e.state) || "ID".equals(e.state) || "CA".equals(e.state))
        .name("FILTER-STATE");

    DataStream<Event> auctions = Disorder.source(env, cfg, "SOURCE-AUCTIONS", auctionsFile,
        new EventParser(personsFile, false), e -> e.timestamp, Event.class)
        .filter(e -> e.category == 10)
        .name("FILTER-CATEGORY");

    IndexingSink.HmFn<Suggestion> hm =
        (IndexingSink.HmFn<Suggestion>) s -> s.hm;

    // 12h tumbling matches Nexmark Q3; provenance tags enter at the join below.
    DataStream<Suggestion> joined = persons
        .join(auctions)
        .where(e -> e.personId)
        .equalTo(e -> e.personId)
        .window(TumblingEventTimeWindows.of(Time.hours(12)))
        .trigger(LiveState.trigger(Disorder.trigger(cfg, Time.hours(12)), "JOIN", 0, Time.hours(12), null, null))
        .with(new SuggestionJoin())
        .name("JOIN")
        .process(new StampDerivedTime<Suggestion>((t, ts) -> t.timestamp = ts))
        .returns(Suggestion.class)
        .name("STAMP-JOIN");
    joined = CapsMetadataProbe.attach(joined, memoryVolume, "JOIN", hm);

    if (live) {
      // live_memory: discard sink that publishes the live-state registry when the stream ends
      joined
          .addSink(new LiveStateSink<Suggestion>(x -> x.hm.channels() * 8L))
          .name("SINK");
    } else if (memoryVolume) {
      joined
          .addSink(new CapsCardinalityDiscardSink<Suggestion>())
          .name("SINK");
    } else if (benchMode) {
      // Index sinks run at the job's parallelism, one shard per subtask, and receive the watermarks.
      BenchSinks.attach(joined, BenchSinks.create(args, 3,
              (IndexingSink.TsFn<Suggestion>) s -> s.timestamp,
              (IndexingSink.HmFn<Suggestion>) s -> s.hm,
              (temporalindex.SampleKeyFn<Suggestion>) (s, out) -> {
                out.writeLong(s.personId);
                out.writeLong(s.timestamp);
                temporalindex.LogicalSampler.writeString(out, s.name);
                temporalindex.LogicalSampler.writeString(out, s.city);
                temporalindex.LogicalSampler.writeString(out, s.state);
                out.writeLong(s.auctionId);
              },
              CHANNELS),
          "SINK-INDEX");
      // E4 (recovery): with CAPS_OUTPUT_LOG=1 every output is also logged with its id (operator, window end, key).
      OutputLogSink.attachIfEnabled(joined, args, 3, "JOIN", c -> c.timestamp, c -> c.personId + "/" + c.auctionId);
    } else {
      joined
          .addSink(new FileSink(outputFile))
          .setParallelism(1)
          .name("SINK");
    }

    JobExecutionResult result = Disorder.execute(env, "NexmarkLocalItemSuggestion-HowMuch");
    CapsMetadataProbe.writeIfEnabled(result, args, 3, "nexmark_2");
    LiveState.writeIfEnabled(result, args, 3, "nexmark_2", "caps");
  }

  // Data types
  /** A person registration (isPerson) or an opened auction. */
  public static class Event implements Serializable {
    public long personId;   // person: own id; auction: sellerID
    public long auctionId;  // auction only
    public long timestamp;
    public boolean isPerson;
    public String name = "";
    public String city = "";
    public String state = "";
    public int category;
  }

  public static class Suggestion implements Serializable {
    public long personId;
    public long timestamp;
    public String name;
    public String city;
    public String state;
    public long auctionId;
    public HowMuch hm;

    public Suggestion() {}

    public Suggestion(long personId, long timestamp, String name, String city,
        String state, long auctionId, HowMuch hm) {
      this.personId = personId;
      this.timestamp = timestamp;
      this.name = name;
      this.city = city;
      this.state = state;
      this.auctionId = auctionId;
      this.hm = hm;
    }

    @Override
    public String toString() {
      return personId + "," + timestamp + "," + name + "," + city + "," + state
          + "," + auctionId + "," + hm;
    }
  }

  // Sources
  /**
   * Parses a line of persons.txt (personID,ts_ms,name,city,state) or auctions.txt
   * (auctionID,ts_ms,sellerID,category,itemName). With CAPS_ZIPF the seller of an auction (the join key) is drawn
   * from the registered persons with a Zipf-distributed rank (persons are ordered as in persons.txt).
   */
  public static class EventParser extends DisorderSource.Parser<Event> {
    private final boolean persons;
    private final String personsFile;
    private transient Zipf zipf;
    private transient long[] personIds;

    public EventParser(String personsFile, boolean persons) {
      this.personsFile = personsFile;
      this.persons = persons;
    }

    @Override
    public void open(DisorderConfig cfg) throws Exception {
      if (persons || cfg.zipfExponent <= 0) return;
      java.util.ArrayList<Long> ids = new java.util.ArrayList<>();
      try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(personsFile))) {
        String l;
        while ((l = br.readLine()) != null)
          if (!l.trim().isEmpty()) ids.add(Long.parseLong(l.substring(0, l.indexOf(','))));
      }
      personIds = new long[ids.size()];
      for (int i = 0; i < personIds.length; i++) personIds[i] = ids.get(i);
      zipf = new Zipf(personIds.length, cfg.zipfExponent, cfg.zipfSeed);
    }

    @Override
    public Event parse(String line, long lineNo) {
      String[] parts = line.split(",", 5);
      Event e = new Event();
      e.timestamp = Long.parseLong(parts[1]);
      e.isPerson = persons;
      if (persons) {
        e.personId = Long.parseLong(parts[0]);
        e.name = parts[2];
        e.city = parts[3];
        e.state = parts[4];
      } else {
        e.auctionId = Long.parseLong(parts[0]);
        e.personId = zipf == null ? Long.parseLong(parts[2]) : personIds[zipf.rank(lineNo, 0)];
        e.category = Integer.parseInt(parts[3]);
      }
      return e;
    }
  }

  // Operators
  /**
   * Shapes the output (name, city, state, auction.id) of each joined pair,
   * merging the how-much counts of both sources: PP=1, PA=1.
   */
  public static class SuggestionJoin implements JoinFunction<Event, Event, Suggestion> {
    // How-much provenance enters here: one count on PP and one on PA.
    @Override
    public Suggestion join(Event person, Event auction) {
      return new Suggestion(
          person.personId,
          // Placeholder: StampDerivedTime sets the window end after the join.
          0,
          person.name,
          person.city,
          person.state,
          auction.auctionId,
          HowMuch.merge(HowMuch.one(PP, CHANNELS), HowMuch.one(PA, CHANNELS)));
    }
  }

  // Sink
  public static class FileSink extends RichSinkFunction<Suggestion> {
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
    public void invoke(Suggestion value, Context context) {
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
