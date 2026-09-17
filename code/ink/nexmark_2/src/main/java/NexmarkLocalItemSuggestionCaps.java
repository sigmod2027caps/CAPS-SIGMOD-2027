import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import ink.InkBenchSinks;
import ink.InkCardinalityDiscardSink;
import ink.InkMeta;
import ink.InkMetadataProbe;
import ink.InkQuerySink;

/**
 * How-much provenance Nexmark Q3 Local Item Suggestion dataflow.
 *
 * Identical computation to the vanilla job, but each tuple carries an
 * {@link InkMeta} polynomial annotation (green_polynomial baseline in the paper)
 * whose how-much folding is one counter per source: PP counts the person
 * registrations and PA the opened auctions that contributed to the output
 * (docs/workflows nexmark_q3 paths).
 *
 * <pre>
 *   Persons(PP=1)  ── Filter(state ∈ {OR,ID,CA}) ── Map ──┐
 *                                                          Join(12h tumbling, merge counts) ── Sink
 *   Auctions(PA=1) ── Filter(category = 10) ──────── Map ──┘
 * </pre>
 *
 * Output per line: personId,timestamp,name,city,state,auctionId,countPP,countPA
 */
public class NexmarkLocalItemSuggestionCaps {

  // How-much provenance channels: one per source.
  public static final int PP = 0;  // person stream
  public static final int PA = 1;  // auction stream
  public static final int CHANNELS = 2;

  // Reorder slack for the online index: output timestamp disorder is bounded
  // by the 12-hour window; doubled for safety.
  public static final long INDEX_SLACK_MS = 2 * 12 * 60 * 60 * 1000L;

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: NexmarkLocalItemSuggestionCaps <personsFile> <auctionsFile>"
          + " <outputFile> [mode benchDir params...]   (mode: provenance |"
          + " provenance_timer | memory_volume, see InkBenchSinks / InkMetadataProbe)");
      System.exit(1);
    }
    final String personsFile = args[0];
    final String auctionsFile = args[1];
    final String outputFile = args[2];
    final boolean memoryVolume = InkMetadataProbe.enabled(args, 3);
    final boolean benchMode = args.length > 3 && !memoryVolume;

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setParallelism(1);

    DataStream<Event> persons = env
        .addSource(new EventSource(personsFile, true))
        .name("SOURCE-PERSONS")
        .assignTimestampsAndWatermarks(new EventWatermarks())
        .filter(e -> "OR".equals(e.state) || "ID".equals(e.state) || "CA".equals(e.state))
        .name("FILTER-STATE");

    DataStream<Event> auctions = env
        .addSource(new EventSource(auctionsFile, false))
        .name("SOURCE-AUCTIONS")
        .assignTimestampsAndWatermarks(new EventWatermarks())
        .filter(e -> e.category == 10)
        .name("FILTER-CATEGORY");

    InkQuerySink.MetaFn<Suggestion> meta =
        (InkQuerySink.MetaFn<Suggestion>) s -> s.hm;

    DataStream<Suggestion> joined = persons
        .join(auctions)
        .where(e -> e.personId)
        .equalTo(e -> e.personId)
        .window(TumblingEventTimeWindows.of(Time.hours(12)))
        .apply(new SuggestionJoin());
    joined = InkMetadataProbe.attach(joined, memoryVolume, "JOIN", meta);

    if (memoryVolume) {
      joined
          .addSink(new InkCardinalityDiscardSink<Suggestion>())
          .name("SINK");
    } else if (benchMode) {
      joined
          .addSink(InkBenchSinks.create(args, 3,
              (InkQuerySink.TsFn<Suggestion>) s -> s.timestamp,
              (InkQuerySink.MetaFn<Suggestion>) s -> s.hm,
              (temporalindex.SampleKeyFn<Suggestion>) (s, out) -> {
                out.writeLong(s.personId);
                out.writeLong(s.timestamp);
                temporalindex.LogicalSampler.writeString(out, s.name);
                temporalindex.LogicalSampler.writeString(out, s.city);
                temporalindex.LogicalSampler.writeString(out, s.state);
                out.writeLong(s.auctionId);
              },
              CHANNELS, INDEX_SLACK_MS))
          .name("SINK-INDEX");
    } else {
      joined
          .addSink(new FileSink(outputFile))
          .name("SINK");
    }

    InkMetadataProbe.writeIfEnabled(env.execute("NexmarkLocalItemSuggestion-HowMuch"),
        args, 3, "nexmark_2");
  }

  // ── Data types ──────────────────────────────────────────────

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
    public InkMeta hm;

    public Suggestion() {}

    public Suggestion(long personId, long timestamp, String name, String city,
        String state, long auctionId, InkMeta hm) {
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

  // ── Sources ─────────────────────────────────────────────────

  /**
   * Reads persons.txt (personID,ts_ms,name,city,state) or
   * auctions.txt (auctionID,ts_ms,sellerID,category,itemName).
   */
  public static class EventSource extends RichSourceFunction<Event> {
    private final String path;
    private final boolean persons;
    private volatile boolean running = true;

    public EventSource(String path, boolean persons) {
      this.path = path;
      this.persons = persons;
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
      try (BufferedReader br = new BufferedReader(new FileReader(path))) {
        String line;
        while (running && (line = br.readLine()) != null) {
          String trimmed = line.trim();
          if (trimmed.isEmpty()) continue;
          String[] parts = trimmed.split(",", 5);
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
            e.personId = Long.parseLong(parts[2]);
            e.category = Integer.parseInt(parts[3]);
          }
          ctx.collect(e);
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  public static class EventWatermarks implements AssignerWithPunctuatedWatermarks<Event> {
    @Override
    public long extractTimestamp(Event e, long prev) {
      return e.timestamp;
    }

    @Override
    public Watermark checkAndGetNextWatermark(Event e, long ts) {
      return new Watermark(ts);
    }
  }

  // ── Operators ──────────────────────────────────────────────

  /**
   * Shapes the output (name, city, state, auction.id) of each joined pair,
   * merging the how-much counts of both sources: PP=1, PA=1.
   */
  public static class SuggestionJoin implements JoinFunction<Event, Event, Suggestion> {
    // Per-operator-instance source-tuple counter; person and auction are
    // first tagged here. See InkMeta.source.
    private transient long seq;

    @Override
    public Suggestion join(Event person, Event auction) {
      // One monomial per source tuple; the join is their product.
      return new Suggestion(
          person.personId,
          Math.max(person.timestamp, auction.timestamp),
          person.name,
          person.city,
          person.state,
          auction.auctionId,
          InkMeta.source(PP, CHANNELS, seq++).times(InkMeta.source(PA, CHANNELS, seq++)));
    }
  }

  // ── Sink ───────────────────────────────────────────────────

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
