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

/**
 * No-provenance (no-provenance) Nexmark Q3 Local Item Suggestion dataflow
 * (docs/workflows nexmark_q3): who is selling in OR, ID or CA in category 10,
 * and for what auction ids?
 *
 * <pre>
 *   Persons  ── Filter(state ∈ {OR,ID,CA}) ── Map ──┐
 *                                                    Join(person.id = auction.seller, 12h tumbling) ── Sink
 *   Auctions ── Filter(category = 10) ──────── Map ──┘
 * </pre>
 *
 * Output per line: personId,timestamp,name,city,state,auctionId
 *
 * Usage:
 *   flink run --class NexmarkLocalItemSuggestion nexmark-2-1.0-SNAPSHOT.jar
 *       &lt;personsFile&gt; &lt;auctionsFile&gt; &lt;outputFile&gt;
 */
public class NexmarkLocalItemSuggestion {

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println(
          "Usage: NexmarkLocalItemSuggestion <personsFile> <auctionsFile> <outputFile>");
      System.exit(1);
    }
    final String personsFile = args[0];
    final String auctionsFile = args[1];
    final String outputFile = args[2];
    final double sampleProb = args.length > 3 ? Double.parseDouble(args[3]) : 0.0;

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    // Mirrors NexmarkLocalItemSuggestionCaps: same filters, join, and 12h tumbling window.
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

    persons
        .join(auctions)
        .where(e -> e.personId)
        .equalTo(e -> e.personId)
        .window(TumblingEventTimeWindows.of(Time.hours(12)))
        .apply(new SuggestionJoin())
        // Sink flips the same sampling coin as the CAPS bench sink; only provenance is missing.
        .addSink(new FileSink(outputFile, sampleProb))
        .name("SINK");

    env.execute("NexmarkLocalItemSuggestion-No-provenance");
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

    public Suggestion() {}

    public Suggestion(long personId, long timestamp, String name, String city,
        String state, long auctionId) {
      this.personId = personId;
      this.timestamp = timestamp;
      this.name = name;
      this.city = city;
      this.state = state;
      this.auctionId = auctionId;
    }

    @Override
    public String toString() {
      return personId + "," + timestamp + "," + name + "," + city + "," + state
          + "," + auctionId;
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

  /** Shapes the output (name, city, state, auction.id) of each joined pair. */
  public static class SuggestionJoin implements JoinFunction<Event, Event, Suggestion> {
    @Override
    public Suggestion join(Event person, Event auction) {
      return new Suggestion(
          person.personId,
          Math.max(person.timestamp, auction.timestamp),
          person.name,
          person.city,
          person.state,
          auction.auctionId);
    }
  }

  // ── Sink ───────────────────────────────────────────────────

  public static class FileSink extends RichSinkFunction<Suggestion> {
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
    private transient temporalindex.SampleKeyFn<Suggestion> keyFn;
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
      keyFn = new temporalindex.SampleKeyFn<Suggestion>() {
        @Override
        public void writeKey(Suggestion v, java.io.DataOutput out)
            throws java.io.IOException {
          out.writeLong(v.personId);
          out.writeLong(v.timestamp);
          temporalindex.LogicalSampler.writeString(out, v.name);
          temporalindex.LogicalSampler.writeString(out, v.city);
          temporalindex.LogicalSampler.writeString(out, v.state);
          out.writeLong(v.auctionId);
        }
      };
      predicateChecksum = 0L;
      selected = 0;
    }

    @Override
    public void invoke(Suggestion value, Context context) {
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
