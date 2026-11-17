import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import org.apache.flink.api.common.JobExecutionResult;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;

import ink.InkBenchSinks;
import ink.InkCardinalityDiscardSink;
import ink.InkMeta;
import ink.InkMetadataProbe;
import ink.InkQuerySink;
import ink.VarSource;
import temporalindex.LiveState;
import temporalindex.Zipf;
import temporalindex.DisorderSource;
import temporalindex.DisorderConfig;
import temporalindex.Disorder;
import temporalindex.LiveStateSink;
import temporalindex.StampDerivedTime;

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

  // Reorder slack for the online index. Every windowed operator stamps its output with
  // the right boundary of its window (StampDerivedTime), so at parallelism 1 the sink
  // input is already in timestamp order and the reorder buffer releases at once.
  public static final long INDEX_SLACK_MS = 0;

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
    final boolean live = LiveState.enabled(args, 3);
    final boolean benchMode = args.length > 3 && !memoryVolume && !live;

    // Same environment, source and watermark assigner as the CAPS and no-provenance jobs (CAPS_* variables, see
    // DisorderConfig): the defaults are the ordered run with a periodic watermark, no disorder.
    final DisorderConfig cfg = DisorderConfig.fromEnv();
    final StreamExecutionEnvironment env = Disorder.environment(cfg);

    DataStream<Event> persons = Disorder.source(env, cfg, "SOURCE-PERSONS", personsFile,
        new EventParser(personsFile, true), e -> e.timestamp, Event.class)
        // Standard Nexmark Q3 state filter; do not change or outputs diverge from the baseline.
        .filter(e -> "OR".equals(e.state) || "ID".equals(e.state) || "CA".equals(e.state))
        .name("FILTER-STATE")
        .map(new TagSource(PP))
        .name("TAG-PERSONS");

    DataStream<Event> auctions = Disorder.source(env, cfg, "SOURCE-AUCTIONS", auctionsFile,
        new EventParser(personsFile, false), e -> e.timestamp, Event.class)
        .filter(e -> e.category == 10)
        .name("FILTER-CATEGORY")
        .map(new TagSource(PA))
        .name("TAG-AUCTIONS");

    InkQuerySink.MetaFn<Suggestion> meta =
        (InkQuerySink.MetaFn<Suggestion>) s -> s.hm;

    // Green tuples carry their variable from the source on (TagSource), so the join buffer holds them.
    final LiveState.Sizer<Event> eventBytes = e -> e.hm.metadataBytes();
    DataStream<Suggestion> joined = persons
        .join(auctions)
        .where(e -> e.personId)
        .equalTo(e -> e.personId)
        .window(TumblingEventTimeWindows.of(Time.hours(12)))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "JOIN", 0, Time.hours(12), null,
            LiveState.join(eventBytes, eventBytes)))
        .apply(new SuggestionJoin())
        .process(new StampDerivedTime<Suggestion>((t, ts) -> t.timestamp = ts))
        .returns(Suggestion.class)
        .name("STAMP-JOIN");
    joined = InkMetadataProbe.attach(joined, memoryVolume, "JOIN", meta);

    if (live) {
      // live_memory: discard sink that publishes the live-state registry when the stream ends
      joined
          .addSink(new LiveStateSink<Suggestion>(x -> x.hm.metadataBytes()))
          .name("SINK");
    } else if (memoryVolume) {
      joined
          .addSink(new InkCardinalityDiscardSink<Suggestion>())
          .name("SINK");
    } else if (benchMode) {
      InkBenchSinks.attach(joined, InkBenchSinks.create(args, 3,
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
              CHANNELS, INDEX_SLACK_MS),
          "SINK-INDEX");
    } else {
      joined
          .addSink(new FileSink(outputFile, CHANNELS))
          .setParallelism(1)
          .name("SINK");
    }

    JobExecutionResult result = env.execute("NexmarkLocalItemSuggestion-HowMuch");
    InkMetadataProbe.writeIfEnabled(result, args, 3, "nexmark_2");
    LiveState.writeIfEnabled(result, args, 3, "nexmark_2", "green_polynomial");
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
    public InkMeta hm;      // the variable of this source tuple, minted after the filter (TagSource)
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
          + "," + auctionId;
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
   * Mints the variable of every source tuple that passed the filter (one per person, one per auction) right after
   * the source side of the dataflow, as the other dataflows do in their first map. A person that joins k auctions
   * carries the same variable in all k pairs.
   */
  public static class TagSource extends RichMapFunction<Event, Event> {
    private final int channel;
    // Source-tuple variables of this operator instance, unique over its subtasks; see InkMeta, VarSource.
    private final VarSource vars = new VarSource(CHANNELS);

    public TagSource(int channel) {
      this.channel = channel;
    }

    @Override
    public void open(Configuration parameters) {
      vars.open(getRuntimeContext());
    }

    @Override
    public Event map(Event e) {
      e.hm = vars.next(channel);
      return e;
    }
  }

  /**
   * Shapes the output (name, city, state, auction.id) of each joined pair,
   * merging the how-much counts of both sources: PP=1, PA=1.
   */
  public static class SuggestionJoin implements JoinFunction<Event, Event, Suggestion> {
    @Override
    public Suggestion join(Event person, Event auction) {
      // One monomial per source tuple; the join is their product.
      return new Suggestion(
          person.personId,
          // Placeholder: StampDerivedTime sets the window end after the join.
          0,
          person.name,
          person.city,
          person.state,
          auction.auctionId,
          person.hm.times(auction.hm));
    }
  }

  // Sink
  public static class FileSink extends RichSinkFunction<Suggestion> {
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
    public void invoke(Suggestion value, Context context) {
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
