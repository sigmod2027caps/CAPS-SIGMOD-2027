import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import temporalindex.Disorder;
import temporalindex.DisorderConfig;
import temporalindex.DisorderSource;
import temporalindex.Zipf;
import temporalindex.StampDerivedTime;

/**
 * No-provenance (no-provenance) Nexmark Q8 Monitor New Users dataflow (docs/workflows
 * nexmark_q8): per 12-hour tumbling window, the users who registered and
 * opened at least one auction within the window, with their auction count.
 *
 * <pre>
 *   Persons  -- Map --+
 *                      Join(person.id = auction.seller, 12h tumbling)
 *   Auctions -- Map --+        |
 *                      Filter(auction.ts >= person.ts) -- Agg(count/person, 12h) -- Sink
 * </pre>
 *
 * Output per line: personId,timestamp,nAuctions,0.0
 *
 * Usage:
 *   flink run --class NexmarkMonitorNewUsers nexmark-1-1.0-REV-SNAPSHOT.jar
 *       &lt;personsFile&gt; &lt;auctionsFile&gt; &lt;outputFile&gt;
 */
public class NexmarkMonitorNewUsers {

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println(
          "Usage: NexmarkMonitorNewUsers <personsFile> <auctionsFile> <outputFile>");
      System.exit(1);
    }
    final String personsFile = args[0];
    final String auctionsFile = args[1];
    final String outputFile = args[2];
    // Optional sampleProb: sink still hashes every output so the coin cost matches CAPS bench mode.
    final double sampleProb = args.length > 3 ? Double.parseDouble(args[3]) : 0.0;

    // Environment, parallel disordered source and watermarks (CAPS_* variables, see DisorderConfig). The
    // defaults are the ordered run: the periodic bounded-out-of-orderness watermark with delta = 0, no
    // disorder. Same wiring in the CAPS and the no-provenance job.
    final DisorderConfig cfg = DisorderConfig.fromEnv();
    final StreamExecutionEnvironment env = Disorder.environment(cfg);

    DataStream<Event> persons = Disorder.source(env, cfg, "SOURCE-PERSONS", personsFile,
        new EventParser(personsFile, true), e -> e.timestamp, Event.class);

    DataStream<Event> auctions = Disorder.source(env, cfg, "SOURCE-AUCTIONS", auctionsFile,
        new EventParser(personsFile, false), e -> e.timestamp, Event.class);

    // Join persons with the auctions they opened in the same 12h window,
    // keeping only auctions opened at or after the registration.
    DataStream<PersonCount> newUserAuctions = persons
        .join(auctions)
        .where(e -> e.personId)
        .equalTo(e -> e.personId)
        .window(TumblingEventTimeWindows.of(Time.hours(12)))
        .trigger(Disorder.trigger(cfg, Time.hours(12)))
        .with(new NewUserJoin())
        .name("JOIN")
        .process(new StampDerivedTime<PersonCount>((t, ts) -> t.timestamp = ts))
        .returns(PersonCount.class)
        .name("STAMP-JOIN");

    // Count the auctions of each new user inside the window
    OutputTag<PersonCount> lateAggCount = Disorder.lateTag("AGG-COUNT", newUserAuctions);
    SingleOutputStreamOperator<PersonCount> opAggCount = Disorder.lateOutput(newUserAuctions
        .keyBy(c -> c.personId)
        .window(TumblingEventTimeWindows.of(Time.hours(12)))
        .trigger(Disorder.trigger(cfg, Time.hours(12))), lateAggCount, cfg)
        .aggregate(new CountAggregate())
        .name("AGG-COUNT");
    Disorder.lateSink(opAggCount, lateAggCount, "AGG-COUNT", cfg);
    opAggCount
        .process(new StampDerivedTime<PersonCount>((t, ts) -> t.timestamp = ts))
        .returns(PersonCount.class)
        .name("STAMP-AGG-COUNT")
        .addSink(new FileSink(outputFile, sampleProb))
        // The discard sink of the timing runs follows CAPS_SINK_PARALLELISM like the CAPS sinks; a file sink stays on one subtask.
        .setParallelism("none".equals(outputFile) ? cfg.sinkParallelism : 1)
        .name("SINK");

    Disorder.execute(env, "NexmarkMonitorNewUsers-No-provenance");
  }

  // Data types
  /** A person registration (isPerson) or an opened auction. */
  public static class Event implements Serializable {
    public long personId;   // person: own id; auction: sellerID
    public long auctionId;  // auction only
    public long timestamp;
    public boolean isPerson;
  }

  public static class PersonCount implements Serializable {
    public long personId;
    public long timestamp;
    public double value1;
    public double value2;

    public PersonCount() {}

    public PersonCount(long personId, long timestamp, double value1, double value2) {
      this.personId = personId;
      this.timestamp = timestamp;
      this.value1 = value1;
      this.value2 = value2;
    }

    @Override
    public String toString() {
      return personId + "," + timestamp + "," + value1 + "," + value2;
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
      } else {
        e.auctionId = Long.parseLong(parts[0]);
        e.personId = zipf == null ? Long.parseLong(parts[2]) : personIds[zipf.rank(lineNo, 0)];
      }
      return e;
    }
  }

  // Operators
  /**
   * Emits one pair per (person, auction) of the window where the auction was
   * opened at or after the registration (the F of the diagram). Pair value:
   * (personId, auction open time, 1 auction).
   */
  public static class NewUserJoin implements FlatJoinFunction<Event, Event, PersonCount> {
    @Override
    public void join(Event person, Event auction, Collector<PersonCount> out) {
      if (auction.timestamp >= person.timestamp) {
        out.collect(new PersonCount(person.personId,
            // Placeholder: StampDerivedTime sets the window end after the join.
            0, 1, 0));
      }
    }
  }

  /** Counts the joined auctions of a person inside the window. */
  public static class CountAggregate
      implements AggregateFunction<PersonCount, CountAggregate.Acc, PersonCount> {

    @Override
    public Acc createAccumulator() {
      return new Acc();
    }

    @Override
    public Acc add(PersonCount value, Acc acc) {
      acc.personId = value.personId;
      acc.count++;
      return acc;
    }

    @Override
    public PersonCount getResult(Acc acc) {
      // Placeholder: StampDerivedTime sets the window end after the operator.
      return new PersonCount(acc.personId, 0, acc.count, 0);
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.personId = b.personId;
      a.count += b.count;
      return a;
    }

    public static class Acc implements Serializable {
      long personId;
      long count;
    }
  }

  // Sink
  public static class FileSink extends RichSinkFunction<PersonCount> {
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
    private transient temporalindex.SampleKeyFn<PersonCount> keyFn;
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
      keyFn = new temporalindex.SampleKeyFn<PersonCount>() {
        @Override
        public void writeKey(PersonCount v, java.io.DataOutput out)
            throws java.io.IOException {
          out.writeLong(v.personId);
          out.writeLong(v.timestamp);
          temporalindex.LogicalSampler.writeDoubleBits(out, v.value1);
          temporalindex.LogicalSampler.writeDoubleBits(out, v.value2);
        }
      };
      predicateChecksum = 0L;
      selected = 0;
    }

    @Override
    public void invoke(PersonCount value, Context context) {
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
