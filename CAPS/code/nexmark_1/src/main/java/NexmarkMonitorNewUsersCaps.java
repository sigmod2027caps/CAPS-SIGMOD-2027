import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import org.apache.flink.api.common.JobExecutionResult;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

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
 * How-much provenance Nexmark Q8 Monitor New Users dataflow.
 *
 * Identical computation to the vanilla job, but each tuple carries a
 * {@link HowMuch} meta-data with one counter per source: PP counts the person
 * registrations and PA the opened auctions that contributed to the output
 * (docs/workflows nexmark_q8 paths).
 *
 * <pre>
 *   Persons(PP=1)  -- Map --+
 *                            Join(person.id = auction.seller, 12h tumbling, merge counts)
 *   Auctions(PA=1) -- Map --+        |
 *                            Filter(auction.ts >= person.ts) -- Agg(count/person, 12h, sum counts) -- Sink
 * </pre>
 *
 * Output per line: personId,timestamp,nAuctions,0.0,countPP,countPA
 */
public class NexmarkMonitorNewUsersCaps {

  // How-much provenance channels: one per source.
  public static final int PP = 0;  // person stream
  public static final int PA = 1;  // auction stream
  public static final int CHANNELS = 2;

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: NexmarkMonitorNewUsersCaps <personsFile> <auctionsFile>"
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
        new EventParser(personsFile, true), e -> e.timestamp, Event.class);

    DataStream<Event> auctions = Disorder.source(env, cfg, "SOURCE-AUCTIONS", auctionsFile,
        new EventParser(personsFile, false), e -> e.timestamp, Event.class);

    IndexingSink.HmFn<PersonCount> hm =
        (IndexingSink.HmFn<PersonCount>) c -> c.hm;

    // Join persons with the auctions they opened in the same 12h window,
    // keeping only auctions opened at or after the registration.
    DataStream<PersonCount> newUserAuctions = persons
        .join(auctions)
        .where(e -> e.personId)
        .equalTo(e -> e.personId)
        // 12h tumbling matches Nexmark Q8; smaller windows change the query.
        .window(TumblingEventTimeWindows.of(Time.hours(12)))
        .trigger(LiveState.trigger(Disorder.trigger(cfg, Time.hours(12)), "JOIN", 0, Time.hours(12), null, null))
        .with(new NewUserJoin())
        .name("JOIN")
        .process(new StampDerivedTime<PersonCount>((t, ts) -> t.timestamp = ts))
        .returns(PersonCount.class)
        .name("STAMP-JOIN");
    newUserAuctions = CapsMetadataProbe.attach(newUserAuctions, memoryVolume, "JOIN", hm);

    // Count the auctions of each new user inside the window
    OutputTag<PersonCount> lateAggCount = Disorder.lateTag("AGG-COUNT", newUserAuctions);
    SingleOutputStreamOperator<PersonCount> opAggCount = Disorder.lateOutput(newUserAuctions
        .keyBy(c -> c.personId)
        .window(TumblingEventTimeWindows.of(Time.hours(12)))
        .trigger(LiveState.trigger(Disorder.trigger(cfg, Time.hours(12)), "AGG-COUNT", 1, Time.hours(12), LiveState.constant(CHANNELS * 8L), null)), lateAggCount, cfg)
        .aggregate(new CountAggregate())
        .name("AGG-COUNT");
    Disorder.lateSink(opAggCount, lateAggCount, "AGG-COUNT", cfg);
    DataStream<PersonCount> counted = opAggCount
        .process(new StampDerivedTime<PersonCount>((t, ts) -> t.timestamp = ts))
        .returns(PersonCount.class)
        .name("STAMP-AGG-COUNT");
    counted = CapsMetadataProbe.attach(counted, memoryVolume, "AGG-COUNT", hm);

    if (live) {
      // live_memory: discard sink that publishes the live-state registry when the stream ends
      counted
          .addSink(new LiveStateSink<PersonCount>(x -> x.hm.channels() * 8L))
          .name("SINK");
    } else if (memoryVolume) {
      counted
          .addSink(new CapsCardinalityDiscardSink<PersonCount>())
          .name("SINK");
    } else if (benchMode) {
      // Bench sink: same sample coin as noprov, plus the how-much temporal index.
      // Index sinks run at the job's parallelism, one shard per subtask, and receive the watermarks.
      BenchSinks.attach(counted, BenchSinks.create(args, 3,
              (IndexingSink.TsFn<PersonCount>) c -> c.timestamp,
              (IndexingSink.HmFn<PersonCount>) c -> c.hm,
              (temporalindex.SampleKeyFn<PersonCount>) (c, out) -> {
                out.writeLong(c.personId);
                out.writeLong(c.timestamp);
                temporalindex.LogicalSampler.writeDoubleBits(out, c.value1);
                temporalindex.LogicalSampler.writeDoubleBits(out, c.value2);
              },
              CHANNELS),
          "SINK-INDEX");
      // E4 (recovery): with CAPS_OUTPUT_LOG=1 every output is also logged with its id (operator, window end, key).
      OutputLogSink.attachIfEnabled(counted, args, 3, "AGG-COUNT", c -> c.timestamp, c -> Long.toString(c.personId));
    } else {
      counted
          .addSink(new FileSink(outputFile))
          .setParallelism(1)
          .name("SINK");
    }

    JobExecutionResult result = Disorder.execute(env, "NexmarkMonitorNewUsers-HowMuch");
    CapsMetadataProbe.writeIfEnabled(result, args, 3, "nexmark_1");
    LiveState.writeIfEnabled(result, args, 3, "nexmark_1", "caps");
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
    public HowMuch hm;

    public PersonCount() {}

    public PersonCount(long personId, long timestamp, double value1, double value2,
        HowMuch hm) {
      this.personId = personId;
      this.timestamp = timestamp;
      this.value1 = value1;
      this.value2 = value2;
      this.hm = hm;
    }

    @Override
    public String toString() {
      return personId + "," + timestamp + "," + value1 + "," + value2 + "," + hm;
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
   * opened at or after the registration. The pair merges the how-much counts
   * of both sources: PP=1 for the person, PA=1 for the auction.
   */
  public static class NewUserJoin implements FlatJoinFunction<Event, Event, PersonCount> {
    // How-much provenance enters here: PP for the person tuple, PA for the auction.
    @Override
    public void join(Event person, Event auction, Collector<PersonCount> out) {
      if (auction.timestamp >= person.timestamp) {
        out.collect(new PersonCount(person.personId,
            // Placeholder: StampDerivedTime sets the window end after the join.
            0, 1, 0,
            HowMuch.merge(HowMuch.one(PP, CHANNELS), HowMuch.one(PA, CHANNELS))));
      }
    }
  }

  /** Counts the joined auctions of a person, merging provenance counts. */
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
      acc.hm.mergeWith(value.hm);
      return acc;
    }

    @Override
    public PersonCount getResult(Acc acc) {
      // Placeholder: StampDerivedTime sets the window end after the operator.
      return new PersonCount(acc.personId, 0, acc.count, 0, acc.hm.copy());
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.personId = b.personId;
      a.count += b.count;
      a.hm.mergeWith(b.hm);
      return a;
    }

    public static class Acc implements Serializable {
      long personId;
      long count;
      HowMuch hm = new HowMuch(CHANNELS);
    }
  }

  // Sink
  public static class FileSink extends RichSinkFunction<PersonCount> {
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
    public void invoke(PersonCount value, Context context) {
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
