import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import org.apache.flink.api.common.JobExecutionResult;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.util.Collector;

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
 * How-much provenance Nexmark Q8 Monitor New Users dataflow.
 *
 * Identical computation to the vanilla job, but each tuple carries an
 * {@link InkMeta} polynomial annotation (green_polynomial baseline in the paper;
 * package name ink is historical) whose how-much folding is one counter per source:
 * PP counts the person
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

  // Reorder slack for the online index. Every windowed operator stamps its output with
  // the right boundary of its window (StampDerivedTime), so at parallelism 1 the sink
  // input is already in timestamp order and the reorder buffer releases at once.
  public static final long INDEX_SLACK_MS = 0;

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: NexmarkMonitorNewUsersCaps <personsFile> <auctionsFile>"
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
        .map(new TagSource(PP))
        .name("TAG-PERSONS");

    DataStream<Event> auctions = Disorder.source(env, cfg, "SOURCE-AUCTIONS", auctionsFile,
        new EventParser(personsFile, false), e -> e.timestamp, Event.class)
        .map(new TagSource(PA))
        .name("TAG-AUCTIONS");

    InkQuerySink.MetaFn<PersonCount> meta =
        (InkQuerySink.MetaFn<PersonCount>) c -> c.hm;

    // Join persons with the auctions they opened in the same 12h window,
    // keeping only auctions opened at or after the registration.
    // Green tuples carry their variable from the source on (TagSource), so the join buffer holds them.
    final LiveState.Sizer<Event> eventBytes = e -> e.hm.metadataBytes();
    DataStream<PersonCount> newUserAuctions = persons
        .join(auctions)
        .where(e -> e.personId)
        .equalTo(e -> e.personId)
        .window(TumblingEventTimeWindows.of(Time.hours(12)))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "JOIN", 0, Time.hours(12), null,
            LiveState.join(eventBytes, eventBytes)))
        .apply(new NewUserJoin())
        .process(new StampDerivedTime<PersonCount>((t, ts) -> t.timestamp = ts))
        .returns(PersonCount.class)
        .name("STAMP-JOIN");
    newUserAuctions = InkMetadataProbe.attach(newUserAuctions, memoryVolume, "JOIN", meta);

    // Count the auctions of each new user inside the window
    DataStream<PersonCount> counted = newUserAuctions
        .keyBy(c -> c.personId)
        .window(TumblingEventTimeWindows.of(Time.hours(12)))
        .trigger(LiveState.trigger(EventTimeTrigger.create(), "AGG-COUNT", 1, Time.hours(12), null, null))
        .aggregate(LiveState.on() ? new CountAggregate.Live() : new CountAggregate())
        .name("AGG-COUNT")
        .process(new StampDerivedTime<PersonCount>((t, ts) -> t.timestamp = ts))
        .returns(PersonCount.class)
        .name("STAMP-AGG-COUNT");
    counted = InkMetadataProbe.attach(counted, memoryVolume, "AGG-COUNT", meta);

    if (live) {
      // live_memory: discard sink that publishes the live-state registry when the stream ends
      counted
          .addSink(new LiveStateSink<PersonCount>(x -> x.hm.metadataBytes()))
          .name("SINK");
    } else if (memoryVolume) {
      counted
          .addSink(new InkCardinalityDiscardSink<PersonCount>())
          .name("SINK");
    } else if (benchMode) {
      InkBenchSinks.attach(counted, InkBenchSinks.create(args, 3,
              (InkQuerySink.TsFn<PersonCount>) c -> c.timestamp,
              (InkQuerySink.MetaFn<PersonCount>) c -> c.hm,
              (temporalindex.SampleKeyFn<PersonCount>) (c, out) -> {
                out.writeLong(c.personId);
                out.writeLong(c.timestamp);
                temporalindex.LogicalSampler.writeDoubleBits(out, c.value1);
                temporalindex.LogicalSampler.writeDoubleBits(out, c.value2);
              },
              CHANNELS, INDEX_SLACK_MS),
          "SINK-INDEX");
    } else {
      counted
          .addSink(new FileSink(outputFile, CHANNELS))
          .setParallelism(1)
          .name("SINK");
    }

    JobExecutionResult result = env.execute("NexmarkMonitorNewUsers-HowMuch");
    InkMetadataProbe.writeIfEnabled(result, args, 3, "nexmark_1");
    LiveState.writeIfEnabled(result, args, 3, "nexmark_1", "green_polynomial");
  }

  // Data types
  /** A person registration (isPerson) or an opened auction. */
  public static class Event implements Serializable {
    public long personId;   // person: own id; auction: sellerID
    public long auctionId;  // auction only
    public long timestamp;
    public boolean isPerson;
    public InkMeta hm;      // the variable of this source tuple, minted right after the source (TagSource)
  }

  public static class PersonCount implements Serializable {
    public long personId;
    public long timestamp;
    public double value1;
    public double value2;
    public InkMeta hm;

    public PersonCount() {}

    public PersonCount(long personId, long timestamp, double value1, double value2,
        InkMeta hm) {
      this.personId = personId;
      this.timestamp = timestamp;
      this.value1 = value1;
      this.value2 = value2;
      this.hm = hm;
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
   * Mints the variable of every source tuple (one per registered person, one per opened auction) right after the
   * source, as the other dataflows do in their first map. A person that joins k auctions then carries one variable
   * in all k pairs, and the count aggregate raises it to the power k.
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
   * Emits one pair per (person, auction) of the window where the auction was
   * opened at or after the registration. The pair multiplies the polynomial
   * annotations of both sources: {@code x_PP * x_PA}.
   */
  public static class NewUserJoin implements FlatJoinFunction<Event, Event, PersonCount> {
    @Override
    public void join(Event person, Event auction, Collector<PersonCount> out) {
      if (auction.timestamp >= person.timestamp) {
        out.collect(new PersonCount(person.personId,
            // Placeholder: StampDerivedTime sets the window end after the join.
            0, 1, 0,
            person.hm.times(auction.hm)));
      }
    }
  }

  /** Counts the joined auctions of a person, multiplying provenance annotations. */
  public static class CountAggregate
      implements AggregateFunction<PersonCount, CountAggregate.Acc, PersonCount> {

    /** Same aggregate; reports the growth of its accumulator's annotation (8 B per variable) to LiveState. Used in live_memory mode only. */
    public static class Live extends CountAggregate {
      @Override
      public Acc add(PersonCount value, Acc acc) {
        long before = acc.hm.numVars();
        super.add(value, acc);
        LiveState.grow(8L * (acc.hm.numVars() - before));
        return acc;
      }
    }

    @Override
    public Acc createAccumulator() {
      return new Acc();
    }

    @Override
    public Acc add(PersonCount value, Acc acc) {
      acc.personId = value.personId;
      acc.count++;
      // Conjunctive aggregate: multiply polynomials, do not add them.
      acc.hm.mul(value.hm);
      return acc;
    }

    @Override
    public PersonCount getResult(Acc acc) {
      // Placeholder: StampDerivedTime sets the window end after the operator.
      return new PersonCount(acc.personId, 0, acc.count, 0, acc.hm.toMeta());
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.personId = b.personId;
      a.count += b.count;
      a.hm.mul(b.hm);
      return a;
    }

    public static class Acc implements Serializable {
      long personId;
      long count;
      MonomialBuilder hm = new MonomialBuilder();
    }
  }

  // Sink
  public static class FileSink extends RichSinkFunction<PersonCount> {
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
    public void invoke(PersonCount value, Context context) {
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
