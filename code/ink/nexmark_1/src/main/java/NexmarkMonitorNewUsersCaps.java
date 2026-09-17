import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
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
import org.apache.flink.util.Collector;

import ink.InkBenchSinks;
import ink.InkCardinalityDiscardSink;
import ink.InkMeta;
import ink.InkMetadataProbe;
import ink.InkQuerySink;
import ink.MonomialBuilder;

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
 *   Persons(PP=1)  ── Map ──┐
 *                            Join(person.id = auction.seller, 12h tumbling, merge counts)
 *   Auctions(PA=1) ── Map ──┘        │
 *                            Filter(auction.ts >= person.ts) ── Agg(count/person, 12h, sum counts) ── Sink
 * </pre>
 *
 * Output per line: personId,timestamp,nAuctions,0.0,countPP,countPA
 */
public class NexmarkMonitorNewUsersCaps {

  // How-much provenance channels: one per source.
  public static final int PP = 0;  // person stream
  public static final int PA = 1;  // auction stream
  public static final int CHANNELS = 2;

  // Reorder slack for the online index: output timestamp disorder is bounded
  // by the 12-hour window; doubled for safety.
  public static final long INDEX_SLACK_MS = 2 * 12 * 60 * 60 * 1000L;

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
    final boolean benchMode = args.length > 3 && !memoryVolume;

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setParallelism(1);

    DataStream<Event> persons = env
        .addSource(new EventSource(personsFile, true))
        .name("SOURCE-PERSONS")
        .assignTimestampsAndWatermarks(new EventWatermarks());

    DataStream<Event> auctions = env
        .addSource(new EventSource(auctionsFile, false))
        .name("SOURCE-AUCTIONS")
        .assignTimestampsAndWatermarks(new EventWatermarks());

    InkQuerySink.MetaFn<PersonCount> meta =
        (InkQuerySink.MetaFn<PersonCount>) c -> c.hm;

    // Join persons with the auctions they opened in the same 12h window,
    // keeping only auctions opened at or after the registration.
    DataStream<PersonCount> newUserAuctions = persons
        .join(auctions)
        .where(e -> e.personId)
        .equalTo(e -> e.personId)
        .window(TumblingEventTimeWindows.of(Time.hours(12)))
        .apply(new NewUserJoin());
    newUserAuctions = InkMetadataProbe.attach(newUserAuctions, memoryVolume, "JOIN", meta);

    // Count the auctions of each new user inside the window
    DataStream<PersonCount> counted = newUserAuctions
        .keyBy(c -> c.personId)
        .window(TumblingEventTimeWindows.of(Time.hours(12)))
        .aggregate(new CountAggregate())
        .name("AGG-COUNT");
    counted = InkMetadataProbe.attach(counted, memoryVolume, "AGG-COUNT", meta);

    if (memoryVolume) {
      counted
          .addSink(new InkCardinalityDiscardSink<PersonCount>())
          .name("SINK");
    } else if (benchMode) {
      counted
          .addSink(InkBenchSinks.create(args, 3,
              (InkQuerySink.TsFn<PersonCount>) c -> c.timestamp,
              (InkQuerySink.MetaFn<PersonCount>) c -> c.hm,
              (temporalindex.SampleKeyFn<PersonCount>) (c, out) -> {
                out.writeLong(c.personId);
                out.writeLong(c.timestamp);
                temporalindex.LogicalSampler.writeDoubleBits(out, c.value1);
                temporalindex.LogicalSampler.writeDoubleBits(out, c.value2);
              },
              CHANNELS, INDEX_SLACK_MS))
          .name("SINK-INDEX");
    } else {
      counted
          .addSink(new FileSink(outputFile))
          .name("SINK");
    }

    InkMetadataProbe.writeIfEnabled(env.execute("NexmarkMonitorNewUsers-HowMuch"),
        args, 3, "nexmark_1");
  }

  // ── Data types ──────────────────────────────────────────────

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
      return personId + "," + timestamp + "," + value1 + "," + value2 + "," + hm;
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
          } else {
            e.auctionId = Long.parseLong(parts[0]);
            e.personId = Long.parseLong(parts[2]);
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
   * Emits one pair per (person, auction) of the window where the auction was
   * opened at or after the registration. The pair multiplies the polynomial
   * annotations of both sources: {@code x_PP * x_PA}.
   */
  public static class NewUserJoin implements FlatJoinFunction<Event, Event, PersonCount> {
    // Per-operator-instance source-tuple counter; person and auction are
    // first tagged here. See InkMeta.source.
    private transient long seq;

    @Override
    public void join(Event person, Event auction, Collector<PersonCount> out) {
      if (auction.timestamp >= person.timestamp) {
        out.collect(new PersonCount(person.personId,
            Math.max(person.timestamp, auction.timestamp), 1, 0,
            InkMeta.source(PP, CHANNELS, seq++).times(InkMeta.source(PA, CHANNELS, seq++))));
      }
    }
  }

  /** Counts the joined auctions of a person, multiplying provenance annotations. */
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
      acc.maxTs = Math.max(acc.maxTs, value.timestamp);
      // Conjunctive aggregate: multiply polynomials, do not add them.
      acc.hm.mul(value.hm);
      return acc;
    }

    @Override
    public PersonCount getResult(Acc acc) {
      return new PersonCount(acc.personId, acc.maxTs, acc.count, 0, acc.hm.toMeta());
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      a.personId = b.personId;
      a.count += b.count;
      a.maxTs = Math.max(a.maxTs, b.maxTs);
      a.hm.mul(b.hm);
      return a;
    }

    public static class Acc implements Serializable {
      long personId;
      long count;
      long maxTs;
      MonomialBuilder hm = new MonomialBuilder();
    }
  }

  // ── Sink ───────────────────────────────────────────────────

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
