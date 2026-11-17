package io.palyvos.provenance.genealog;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import temporalindex.GenealogIndex;
import tieredindex.ShardGroup;
import tieredindex.WatermarkOrderer;

/**
 * GeneaLog temporal-query baseline: the counterpart of CAPS's
 * IndexingSink. Output tuples are stored (in time order) in a {@link GenealogIndex} while the
 * dataflow runs - keeping their
 * provenance graph reachable. A time-range how-much query [ts, te] is then
 * answered the only way GeneaLog can: locate the outputs in range through the
 * index and BFS-traverse the provenance graph of EVERY one of them, counting
 * source-tuple contributions (the same how-much quantity the CAPS index
 * computes, so verify.py can compare the answers).
 *
 * At end of stream the sink benchmarks random how-much and versioning queries
 * (same seeds and range distribution as CAPS's IndexingSink; versioning =
 * two range computations + subtraction) and writes to
 * {@code indexBenchDir/query_latency_genealog.csv}:
 *   method,query,records,queries,avg_us,total_ms   (method = genealog)
 *
 * Disorder. The baseline faces the disorder CAPS faces and is treated the same way: outputs go
 * into the same watermark-driven Tier 1 (an augmented red-black tree of keys, the tuples sit in a
 * side table; {@link WatermarkOrderer}) and a watermark appends the certified prefix to the
 * GenealogIndex in timestamp order. A tuple with ts &lt;= W is late: counted and written to
 * late_genealog_s&lt;i&gt;.csv, not indexed. Every sink subtask owns one shard (Tier 1 plus GenealogIndex);
 * the shards of a sink meet in {@link ShardGroup} (the harness runs on one TaskManager JVM) and
 * the last subtask to close answers the queries as the sum of the per-shard graph traversals.
 * Statistics: tiered_stats_genealog.csv in the index bench directory.
 *
 * numQueries ({@code --indexNumQueries}) must match the CAPS run so the
 * verification covers all queries. Note one query traverses the graphs of
 * millions of outputs and takes seconds, versus microseconds for the
 * prefix-sum index.
 *
 * The GeneaLog baseline of Paper Section 7.4 (Figures 9b and 10) on the same Tier 1 as CAPS.
 */
public class GenealogIndexingSink<T extends GenealogTuple> extends RichSinkFunction<T>
    implements Consumer<Watermark>, Serializable {

  private static final long serialVersionUID = 2L;

  private final ExperimentSettings settings;
  private final String token = UUID.randomUUID().toString();
  /** CAPS_INSERT_TIMING=1, read on the client: nanoTime around every Tier-1 insert. Off in every timed run. */
  private final boolean insertTiming = "1".equals(System.getenv("CAPS_INSERT_TIMING"));

  private transient Shard shard;
  private transient int subtask;
  private transient int parallelism;

  public GenealogIndexingSink(ExperimentSettings settings) {
    this.settings = settings;
  }

  /**
   * Adds the sink at the job's parallelism (one shard per subtask), through the framework's
   * WatermarkTriggeringSink so that it receives the watermarks of its input.
   */
  public static <T extends GenealogTuple> void attach(DataStream<T> stream,
      GenealogIndexingSink<T> sink, String name) {
    WatermarkTriggeringSink.addSink(stream, name,
        stream.getExecutionEnvironment().getParallelism(), false, sink, sink);
  }

  /** State of one sink subtask. */
  static final class Shard {
    final int subtask;
    final GenealogIndex<GenealogTuple> index = new GenealogIndex<>();
    final ProvenanceAggregateStrategy aggregateStrategy;
    final WatermarkOrderer<GenealogTuple> tier1 = new WatermarkOrderer<>();
    final WatermarkOrderer.Appender<GenealogTuple> appender = (ts, t) -> index.append(ts, t);
    long records;
    long watermarks;
    long maxWMinusTs = Long.MIN_VALUE;
    long minTs = Long.MAX_VALUE;
    long maxTs = Long.MIN_VALUE;
    PrintWriter late;

    Shard(int subtask, ProvenanceAggregateStrategy aggregateStrategy) {
      this.subtask = subtask;
      this.aggregateStrategy = aggregateStrategy;
    }
  }

  @Override
  public void open(Configuration parameters) {
    try {
      subtask = getRuntimeContext().getIndexOfThisSubtask();
      parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
    } catch (IllegalStateException e) { // outside a Flink task
      subtask = 0;
      parallelism = 1;
    }
    shard = new Shard(subtask, settings.aggregateStrategySupplier().get());
    shard.tier1.timing = insertTiming;
  }

  @Override
  public void invoke(T tuple, Context context) throws Exception {
    long ts = tuple.getTimestamp();
    Shard s = shard;
    s.records++;
    long w = s.tier1.watermark();
    if (w != Long.MIN_VALUE && w - ts > s.maxWMinusTs) {
      s.maxWMinusTs = w - ts;
    }
    if (!s.tier1.add(ts, tuple)) {
      if (s.late == null) {
        s.late = new PrintWriter(new FileWriter(
            settings.indexBenchDir() + "/late_genealog_s" + subtask + ".csv"));
        s.late.println("ts,watermark");
      }
      s.late.println(ts + "," + w);
      return;
    }
    if (ts < s.minTs) {
      s.minTs = ts;
    }
    if (ts > s.maxTs) {
      s.maxTs = ts;
    }
  }

  /** Watermark callback, delivered by WatermarkTriggeringSink. */
  @Override
  public void accept(Watermark mark) {
    shard.watermarks++;
    shard.tier1.advanceWatermark(mark.getTimestamp(), shard.appender);
  }

  @Override
  public void close() throws Exception {
    if (shard == null) {
      return;
    }
    shard.tier1.advanceWatermark(Long.MAX_VALUE, shard.appender); // end of input
    if (shard.late != null) {
      shard.late.close();
    }
    List<Shard> all = ShardGroup.arrive(token, parallelism, subtask, shard);
    if (all == null) {
      return;
    }
    long size = 0;
    for (Shard s : all) {
      size += s.index.size();
    }
    if (size == 0) {
      throw new IllegalStateException("no records reached the genealog indexing sink");
    }
    shards = all;
    writeStats();
    if (settings.indexQueryLensMs() != null) {
      benchLengthSweep();
    } else {
      bench();
    }
  }

  /** The shards, set by the last subtask to close (transient: only that subtask's copy uses it). */
  private transient List<Shard> shards;

  private int indexSize() {
    int n = 0;
    for (Shard s : shards) {
      n += s.index.size();
    }
    return n;
  }

  private long minTs() {
    long m = Long.MAX_VALUE;
    for (Shard s : shards) {
      m = Math.min(m, s.minTs);
    }
    return m;
  }

  private long maxTs() {
    long m = Long.MIN_VALUE;
    for (Shard s : shards) {
      m = Math.max(m, s.maxTs);
    }
    return m;
  }

  private void writeStats() throws IOException {
    try (PrintWriter pw = new PrintWriter(new FileWriter(
        settings.indexBenchDir() + "/tiered_stats_genealog.csv"))) {
      pw.println("sink,shard,parallelism,records,indexed,late,migrated,tier1_peak,tier1_final,"
          + "insert_ns,migrate_ns,watermarks,max_w_minus_ts_ms,min_ts,max_ts");
      for (Shard s : shards) {
        WatermarkOrderer<GenealogTuple> t = s.tier1;
        pw.printf(Locale.ROOT, "genealog,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s,%d,%d%n",
            s.subtask, shards.size(), s.records, s.records - t.lateCount, t.lateCount,
            t.migratedCount, t.peakTier1, t.tier1Size(), t.insertNanos, t.migrationNanos,
            s.watermarks,
            s.maxWMinusTs == Long.MIN_VALUE ? "NA" : Long.toString(s.maxWMinusTs),
            s.minTs, s.maxTs);
      }
    }
  }

  private void bench() throws Exception {
    final int numQueries = settings.indexNumQueries();
    final long queryLenMs = settings.indexQueryLenMs();
    if (queryLenMs <= 0) {
      throw new IllegalStateException("--indexQueryLenMs is required for the query bench");
    }
    long minTs = minTs();
    long maxTs = maxTs();
    long[][] howMuchQ = randomRanges(numQueries, minTs, maxTs, queryLenMs, new Random(42));
    long[][] versionW1 = randomRanges(numQueries, minTs, maxTs, queryLenMs, new Random(43));
    long[][] versionW2 = randomRanges(numQueries, minTs, maxTs, queryLenMs, new Random(44));

    // Warmup pass (JIT), same as CAPS's IndexingSink where the verification
    // pass runs before the timed loops. The answers computed here are the
    // ones written to query_answers_genealog.csv for verify.py.
    long[] howMuchAns = new long[numQueries];
    long[] versionAns1 = new long[numQueries];
    long[] versionAns2 = new long[numQueries];
    for (int i = 0; i < numQueries; i++) {
      howMuchAns[i] = howMuch(howMuchQ[i][0], howMuchQ[i][1]);
      versionAns1[i] = howMuch(versionW1[i][0], versionW1[i][1]);
      versionAns2[i] = howMuch(versionW2[i][0], versionW2[i][1]);
    }

    // Timed on windows the warm-up pass has not touched (seeds 142-144, as in caps.IndexingSink).
    long[][] timedQ = randomRanges(numQueries, minTs, maxTs, queryLenMs, new Random(142));
    long[][] timedW1 = randomRanges(numQueries, minTs, maxTs, queryLenMs, new Random(143));
    long[][] timedW2 = randomRanges(numQueries, minTs, maxTs, queryLenMs, new Random(144));
    long checksum = 0;

    long howMuchStart = System.nanoTime();
    for (int i = 0; i < numQueries; i++) {
      checksum += howMuch(timedQ[i][0], timedQ[i][1]);
    }
    long howMuchNanos = System.nanoTime() - howMuchStart;

    long versionStart = System.nanoTime();
    for (int i = 0; i < numQueries; i++) {
      checksum += howMuch(timedW2[i][0], timedW2[i][1])
          - howMuch(timedW1[i][0], timedW1[i][1]);
    }
    long versionNanos = System.nanoTime() - versionStart;

    if (checksum == Long.MIN_VALUE) {
      throw new IllegalStateException("unreachable");  // keep the timed loops live
    }

    String out = settings.indexBenchDir() + "/query_latency_genealog.csv";
    try (PrintWriter pw = new PrintWriter(new FileWriter(out))) {
      pw.println("method,query,records,queries,avg_us,total_ms");
      pw.printf(Locale.ROOT, "genealog,howmuch,%d,%d,%.3f,%.3f%n", indexSize(), numQueries,
          howMuchNanos / 1000.0 / numQueries, howMuchNanos / 1e6);
      pw.printf(Locale.ROOT, "genealog,versioning,%d,%d,%.3f,%.3f%n", indexSize(), numQueries,
          versionNanos / 1000.0 / numQueries, versionNanos / 1e6);
    }
    // Per-query answers, for verification against the CAPS index's
    // answers (verify.py).
    String answers = settings.indexBenchDir() + "/query_answers_genealog.csv";
    try (PrintWriter pw = new PrintWriter(new FileWriter(answers))) {
      pw.println("query,i,from,to,total");
      for (int i = 0; i < numQueries; i++) {
        pw.println("howmuch," + i + "," + howMuchQ[i][0] + "," + howMuchQ[i][1]
            + "," + howMuchAns[i]);
        pw.println("versioning_w1," + i + "," + versionW1[i][0] + "," + versionW1[i][1]
            + "," + versionAns1[i]);
        pw.println("versioning_w2," + i + "," + versionW2[i][0] + "," + versionW2[i][1]
            + "," + versionAns2[i]);
      }
    }
    System.out.println("GenealogIndexingSink: " + indexSize() + " outputs stored, "
        + numQueries + " how-much + " + numQueries
        + " versioning queries benchmarked");
  }

  /**
   * Latency-vs-window-length sweep (--indexQueryLensMs), the counterpart of
   * CAPS's QueryLengthSink: for each length, numQueries how-much queries
   * with random start (seed 42, identical windows to the CAPS run) answered
   * by the graph BFS, timed after a warmup pass. Writes
   * query_length_latency_genealog.csv and query_length_answers_genealog.csv
   * (verified against the CAPS answers by verify.py).
   */
  private void benchLengthSweep() throws Exception {
    final int numQueries = settings.indexNumQueries();
    final long[] lensMs = settings.indexQueryLensMs();
    long minTs = minTs();
    long maxTs = maxTs();

    String latencyFile = settings.indexBenchDir() + "/query_length_latency_genealog.csv";
    String answersFile = settings.indexBenchDir() + "/query_length_answers_genealog.csv";
    try (PrintWriter latency = new PrintWriter(new FileWriter(latencyFile));
         PrintWriter answers = new PrintWriter(new FileWriter(answersFile))) {
      latency.println("method,len_ms,records,queries,avg_us");
      answers.println("len_ms,i,from,to,total");

      for (long lenMs : lensMs) {
        long[][] queries = randomRanges(numQueries, minTs, maxTs, lenMs, new Random(42));

        // Warmup pass; its answers go to the verification CSV.
        for (int i = 0; i < numQueries; i++) {
          answers.println(lenMs + "," + i + "," + queries[i][0] + "," + queries[i][1]
              + "," + howMuch(queries[i][0], queries[i][1]));
        }

        long checksum = 0;
        long start = System.nanoTime();
        for (long[] q : queries) {
          checksum += howMuch(q[0], q[1]);
        }
        long nanos = System.nanoTime() - start;
        if (checksum == Long.MIN_VALUE) {
          throw new IllegalStateException("unreachable");  // keep the loop live
        }

        latency.printf(Locale.ROOT, "genealog,%d,%d,%d,%.3f%n", lenMs, indexSize(),
            numQueries, nanos / 1000.0 / numQueries);
        System.out.println("GenealogIndexingSink length sweep: len " + lenMs + " ms done ("
            + nanos / 1_000_000 + " ms total)");
      }
    }
    System.out.println("GenealogIndexingSink: " + indexSize() + " outputs stored, "
        + numQueries + " how-much queries benchmarked for " + lensMs.length
        + " window lengths");
  }

  /**
   * Total source-tuple contributions over the outputs in [tsFrom, tsTo]: BFS
   * each output's provenance graph, counting source hits WITH multiplicity
   * (the how-much semantics: a source contributing through both join paths,
   * or twice into a window, counts each time) - the same quantity the
   * CAPS index computes, so the answers are directly comparable.
   */
  private long howMuch(long tsFrom, long tsTo) {
    long total = 0;
    for (Shard s : shards) {
      int to = s.index.upperBound(tsTo);
      for (int i = s.index.lowerBound(tsFrom); i < to; i++) {
        total += countContributions(s, s.index.get(i));
      }
    }
    return total;
  }

  /** BFS over one output's provenance graph, counting source-tuple visits. */
  private long countContributions(Shard shard, GenealogTuple start) {
    long count = 0;
    ArrayDeque<GenealogTuple> queue = new ArrayDeque<>();
    queue.addLast(start);
    while (!queue.isEmpty()) {
      GenealogTuple t = queue.removeFirst();
      switch (t.getTupleType()) {
        case SOURCE:
          count++;
          break;
        case MAP:
          queue.addLast(t.getU1());
          break;
        case JOIN:
          queue.addLast(t.getU1());
          queue.addLast(t.getU2());
          break;
        case AGGREGATE:
          Iterator<GenealogTuple> it = shard.aggregateStrategy.provenanceIterator(t);
          while (it.hasNext()) {
            queue.addLast(it.next());
          }
          break;
        case REMOTE:
          // Tuple crossed a serialization boundary: GeneaLog's serializer
          // already collapsed its subgraph into the set of source tuples.
          count += t.getProvenance().size();
          break;
        case META_SOURCE:
          count++;
          break;
        default:
          throw new IllegalStateException("Invalid TupleType: " + t.getTupleType());
      }
    }
    return count;
  }

  /** Fixed-length windows, identical generation to CAPS's IndexingSink
   * (same seeds, same length) so the answers are directly comparable. */
  private static long[][] randomRanges(int n, long minTs, long maxTs, long lenMs, Random rnd) {
    long len = Math.min(lenMs, maxTs - minTs);
    long[][] ranges = new long[n][2];
    for (int i = 0; i < n; i++) {
      long start = minTs + (long) (rnd.nextDouble() * (maxTs - minTs - len));
      ranges[i][0] = start;
      ranges[i][1] = start + len;
    }
    return ranges;
  }
}
