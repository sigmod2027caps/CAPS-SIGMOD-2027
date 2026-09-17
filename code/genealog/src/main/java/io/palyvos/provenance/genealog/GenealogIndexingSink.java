package io.palyvos.provenance.genealog;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.Random;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import temporalindex.GenealogIndex;

/**
 * GeneaLog temporal-query baseline: the counterpart of CAPS's
 * IndexingSink. Output tuples are stored (in time order, same reorder-buffer
 * scheme) in a {@link GenealogIndex} while the dataflow runs — keeping their
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
 * numQueries ({@code --indexNumQueries}) must match the CAPS run so the
 * verification covers all queries. Note one query traverses the graphs of
 * millions of outputs and takes seconds, versus microseconds for the
 * prefix-sum index.
 */
public class GenealogIndexingSink<T extends GenealogTuple> extends RichSinkFunction<T> {

  private static final long serialVersionUID = 1L;

  private final ExperimentSettings settings;
  private final long slackMs;

  private transient GenealogIndex<GenealogTuple> index;
  private transient ProvenanceAggregateStrategy aggregateStrategy;
  private transient PriorityQueue<Entry> reorder;
  private transient long maxSeenTs;

  public GenealogIndexingSink(ExperimentSettings settings, long slackMs) {
    this.settings = settings;
    this.slackMs = slackMs;
  }

  @Override
  public void open(Configuration parameters) {
    index = new GenealogIndex<>();
    aggregateStrategy = settings.aggregateStrategySupplier().get();
    reorder = new PriorityQueue<>();
    maxSeenTs = Long.MIN_VALUE;
  }

  @Override
  public void invoke(T tuple, Context context) {
    long ts = tuple.getTimestamp();
    reorder.add(new Entry(ts, tuple));
    maxSeenTs = Math.max(maxSeenTs, ts);
    while (!reorder.isEmpty() && reorder.peek().ts <= maxSeenTs - slackMs) {
      Entry e = reorder.poll();
      index.append(e.ts, e.tuple);
    }
  }

  @Override
  public void close() throws Exception {
    while (!reorder.isEmpty()) {
      Entry e = reorder.poll();
      index.append(e.ts, e.tuple);
    }
    if (index.size() == 0) {
      throw new IllegalStateException("no records reached the genealog indexing sink");
    }
    if (settings.indexQueryLensMs() != null) {
      benchLengthSweep();
    } else {
      bench();
    }
  }

  private void bench() throws Exception {
    final int numQueries = settings.indexNumQueries();
    final long queryLenMs = settings.indexQueryLenMs();
    if (queryLenMs <= 0) {
      throw new IllegalStateException("--indexQueryLenMs is required for the query bench");
    }
    long minTs = index.firstTs();
    long maxTs = maxSeenTs;
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

    long checksum = 0;

    long howMuchStart = System.nanoTime();
    for (int i = 0; i < numQueries; i++) {
      checksum += howMuch(howMuchQ[i][0], howMuchQ[i][1]);
    }
    long howMuchNanos = System.nanoTime() - howMuchStart;

    long versionStart = System.nanoTime();
    for (int i = 0; i < numQueries; i++) {
      checksum += howMuch(versionW2[i][0], versionW2[i][1])
          - howMuch(versionW1[i][0], versionW1[i][1]);
    }
    long versionNanos = System.nanoTime() - versionStart;

    if (checksum == Long.MIN_VALUE) {
      throw new IllegalStateException("unreachable");  // keep the timed loops live
    }

    String out = settings.indexBenchDir() + "/query_latency_genealog.csv";
    try (PrintWriter pw = new PrintWriter(new FileWriter(out))) {
      pw.println("method,query,records,queries,avg_us,total_ms");
      pw.printf(Locale.ROOT, "genealog,howmuch,%d,%d,%.3f,%.3f%n", index.size(), numQueries,
          howMuchNanos / 1000.0 / numQueries, howMuchNanos / 1e6);
      pw.printf(Locale.ROOT, "genealog,versioning,%d,%d,%.3f,%.3f%n", index.size(), numQueries,
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
    System.out.println("GenealogIndexingSink: " + index.size() + " outputs stored, "
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
    long minTs = index.firstTs();
    long maxTs = maxSeenTs;

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

        latency.printf(Locale.ROOT, "genealog,%d,%d,%d,%.3f%n", lenMs, index.size(),
            numQueries, nanos / 1000.0 / numQueries);
        System.out.println("GenealogIndexingSink length sweep: len " + lenMs + " ms done ("
            + nanos / 1_000_000 + " ms total)");
      }
    }
    System.out.println("GenealogIndexingSink: " + index.size() + " outputs stored, "
        + numQueries + " how-much queries benchmarked for " + lensMs.length
        + " window lengths");
  }

  /**
   * Total source-tuple contributions over the outputs in [tsFrom, tsTo]: BFS
   * each output's provenance graph, counting source hits WITH multiplicity
   * (the how-much semantics: a source contributing through both join paths,
   * or twice into a window, counts each time) — the same quantity the
   * CAPS index computes, so the answers are directly comparable.
   */
  private long howMuch(long tsFrom, long tsTo) {
    long total = 0;
    int to = index.upperBound(tsTo);
    for (int i = index.lowerBound(tsFrom); i < to; i++) {
      total += countContributions(index.get(i));
    }
    return total;
  }

  /** BFS over one output's provenance graph, counting source-tuple visits. */
  private long countContributions(GenealogTuple start) {
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
          Iterator<GenealogTuple> it = aggregateStrategy.provenanceIterator(t);
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

  private static final class Entry implements Comparable<Entry> {
    final long ts;
    final GenealogTuple tuple;

    Entry(long ts, GenealogTuple tuple) {
      this.ts = ts;
      this.tuple = tuple;
    }

    @Override
    public int compareTo(Entry other) {
      return Long.compare(ts, other.ts);
    }
  }
}
