package caps;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import temporalindex.ArrayStore;
import tieredindex.Answer;
import tieredindex.ShardedIndex;
import tieredindex.TwoTierIndex;

/**
 * Like IndexingSink but sweeps several query window lengths; seed 42 per length matches GeneaLog.
 * Writes query_length_latency.csv and query_length_answers.csv (plus the two-tier statistics).
 * Paper Section 7.4, Figure 9b (latency against the query window length) on the two-tier index; repeated REPS times.
 */
public class QueryLengthSink<T> extends TieredIndexSink<T, TieredIndexSink.Shard> {

  private static final long serialVersionUID = 2L;

  private final int numQueries;
  private final long[] queryLensMs;

  /** numQueries queries (seed 42, as in the GeneaLog sink) for every window length in queryLensMs, answered at close. */
  public QueryLengthSink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn, int channels,
                         String benchDir, int numQueries, long[] queryLensMs) {
    super(tsFn, hmFn, channels, benchDir, "querylen", queryLensMs[0]);
    this.numQueries = numQueries;
    this.queryLensMs = queryLensMs;
  }

  @Override
  protected boolean withArray() {
    return true;
  }

  @Override
  protected Shard newShard(int subtask, TwoTierIndex index) {
    return new Shard(subtask, index);
  }

  @Override
  protected void finish(List<Shard> shards) throws Exception {
    ShardedIndex index = assemble(shards);
    ArrayStore[] array = arrays(shards);
    long minTs = minTs(shards);
    long maxTs = maxTs(shards);
    long records = 0;
    for (Shard s : shards)
      records += s.index.tier2Size();

    int finals = 0, defined = 0, total = 0;
    try (PrintWriter latency = new PrintWriter(
            new FileWriter(benchDir + "/query_length_latency.csv"));
         PrintWriter answers = new PrintWriter(
            new FileWriter(benchDir + "/query_length_answers.csv"))) {
      latency.println("method,len_ms,records,queries,avg_us");
      StringBuilder header = new StringBuilder("len_ms,i,from,to");
      for (int c = 0; c < channels; c++)
        header.append(",c").append(c);
      header.append(",defined,final");
      answers.println(header);

      for (long lenMs : queryLensMs) {
        // Fresh Random(42) per length: do not change without rerunning the competitor jobs.
        long[][] queries = randomRanges(numQueries, minTs, maxTs, lenMs, new Random(42));

        // Warmup + cross-check + answers for the verification.
        for (int i = 0; i < numQueries; i++) {
          long[] expected = scan(array, queries[i][0], queries[i][1], channels);
          Answer a = index.query(queries[i][0], queries[i][1]);
          if (a.defined != (scanCount(array, queries[i][0], queries[i][1]) > 0))
            throw new IllegalStateException("definedness mismatch on len " + lenMs + " query " + i);
          if (!a.isFinal)
            throw new IllegalStateException("query " + i + " is not final after the end of the input");
          StringBuilder row = new StringBuilder();
          row.append(lenMs).append(',').append(i).append(',')
              .append(queries[i][0]).append(',').append(queries[i][1]);
          for (int c = 0; c < channels; c++) {
            if (a.counts[c] != expected[c])
              throw new IllegalStateException("array/index mismatch on len " + lenMs
                  + " query " + i + " channel " + c + ": " + expected[c] + " vs " + a.counts[c]);
            row.append(',').append(expected[c]);
          }
          row.append(',').append(a.defined ? 1 : 0).append(',').append(a.isFinal ? 1 : 0);
          answers.println(row);
          total++;
          finals += a.isFinal ? 1 : 0;
          defined += a.defined ? 1 : 0;
        }

        long checksum = 0;

        long arrayStart = System.nanoTime();
        for (long[] q : queries)
          checksum += scan(array, q[0], q[1], channels)[0];
        long arrayNanos = System.nanoTime() - arrayStart;

        long indexStart = System.nanoTime();
        for (long[] q : queries)
          checksum += index.query(q[0], q[1]).counts[0];
        long indexNanos = System.nanoTime() - indexStart;

        if (checksum == Long.MIN_VALUE)
          throw new IllegalStateException("unreachable"); // keep the loops live

        latency.printf(Locale.ROOT, "array,%d,%d,%d,%.3f%n", lenMs, records, numQueries,
            arrayNanos / 1000.0 / numQueries);
        latency.printf(Locale.ROOT, "prefix,%d,%d,%d,%.3f%n", lenMs, records, numQueries,
            indexNanos / 1000.0 / numQueries);
      }
    }
    queryStatus("howmuch_lengths", total, finals, defined);
    System.out.println("QueryLengthSink: " + records + " records indexed online in "
        + shards.size() + " shard(s), " + numQueries + " how-much queries benchmarked for "
        + queryLensMs.length + " window lengths");
  }
}
