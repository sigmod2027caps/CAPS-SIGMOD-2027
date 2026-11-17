package caps;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import temporalindex.ArrayStore;
import tieredindex.Answer;
import tieredindex.ShardedIndex;
import tieredindex.TwoTierIndex;

/**
 * Online Section-5 sink: builds the two-tier index live (see TieredIndexSink), benchmarks at close.
 * Replaces the slack-based reorder buffer of the released artifact: Tier 1 holds the outputs the
 * watermark has not certified, Tier 2 is the append-only prefix-sum CapsIndex.
 * Writes query_latency.csv, insert_latency.csv, index_stats.csv, query_answers.csv under benchDir,
 * plus the two-tier statistics of TieredIndexSink.
 */
public class IndexingSink<T> extends TieredIndexSink<T, TieredIndexSink.Shard> {

  /** Extracts the event timestamp of a tuple. */
  public interface TsFn<T> extends Serializable {
    long ts(T tuple);
  }

  /** Extracts the how-much meta-data of a tuple. */
  public interface HmFn<T> extends Serializable {
    HowMuch hm(T tuple);
  }

  private static final long serialVersionUID = 2L;

  private final int numQueries;
  private final long queryLenMs;
  /** CAPS_QUERY_RANGES_FROM: query_answers.csv of a reference run whose query windows this run answers (E3). */
  private final String rangesFrom;
  /** CAPS_NO_VERSIONING=1: only the how-much queries run (no versioning queries, no versioning rows in the outputs). */
  private final boolean versioning;

  /**
   * tsFn / hmFn read the derived timestamp and the HowMuch of an output; numQueries how-much (and versioning)
   * queries of window queryLenMs are answered at close on the two-tier index and on the scan baseline.
   */
  public IndexingSink(TsFn<T> tsFn, HmFn<T> hmFn, int channels,
                      String benchDir, int numQueries, long queryLenMs) {
    super(tsFn, hmFn, channels, benchDir, "index", queryLenMs);
    this.numQueries = numQueries;
    this.queryLenMs = queryLenMs;
    String r = System.getenv("CAPS_QUERY_RANGES_FROM");
    this.rangesFrom = r == null ? "" : r.trim();
    this.versioning = !"1".equals(System.getenv("CAPS_NO_VERSIONING"));
  }

  @Override
  protected boolean withArray() {
    return true;
  }

  @Override
  protected Shard newShard(int subtask, TwoTierIndex index) {
    return new Shard(subtask, index);
  }

  /**
   * Query-latency experiment on the live, online-built structures: numQueries temporal how-much
   * queries (paper 5.1) and, unless CAPS_NO_VERSIONING=1, numQueries versioning queries (paper
   * Def. versioning-abs: deltaHP between two random windows), each answered by the two-tier index (merged over the shards) and by the scan
   * baseline, timed separately. All queries run after the end of the input, so every answer is final.
   * A versioning query on the scan baseline is two scans plus a channel-wise subtraction; on the index
   * it is two queries plus the subtraction. Every query window has the same fixed length (queryLenMs,
   * ten window slides of the dataflow) and a uniformly random start.
   */
  @Override
  protected void finish(List<Shard> shards) throws Exception {
    ShardedIndex index = assemble(shards);
    ArrayStore[] array = arrays(shards);
    long minTs = minTs(shards);
    long maxTs = maxTs(shards);
    // Seeds 42/43/44 give three independent query sets in one run.
    long[][] howMuchQ = randomRanges(numQueries, minTs, maxTs, queryLenMs, new Random(42));
    long[][] versionW1 = randomRanges(numQueries, minTs, maxTs, queryLenMs, new Random(43));
    long[][] versionW2 = randomRanges(numQueries, minTs, maxTs, queryLenMs, new Random(44));
    if (!rangesFrom.isEmpty()) // the query windows of the ordered reference run: answers are then comparable
      readRanges(rangesFrom, howMuchQ, versionW1, versionW2, versioning);

    // Correctness cross-checks plus JIT warmup: the scan and the two-tier index must agree on every
    // query, the index must call the answer defined exactly when a record is in range, and every
    // answer must be final. Answers are kept and written to query_answers.csv so that the
    // verification can compare them with GeneaLog's and across parallelism.
    long[][] howMuchAns = new long[numQueries][];
    long[][] versionAns1 = new long[numQueries][];
    long[][] versionAns2 = new long[numQueries][];
    boolean[] howMuchDef = new boolean[numQueries];
    boolean[] versionDef1 = new boolean[numQueries];
    boolean[] versionDef2 = new boolean[numQueries];
    int hmFinal = 0, hmDefined = 0, verFinal = 0, verDefined = 0;
    for (int i = 0; i < numQueries; i++) {
      Answer a = index.query(howMuchQ[i][0], howMuchQ[i][1]);
      long[] expected = scan(array, howMuchQ[i][0], howMuchQ[i][1], channels);
      for (int c = 0; c < channels; c++)
        if (a.counts[c] != expected[c])
          throw new IllegalStateException("array/index mismatch on query " + i
              + " channel " + c + ": " + expected[c] + " vs " + a.counts[c]);
      boolean exists = scanCount(array, howMuchQ[i][0], howMuchQ[i][1]) > 0;
      if (a.defined != exists)
        throw new IllegalStateException("definedness mismatch on query " + i);
      if (!a.isFinal)
        throw new IllegalStateException("query " + i + " is not final after the end of the input");
      howMuchAns[i] = expected;
      howMuchDef[i] = a.defined;
      hmFinal += a.isFinal ? 1 : 0;
      hmDefined += a.defined ? 1 : 0;

      if (!versioning)
        continue;
      long[] a1 = scan(array, versionW1[i][0], versionW1[i][1], channels);
      long[] a2 = scan(array, versionW2[i][0], versionW2[i][1], channels);
      boolean d1 = scanCount(array, versionW1[i][0], versionW1[i][1]) > 0;
      boolean d2 = scanCount(array, versionW2[i][0], versionW2[i][1]) > 0;
      Answer.Version v = index.versioning(
          versionW1[i][0], versionW1[i][1], versionW2[i][0], versionW2[i][1]);
      if (v.defined != (d1 && d2))
        throw new IllegalStateException("versioning definedness mismatch on query " + i);
      if (!v.isFinal)
        throw new IllegalStateException("versioning query " + i + " is not final");
      if (v.defined)
        for (int c = 0; c < channels; c++)
          if (v.delta[c] != a2[c] - a1[c])
            throw new IllegalStateException("array/index mismatch on versioning query "
                + i + " channel " + c + ": " + (a2[c] - a1[c]) + " vs " + v.delta[c]);
      versionAns1[i] = a1;
      versionAns2[i] = a2;
      versionDef1[i] = d1;
      versionDef2[i] = d2;
      verFinal += v.isFinal ? 1 : 0;
      verDefined += v.defined ? 1 : 0;
    }
    queryStatus("howmuch", numQueries, hmFinal, hmDefined);
    if (versioning)
      queryStatus("versioning", numQueries, verFinal, verDefined);

    // The timed loops use windows the verification pass above has not touched (seeds 142-144): the pass warms
    // the JIT but not the data cache for the windows that are timed, which would favour a method whose working
    // set of 2 x numQueries positions fits in the cache. Same windows for every method.
    long[][] timedQ = howMuchQ;
    long[][] timedW1 = versionW1;
    long[][] timedW2 = versionW2;
    if (rangesFrom.isEmpty()) {
      timedQ = randomRanges(numQueries, minTs, maxTs, queryLenMs, new Random(142));
      timedW1 = randomRanges(numQueries, minTs, maxTs, queryLenMs, new Random(143));
      timedW2 = randomRanges(numQueries, minTs, maxTs, queryLenMs, new Random(144));
    }

    // Touch answers so the JIT cannot erase the timed loops.
    long checksum = 0;

    long arrayStart = System.nanoTime();
    for (long[] q : timedQ)
      checksum += scan(array, q[0], q[1], channels)[0];
    long arrayHowMuchNanos = System.nanoTime() - arrayStart;

    long indexStart = System.nanoTime();
    for (long[] q : timedQ)
      checksum += index.query(q[0], q[1]).counts[0];
    long indexHowMuchNanos = System.nanoTime() - indexStart;

    long arrayVersionNanos = 0, indexVersionNanos = 0;
    if (versioning) {
      long arrayVersionStart = System.nanoTime();
      for (int i = 0; i < numQueries; i++)
        checksum += arrayDelta(array, timedW1[i], timedW2[i])[0];
      arrayVersionNanos = System.nanoTime() - arrayVersionStart;

      long indexVersionStart = System.nanoTime();
      for (int i = 0; i < numQueries; i++) {
        Answer a = index.query(timedW1[i][0], timedW1[i][1]);
        Answer b = index.query(timedW2[i][0], timedW2[i][1]);
        checksum += b.counts[0] - a.counts[0];
      }
      indexVersionNanos = System.nanoTime() - indexVersionStart;
    }

    long records = 0, bytes = 0, tier1Nanos = 0, splitNanos = 0, appendNanos = 0, scanAppendNanos = 0;
    for (Shard s : shards) {
      records += s.index.tier2Size();
      bytes += s.index.tier2().memoryBytes();
      tier1Nanos += s.index.insertNanos;
      splitNanos += s.index.splitNanos;
      appendNanos += s.index.migrationNanos;
      scanAppendNanos += s.index.listenerNanos;
    }
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/query_latency.csv"))) {
      pw.println("method,query,records,queries,avg_us,total_ms");
      writeRow(pw, "array", "howmuch", records, arrayHowMuchNanos);
      writeRow(pw, "prefix", "howmuch", records, indexHowMuchNanos);
      if (versioning) {
        writeRow(pw, "array", "versioning", records, arrayVersionNanos);
        writeRow(pw, "prefix", "versioning", records, indexVersionNanos);
      }
    }
    // Insert cost per record, Tier-1 insert and the split included for both: the scan baseline faces
    // the same disorder and the same Tier 1; the methods differ in the append they feed.
    long prefixInsert = tier1Nanos + splitNanos + appendNanos;
    long arrayInsert = tier1Nanos + splitNanos + scanAppendNanos;
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/insert_latency.csv"))) {
      pw.println("method,records,avg_ns,total_ms");
      pw.printf(Locale.ROOT, "array,%d,%.1f,%.3f%n", records,
          (double) arrayInsert / records, arrayInsert / 1e6);
      pw.printf(Locale.ROOT, "prefix,%d,%.1f,%.3f%n", records,
          (double) prefixInsert / records, prefixInsert / 1e6);
    }
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/index_stats.csv"))) {
      pw.println("records,index_bytes");
      pw.println(records + "," + bytes);
    }
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/query_answers.csv"))) {
      StringBuilder header = new StringBuilder("query,i,from,to");
      for (int c = 0; c < channels; c++)
        header.append(",c").append(c);
      header.append(",defined,final");
      pw.println(header);
      for (int i = 0; i < numQueries; i++) {
        writeAnswer(pw, "howmuch", i, howMuchQ[i], howMuchAns[i], howMuchDef[i]);
        if (versioning) {
          writeAnswer(pw, "versioning_w1", i, versionW1[i], versionAns1[i], versionDef1[i]);
          writeAnswer(pw, "versioning_w2", i, versionW2[i], versionAns2[i], versionDef2[i]);
        }
      }
    }
    System.out.println("IndexingSink: " + records + " records indexed online in " + shards.size()
        + " shard(s), " + numQueries + " how-much + " + (versioning ? numQueries : 0)
        + " versioning queries benchmarked (checksum " + checksum + ")");
  }

  /** Overwrites the query windows with those of a query_answers.csv (type,i,from,to,...). */
  private static void readRanges(String file, long[][] howMuch, long[][] w1, long[][] w2, boolean versioning) throws java.io.IOException {
    int found = 0;
    try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(file))) {
      String line = br.readLine(); // header
      while ((line = br.readLine()) != null) {
        String[] f = line.split(",");
        int i = Integer.parseInt(f[1]);
        if (!versioning && !f[0].equals("howmuch"))
          continue;
        long[][] target = f[0].equals("howmuch") ? howMuch : f[0].equals("versioning_w1") ? w1 : w2;
        target[i][0] = Long.parseLong(f[2]);
        target[i][1] = Long.parseLong(f[3]);
        found++;
      }
    }
    int expected = (versioning ? 3 : 1) * howMuch.length;
    if (found != expected)
      throw new IllegalStateException(file + " has " + found + " query windows, expected " + expected);
  }

  /** Versioning on the scan baseline: scan both windows, subtract channel-wise. */
  private long[] arrayDelta(ArrayStore[] array, long[] w1, long[] w2) {
    long[] a = scan(array, w1[0], w1[1], channels);
    long[] b = scan(array, w2[0], w2[1], channels);
    long[] delta = new long[channels];
    for (int c = 0; c < channels; c++)
      delta[c] = b[c] - a[c];
    return delta;
  }

  private void writeRow(PrintWriter pw, String method, String query, long records, long nanos) {
    pw.printf(Locale.ROOT, "%s,%s,%d,%d,%.3f,%.3f%n", method, query, records, numQueries,
        nanos / 1000.0 / numQueries, nanos / 1e6);
  }

  private static void writeAnswer(PrintWriter pw, String type, int i, long[] q, long[] answer,
                                  boolean defined) {
    StringBuilder sb = new StringBuilder();
    sb.append(type).append(',').append(i).append(',').append(q[0]).append(',').append(q[1]);
    for (long a : answer)
      sb.append(',').append(a);
    sb.append(',').append(defined ? 1 : 0).append(",1");
    pw.println(sb);
  }
}
