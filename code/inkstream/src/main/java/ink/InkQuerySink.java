package ink;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import temporalindex.LogicalSampler;
import temporalindex.QueryFixture;
import temporalindex.QueryIdManifest;
import temporalindex.SampleKeyFn;

/**
 * Bench sink of the polynomial baseline, a near-copy of
 * {@code caps.ProvenanceQuerySink}. While the stream runs it flips the
 * same biased coin (seed 42) and, on heads, answers and times the
 * how-much query — here {@link InkMeta#howMuch} instead of a counter
 * read.
 *
 * <p>Verification matches the CAPS twin exactly: every output is
 * accumulated (count + sumOfAnswers + sumOfSquares) by timestamp, so
 * the two sinks do identical bookkeeping and the runtime difference
 * between them is the annotation, not the harness. The fold is
 * performed only for coin-sampled outputs (the timed query) and, in
 * verification, for every output. We deliberately do NOT retain the
 * annotations themselves — holding 13.5 M polynomials of ~159
 * variables would exhaust the heap for reasons that are the
 * harness's fault rather than the scheme's.
 *
 * <p>Timestamp-grouped verification: the three methods emit the same
 * output multiset but in different order, and the order of outputs
 * sharing a timestamp is not even stable across two runs of the SAME
 * job, so index-based or reservoir sampling cannot be compared across
 * methods. Accumulating every output by timestamp makes every method
 * cover exactly the same outputs regardless of order. The map is
 * bounded by the number of distinct output timestamps, itself at most
 * the number of distinct source timestamps (measured 47K-553K
 * entries, 4-46 MB), not by output count.
 */
// Timing vs verification are seperate jobs — folding every output would land in the runtime column.
// Sampling uses FNV-1a of the logical key, not the LCG below (kept as historical record).
// avg_ns is nanoseconds per query; total_ms is the same wall time in milliseconds.
public class InkQuerySink<T> extends RichSinkFunction<T> {

  private static final long serialVersionUID = 1L;

  /**
   * Knuth 64-bit LCG (Numerical Recipes / MMIX). Same algorithm and seed
   * in ProvenanceQuerySink, InkQuerySink, GenealogTraversalSink, and the
   * noprov FileSink, so sampling decisions stay deterministic and identical
   * and every method pays the same per-output coin cost. Replaces
   * {@code java.util.Random#nextDouble}, which does two CAS loops on a
   * shared AtomicLong (~15-20 ns); this is a plain non-volatile
   * multiply-add on a sink-local field (~2 ns, no CAS).
   */
  private static final long COIN_MULT = 6364136223846793005L;
  private static final long COIN_INC = 1442695040888963407L;
  private static final long COIN_SEED = 42L;

  /**
   * Incremented by the calibration variant inside the timed region. Volatile
   * so that neither the JIT nor the CPU can drop the write, which would
   * leave the bracket enclosing nothing at all.
   */
  private static volatile int dummy = 0;

  /**
   * Timestamp extractor, declared here so {@code inkstream} does not
   * depend on {@code caps}.
   */
  public interface TsFn<T> extends Serializable {
    long ts(T tuple);
  }

  /**
   * Annotation extractor, the analogue of {@code caps.IndexingSink.HmFn}.
   */
  public interface MetaFn<T> extends Serializable {
    InkMeta meta(T tuple);
  }

  private final TsFn<T> tsFn;
  private final MetaFn<T> metaFn;
  private static final int SHARED_REQUIRED = 1000;

  private final SampleKeyFn<T> keyFn;
  private final int channels;
  private final String benchDir;
  private final double sampleProb;
  private final boolean calibrate;
  private final boolean verifyMode;
  private final boolean vectorQuery;
  private final boolean sharedIds;
  private final String manifestPath;

  private transient HashMap<Long, LogicalSampler.VerifyGroup> verify;
  private transient LogicalSampler sampler;
  private transient LogicalSampler.SelectedSet selectedKeys;
  private transient QueryIdManifest.Matcher matcher;
  private transient HashMap<Long, Long> answers;
  private transient HashMap<Long, InkMeta> captured;
  private transient long coinThreshold;
  private transient int sampled;
  private transient long queryNanos;
  private transient long checksum;
  private transient long predicateChecksum;
  private transient long outputs;
  private transient long totalMonomials;
  private transient long totalVars;
  private transient long maxVars;
  private transient long totalBytes;

  public InkQuerySink(TsFn<T> tsFn, MetaFn<T> metaFn, int channels,
                      String benchDir, double sampleProb) {
    this(tsFn, metaFn, timestampKey(tsFn), channels, benchDir, sampleProb, false, false, false);
  }

  /**
   * @param calibrate run the calibration variant instead of the measurement,
   *     see the discussion of the timed region in {@link #invoke}
   */
  public InkQuerySink(TsFn<T> tsFn, MetaFn<T> metaFn, int channels,
                      String benchDir, double sampleProb, boolean calibrate) {
    this(tsFn, metaFn, timestampKey(tsFn), channels, benchDir, sampleProb,
        calibrate, false, false);
  }

  public InkQuerySink(TsFn<T> tsFn, MetaFn<T> metaFn, int channels,
                      String benchDir, double sampleProb, boolean calibrate,
                      boolean verifyMode) {
    this(tsFn, metaFn, timestampKey(tsFn), channels, benchDir, sampleProb,
        calibrate, verifyMode, verifyMode);
  }

  public InkQuerySink(TsFn<T> tsFn, MetaFn<T> metaFn, SampleKeyFn<T> keyFn,
                      int channels, String benchDir, double sampleProb) {
    this(tsFn, metaFn, keyFn, channels, benchDir, sampleProb, false, false, false);
  }

  public InkQuerySink(TsFn<T> tsFn, MetaFn<T> metaFn, SampleKeyFn<T> keyFn,
                      int channels, String benchDir, double sampleProb,
                      boolean calibrate) {
    this(tsFn, metaFn, keyFn, channels, benchDir, sampleProb, calibrate, false, false);
  }

  public InkQuerySink(TsFn<T> tsFn, MetaFn<T> metaFn, SampleKeyFn<T> keyFn,
                      int channels, String benchDir, double sampleProb,
                      boolean calibrate, boolean verifyMode, boolean vectorQuery) {
    this(tsFn, metaFn, keyFn, channels, benchDir, sampleProb, calibrate, verifyMode,
        vectorQuery, false, null);
  }

  /** Capture the first occurrence of each manifest ID into a query fixture. */
  public InkQuerySink(TsFn<T> tsFn, MetaFn<T> metaFn, SampleKeyFn<T> keyFn,
                      int channels, String benchDir, String manifestPath) {
    this(tsFn, metaFn, keyFn, channels, benchDir, 0.0, false, false, false,
        true, manifestPath);
  }

  private InkQuerySink(TsFn<T> tsFn, MetaFn<T> metaFn, SampleKeyFn<T> keyFn,
                       int channels, String benchDir, double sampleProb,
                       boolean calibrate, boolean verifyMode, boolean vectorQuery,
                       boolean sharedIds, String manifestPath) {
    this.tsFn = tsFn;
    this.metaFn = metaFn;
    this.keyFn = keyFn;
    this.channels = channels;
    this.benchDir = benchDir;
    this.sampleProb = sampleProb;
    this.calibrate = calibrate;
    this.verifyMode = verifyMode;
    this.vectorQuery = vectorQuery;
    this.sharedIds = sharedIds;
    this.manifestPath = manifestPath;
  }

  boolean sharedIds() {
    return sharedIds;
  }

  boolean calibrates() {
    return calibrate;
  }

  boolean verifies() {
    return verifyMode;
  }

  boolean vectorQuery() {
    return vectorQuery;
  }

  String manifestPath() {
    return manifestPath;
  }

  static <T> SampleKeyFn<T> timestampKey(final TsFn<T> tsFn) {
    return new SampleKeyFn<T>() {
      @Override
      public void writeKey(T value, java.io.DataOutput out) throws java.io.IOException {
        out.writeLong(tsFn.ts(value));
      }
    };
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    sampler = new LogicalSampler();
    outputs = 0;
    if (sharedIds) {
      // The three methods must answer the same 1,000 outputs, otherwise the ratio says nothing.
      matcher = QueryIdManifest.Matcher.load(manifestPath);
      answers = new HashMap<Long, Long>();
      captured = new HashMap<Long, InkMeta>();
      checksum = 0;
      return;
    }
    coinThreshold = LogicalSampler.threshold53(sampleProb);
    if (verifyMode) {
      verify = new HashMap<Long, LogicalSampler.VerifyGroup>();
    } else {
      selectedKeys = new LogicalSampler.SelectedSet();
    }
    sampled = 0;
    queryNanos = 0;
    checksum = 0;
    predicateChecksum = 0;
    totalMonomials = 0;
    totalVars = 0;
    maxVars = 0;
    totalBytes = 0;
  }

  @Override
  public void invoke(T value, Context context) {
    InkMeta meta = metaFn.meta(value);

    outputs++;
    long keyHash = sampler.hashKey(keyFn, value);
    if (sharedIds) {
      if (!matcher.selectFirst(keyHash)) {
        return;
      }
      long answer = meta.howMuchTotal(channels);
      checksum += answer;
      Long boxed = Long.valueOf(keyHash);
      answers.put(boxed, Long.valueOf(answer));
      captured.put(boxed, meta.copy());
      return;
    }
    predicateChecksum += keyHash;

    if (verifyMode) {
      totalMonomials += meta.numMonomials();
      int tv = meta.totalVars();
      totalVars += tv;
      if (tv > maxVars) {
        maxVars = tv;
      }
      totalBytes += meta.metadataBytes();
      accumulate(tsFn.ts(value), keyHash, meta);
      return;
    }

    // The coin fires on roughly one output in a few hundred to a few thousand,
    // so this branch is cold every time it is taken
    //
    // For each selected output, we run both calibration and measurement variants.
    // In Table 2, we show the difference between these two, which does not inlude the overhead of the timer.
    boolean heads = LogicalSampler.selected(keyHash, coinThreshold);
    if (heads) {
      long answer;
      if (calibrate) {
        long t0 = System.nanoTime();
        dummy++;
        queryNanos += System.nanoTime() - t0;
        // Answered anyway, outside the bracket, so that the two variants do
        // identical work per output and differ only in what is timed.
        answer = timedAnswer(meta);
      } else {
        long t0 = System.nanoTime();
        answer = timedAnswer(meta);
        queryNanos += System.nanoTime() - t0;
      }
      sampled++;
      checksum += answer;
      selectedKeys.add(keyHash);
    }
  }

  @Override
  public void close() throws Exception {
    final long n = outputs;
    if (n == 0) {
      throw new IllegalStateException("no records reached the ink query sink");
    }
    if (sharedIds) {
      writeSharedResults(n);
      return;
    }
    if (verifyMode) {
      ArrayList<Long> keys = new ArrayList<Long>(verify.keySet());
      Collections.sort(keys);
      try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/provenance_answers_ink.csv"))) {
        pw.println("ts,group_count,vector_hi,vector_lo,payload_hi,payload_lo,total_hi,total_lo,semantics");
        for (int i = 0; i < keys.size(); i++) {
          long ts = keys.get(i).longValue();
          LogicalSampler.VerifyGroup g = verify.get(keys.get(i));
          long[] v = g.vectorDigest();
          long[] p = g.payloadDigest();
          long[] t = g.totalDigest();
          pw.println(ts + "," + g.count + "," + v[0] + "," + v[1] + ","
              + p[0] + "," + p[1] + "," + t[0] + "," + t[1] + ","
              + LogicalSampler.HMP_VECTOR);
        }
      }
      try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/outputs_ink.txt"))) {
        pw.println(n);
      }
      try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/metadata_ink.csv"))) {
        pw.println("outputs,total_monomials,total_vars,max_vars,bytes");
        pw.println(n + "," + totalMonomials + "," + totalVars + "," + maxVars + "," + totalBytes);
      }
      System.out.println("InkQuerySink: " + n + " outputs, verification over "
          + keys.size() + " timestamps, monomials " + totalMonomials
          + ", vars " + totalVars + ", bytes " + totalBytes);
      return;
    }
    if (sampled == 0) {
      throw new IllegalStateException("coin never landed heads: sampleProb too small");
    }
    final int samples = sampled;
    final long nanos = queryNanos;
    final String semantics = vectorQuery
        ? LogicalSampler.HMP_VECTOR : LogicalSampler.TOTAL_CONTRIBUTIONS;

    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/provenance_query_ink.csv"))) {
      pw.println("method,outputs,samples,avg_ns,total_ms,checksum,query_semantics,predicate_checksum,unique_selected");
      pw.printf(Locale.ROOT, "%s,%d,%d,%.1f,%.3f,%d,%s,%d,%d%n",
          calibrate ? "ink_timer" : "ink",
          n, samples, (double) nanos / samples, nanos / 1e6, checksum,
          semantics, predicateChecksum, selectedKeys.unique());
    }
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/selected_samples_ink.csv"))) {
      pw.println("key_hash,count");
      ArrayList<long[]> rows = selectedKeys.rows();
      for (int i = 0; i < rows.size(); i++) {
        long[] row = rows.get(i);
        pw.println(row[0] + "," + row[1]);
      }
    }
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/outputs_ink.txt"))) {
      pw.println(n);
    }
    System.out.println("InkQuerySink: " + n + " outputs, " + samples
        + " provenance queries answered online (coin p=" + sampleProb
        + ", checksum " + checksum + ", semantics " + semantics
        + ", predicate " + predicateChecksum + ")");
  }

  private void writeSharedResults(long n) throws java.io.IOException {
    if (matcher.total() != SHARED_REQUIRED) {
      throw new IllegalStateException(
          "manifest total " + matcher.total() + " != " + SHARED_REQUIRED);
    }
    if (matcher.remaining() != 0) {
      throw new IllegalStateException(
          "manifest unmatched: remaining=" + matcher.remaining());
    }
    if (matcher.matched() != matcher.total()) {
      throw new IllegalStateException(
          "manifest matched " + matcher.matched() + " != total " + matcher.total());
    }
    ArrayList<Long> ids = matcher.matchedRows();
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/query_answers_green.csv"))) {
      pw.println("query_id,total_contributions");
      for (int i = 0; i < ids.size(); i++) {
        Long id = ids.get(i);
        pw.println(id.longValue() + "," + answers.get(id).longValue());
      }
    }
    long[] idArray = new long[ids.size()];
    Object[] queries = new Object[ids.size()];
    for (int i = 0; i < ids.size(); i++) {
      Long id = ids.get(i);
      idArray[i] = id.longValue();
      queries[i] = captured.get(id);
    }
    QueryFixture.of(QueryFixture.METHOD_GREEN, channels, idArray, queries)
        .write(benchDir + "/query_fixture_green.ser");
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/outputs_ink.txt"))) {
      pw.println(n);
    }
  }

  private long timedAnswer(InkMeta meta) {
    if (vectorQuery) {
      long[] vec = meta.howMuch(channels);
      return LogicalSampler.hashLongs(vec);
    }
    return meta.howMuchTotal(channels);
  }

  private void accumulate(long ts, long payloadHash, InkMeta meta) {
    Long key = Long.valueOf(ts);
    LogicalSampler.VerifyGroup g = verify.get(key);
    if (g == null) {
      g = new LogicalSampler.VerifyGroup();
      verify.put(key, g);
    }
    long[] vec = meta.howMuch(channels);
    long total = 0;
    for (int c = 0; c < vec.length; c++) {
      total += vec[c];
    }
    g.add(Long.valueOf(LogicalSampler.hashLongs(vec)), payloadHash,
        LogicalSampler.hashScalar(total));
  }

  @SuppressWarnings("unused")
  private static long unusedLcgTouch() {
    return COIN_MULT + COIN_INC + COIN_SEED;
  }
}
