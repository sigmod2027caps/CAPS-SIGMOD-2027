package caps;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import temporalindex.LogicalSampler;
import temporalindex.QueryFixture;
import temporalindex.QueryIdManifest;
import temporalindex.SampleKeyFn;
import temporalindex.SampleTimes;
import tieredindex.ShardGroup;

/**
 * Per-tuple how-much query benchmark: FNV coin on logical output key, ~1000 samples per run.
 * Timing mode keeps almost nothing per output; verification is a separate submission (see below).
 * calibrate=true times an empty bracket so Figure 9a can subtract nanoTime overhead.
 * Timestamp-grouped verification: the three methods emit the same output
 * multiset but in different order, and the order of outputs sharing a
 * timestamp is not even stable across two runs of the SAME job, so
 * index-based or reservoir sampling cannot be compared across methods.
 * Accumulating every output by timestamp makes every method cover exactly
 * the same outputs regardless of order. The map is bounded by the number
 * of distinct output timestamps, itself at most the number of distinct
 * source timestamps (measured 47K-553K entries, 4-46 MB), not by output
 * count.
 *
 * A submission is either timing or verification, never both. Timing (the
 * default, every figure and table) retains nothing per output - no entries
 * list, no per-timestamp map - only running scalars plus the coin-sampled
 * queries. Verification must be a separate job: answering every output at
 * a selected timestamp is, for GeneaLog, a graph traversal, and that cost
 * would otherwise land in the runtime column. Verification (opt-in) answers
 * every output and accumulates {count, sumOfAnswers, sumOfSquares} per
 * timestamp over ALL timestamps, no stride. Per-timestamp grouping over
 * all timestamps is the comparison basis because the three methods emit
 * the same output multiset in different order, and the order of outputs
 * sharing a timestamp is not stable even across two runs of the same job.
 *
 * Sampling is now a 64-bit FNV-1a of the logical-output sample key
 * (timestamp, grouping key, result values), not the arrival-index LCG
 * below. The LCG constants stay so the comment above remains the
 * historical record; they are not consulted. Verification writes
 * order-independent 128-bit digests of per-channel vectors and of
 * payload keys (probabilistic, never "exact"), plus a scalar-total
 * digest for GeneaLog comparison.
 *
 * Parallelism. The query of this sink is per output: the vector rides on the tuple, so every
 * subtask answers its own samples and nothing needs merging while the job runs. What the released
 * sink assumed was a single subtask in close(): "no output", "coin never landed heads", a manifest
 * that totals exactly 1000 and matches completely, one fixed file per result. Those are now checks
 * over the merged results: every subtask hands its partial state to ShardGroup and the last one to
 * close (all subtasks share the TaskManager JVM of the harness) merges the partials and writes the
 * files. A key that occurs on two subtasks is captured once (first occurrence in subtask order).
 * The per-output how-much query of Paper Section 7.3 (Figure 9a, in situ): the answer is read off the HowMuch counters that
 * ride on the tuple. The JMH counterpart over the same 1,000 outputs is query_jmh.
 */
public class ProvenanceQuerySink<T> extends RichSinkFunction<T> {

  private static final long serialVersionUID = 2L;

  private final String token = UUID.randomUUID().toString();
  private transient int subtask;
  private transient int parallelism;

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
   * Incremented by the calibration variant inside the timed region. Volatile so
   * that neither the JIT nor the CPU can drop the write, which would leave the
   * bracket enclosing nothing at all.
   */
  private static volatile int dummy = 0;

  private final IndexingSink.TsFn<T> tsFn;
  private final IndexingSink.HmFn<T> hmFn;
  // Must match QueryFixture.REQUIRED_COUNT and the manifest size.
  private static final int SHARED_REQUIRED = 1000;
  private static final long MANIFEST_SEED = 42L;

  private final SampleKeyFn<T> keyFn;
  private final int channels;
  private final String benchDir;
  private final double sampleProb;
  private final boolean calibrate;
  private final boolean verifyMode;
  private final boolean vectorQuery;
  private final boolean generateIds;
  private final boolean sharedIds;
  private final String manifestPath;
  private final int generateCount;

  private transient HashMap<Long, LogicalSampler.VerifyGroup> verify;
  private transient LogicalSampler sampler;
  private transient LogicalSampler.SelectedSet selectedKeys;
  private transient QueryIdManifest.Generator generator;
  private transient QueryIdManifest.Matcher matcher;
  private transient HashMap<Long, Long> answers;
  private transient HashMap<Long, HowMuch> captured;
  private transient long coinThreshold;
  private transient int sampled;
  private transient long queryNanos;
  private transient SampleTimes sampleTimes = new SampleTimes(); // per-sample durations, for the median
  private transient long checksum;
  private transient long predicateChecksum;
  private transient long outputs;

  public ProvenanceQuerySink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn,
                             int channels, String benchDir, double sampleProb) {
    this(tsFn, hmFn, timestampKey(tsFn), channels, benchDir, sampleProb, false, false, false);
  }

  /**
   * @param calibrate run the calibration variant instead of the measurement,
   *     see the discussion of the timed region in {@link #invoke}
   */
  public ProvenanceQuerySink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn,
                             int channels, String benchDir, double sampleProb,
                             boolean calibrate) {
    this(tsFn, hmFn, timestampKey(tsFn), channels, benchDir, sampleProb, calibrate, false, false);
  }

  public ProvenanceQuerySink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn,
                             int channels, String benchDir, double sampleProb,
                             boolean calibrate, boolean verifyMode) {
    this(tsFn, hmFn, timestampKey(tsFn), channels, benchDir, sampleProb,
        calibrate, verifyMode, verifyMode);
  }

  public ProvenanceQuerySink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn,
                             SampleKeyFn<T> keyFn, int channels, String benchDir,
                             double sampleProb) {
    this(tsFn, hmFn, keyFn, channels, benchDir, sampleProb, false, false, false);
  }

  public ProvenanceQuerySink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn,
                             SampleKeyFn<T> keyFn, int channels, String benchDir,
                             double sampleProb, boolean calibrate) {
    this(tsFn, hmFn, keyFn, channels, benchDir, sampleProb, calibrate, false, false);
  }

  public ProvenanceQuerySink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn,
                             SampleKeyFn<T> keyFn, int channels, String benchDir,
                             double sampleProb, boolean calibrate, boolean verifyMode,
                             boolean vectorQuery) {
    this(tsFn, hmFn, keyFn, channels, benchDir, sampleProb, calibrate, verifyMode,
        vectorQuery, false, false, null, 0);
  }

  /** Hash every output into a seeded bottom-k manifest; no provenance query. */
  public ProvenanceQuerySink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn,
                             SampleKeyFn<T> keyFn, int channels, String benchDir,
                             String manifestPath, int generateCount) {
    this(tsFn, hmFn, keyFn, channels, benchDir, 0.0, false, false, false,
        true, false, manifestPath, generateCount);
  }

  /** Capture the first occurrence of each manifest ID into a query fixture. */
  public ProvenanceQuerySink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn,
                             SampleKeyFn<T> keyFn, int channels, String benchDir,
                             String manifestPath) {
    this(tsFn, hmFn, keyFn, channels, benchDir, 0.0, false, false, false,
        false, true, manifestPath, 0);
  }

  private ProvenanceQuerySink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn,
                              SampleKeyFn<T> keyFn, int channels, String benchDir,
                              double sampleProb, boolean calibrate, boolean verifyMode,
                              boolean vectorQuery, boolean generateIds, boolean sharedIds,
                              String manifestPath, int generateCount) {
    this.tsFn = tsFn;
    this.hmFn = hmFn;
    this.keyFn = keyFn;
    this.channels = channels;
    this.benchDir = benchDir;
    this.sampleProb = sampleProb;
    this.calibrate = calibrate;
    this.verifyMode = verifyMode;
    this.vectorQuery = vectorQuery;
    this.generateIds = generateIds;
    this.sharedIds = sharedIds;
    this.manifestPath = manifestPath;
    this.generateCount = generateCount;
  }

  boolean generatesIds() {
    return generateIds;
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

  int generateCount() {
    return generateCount;
  }

  static <T> SampleKeyFn<T> timestampKey(final IndexingSink.TsFn<T> tsFn) {
    return new SampleKeyFn<T>() {
      @Override
      public void writeKey(T value, java.io.DataOutput out) throws java.io.IOException {
        out.writeLong(tsFn.ts(value));
      }
    };
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    try {
      subtask = getRuntimeContext().getIndexOfThisSubtask();
      parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
    } catch (IllegalStateException e) { // used outside a Flink task (unit tests): keep what the test set
      if (parallelism == 0) {
        parallelism = 1;
      }
    }
    sampler = new LogicalSampler();
    outputs = 0;
    if (generateIds) {
      generator = new QueryIdManifest.Generator(generateCount, MANIFEST_SEED);
      return;
    }
    if (sharedIds) {
      matcher = QueryIdManifest.Matcher.load(manifestPath);
      answers = new HashMap<Long, Long>();
      captured = new HashMap<Long, HowMuch>();
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
    sampleTimes = new SampleTimes();
    checksum = 0;
    predicateChecksum = 0;
  }

  @Override
  public void invoke(T value, Context context) {
    if (generateIds) {
      outputs++;
      generator.observe(sampler.hashKey(keyFn, value));
      return;
    }
    if (sharedIds) {
      outputs++;
      long keyHash = sampler.hashKey(keyFn, value);
      if (!matcher.selectFirst(keyHash)) {
        return;
      }
      HowMuch copy = hmFn.hm(value).copy();
      long answer = copy.total();
      checksum += answer;
      Long boxed = Long.valueOf(keyHash);
      answers.put(boxed, Long.valueOf(answer));
      captured.put(boxed, copy);
      return;
    }
    outputs++;
    long keyHash = sampler.hashKey(keyFn, value);
    predicateChecksum += keyHash;

    if (verifyMode) {
      accumulate(tsFn.ts(value), keyHash, hmFn.hm(value));
      return;
    }

    // The coin fires on roughly one output in a few hundred to a few thousand,
    // so this branch is cold every time it is taken
    //
    // For each selected output, we run both calibration and measurement variants.
    // In Figure 9a, we show the difference between these two, which does not inlude the overhead of the timer.
    boolean heads = LogicalSampler.selected(keyHash, coinThreshold);
    if (heads) {
      long answer;
      if (calibrate) {
        long t0 = System.nanoTime();
        dummy++;
        long dt = System.nanoTime() - t0;
        queryNanos += dt;
        sampleTimes.add(dt);
        // Answered anyway, outside the bracket, so that the two variants do
        // identical work per output and differ only in what is timed.
        answer = timedAnswer(value);
      } else {
        long t0 = System.nanoTime();
        answer = timedAnswer(value);
        long dt = System.nanoTime() - t0;
        queryNanos += dt;
        sampleTimes.add(dt);
      }
      sampled++;
      checksum += answer;
      selectedKeys.add(keyHash);
    }
  }

  @Override
  public void close() throws Exception {
    List<ProvenanceQuerySink<T>> all = ShardGroup.arrive(token, parallelism, subtask, this);
    if (all != null) {
      mergeAndWrite(all);
    }
  }

  /** Runs in the last subtask to close: merges the partial state of all subtasks, checks it, writes the files. */
  private void mergeAndWrite(List<ProvenanceQuerySink<T>> all) throws Exception {
    long n = 0;
    for (ProvenanceQuerySink<T> part : all) {
      n += part.outputs;
    }
    if (n == 0) {
      throw new IllegalStateException("no records reached the provenance query sink");
    }
    if (generateIds) {
      QueryIdManifest.Generator merged = all.get(0).generator;
      for (int i = 1; i < all.size(); i++) {
        merged.merge(all.get(i).generator);
      }
      merged.write(manifestPath);
      return;
    }
    if (sharedIds) {
      writeSharedResults(all, n);
      return;
    }
    if (verifyMode) {
      // --- verification: per-timestamp answer sums (order-independent) ---
      // The two jobs (caps, genealog) emit the same output multiset but in
      // different order, so per-position samples are not comparable. All
      // outputs sharing a timestamp (one window boundary) form a group whose
      // answer count/sum/sum-of-squares IS order-independent; both sinks
      // accumulate every timestamp and verify.py compares them.
      // Written from the online map in ascending timestamp order so the file
      // is deterministic and order-insensitive. The map is bounded by the
      // number of distinct output timestamps, itself at most the number of
      // distinct source timestamps (measured 47K-553K entries, 4-46 MB).
      // With several subtasks the groups of one timestamp are merged first.
      //
      // Columns after group_count are 128-bit digests (hi,lo) of the sorted
      // per-output hashes, probabilistic, never exact. semantics names the
      // timed/verified query: hmp_vector or total_contributions.
      HashMap<Long, LogicalSampler.VerifyGroup> merged = all.get(0).verify;
      for (int i = 1; i < all.size(); i++) {
        for (java.util.Map.Entry<Long, LogicalSampler.VerifyGroup> e : all.get(i).verify.entrySet()) {
          LogicalSampler.VerifyGroup g = merged.get(e.getKey());
          if (g == null) {
            merged.put(e.getKey(), e.getValue());
          } else {
            g.merge(e.getValue());
          }
        }
      }
      ArrayList<Long> keys = new ArrayList<Long>(merged.keySet());
      Collections.sort(keys);
      try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/provenance_answers.csv"))) {
        pw.println("ts,group_count,vector_hi,vector_lo,payload_hi,payload_lo,total_hi,total_lo,semantics");
        for (int i = 0; i < keys.size(); i++) {
          long ts = keys.get(i).longValue();
          LogicalSampler.VerifyGroup g = merged.get(keys.get(i));
          long[] v = g.vectorDigest();
          long[] p = g.payloadDigest();
          long[] t = g.totalDigest();
          pw.println(ts + "," + g.count + "," + v[0] + "," + v[1] + ","
              + p[0] + "," + p[1] + "," + t[0] + "," + t[1] + ","
              + LogicalSampler.HMP_VECTOR);
        }
      }
      try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/outputs_caps.txt"))) {
        pw.println(n);
      }
      System.out.println("ProvenanceQuerySink: " + n + " outputs, verification over "
          + keys.size() + " timestamps");
      return;
    }
    int sampledAll = 0;
    long nanosAll = 0;
    long checksumAll = 0;
    long predicateAll = 0;
    SampleTimes timesAll = new SampleTimes();
    LogicalSampler.SelectedSet selectedAll = all.get(0).selectedKeys;
    for (int i = 0; i < all.size(); i++) {
      ProvenanceQuerySink<T> part = all.get(i);
      sampledAll += part.sampled;
      nanosAll += part.queryNanos;
      timesAll.addAll(part.sampleTimes);
      checksumAll += part.checksum;
      predicateAll += part.predicateChecksum;
      if (i > 0) {
        selectedAll.merge(part.selectedKeys);
      }
    }
    if (sampledAll == 0) {
      throw new IllegalStateException("coin never landed heads: sampleProb too small");
    }
    final int samples = sampledAll;
    final long nanos = nanosAll;
    final long checksumTotal = checksumAll;
    final String semantics = vectorQuery
        ? LogicalSampler.HMP_VECTOR : LogicalSampler.TOTAL_CONTRIBUTIONS;

    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/provenance_query.csv"))) {
      pw.println("method,outputs,samples,avg_ns,total_ms,checksum,query_semantics,predicate_checksum,unique_selected,median_ns");
      pw.printf(Locale.ROOT, "%s,%d,%d,%.1f,%.3f,%d,%s,%d,%d,%.1f%n",
          calibrate ? "caps_timer" : "caps",
          n, samples, (double) nanos / samples, nanos / 1e6, checksumTotal,
          semantics, predicateAll, selectedAll.unique(), timesAll.median());
    }
    writeSelected(benchDir + "/selected_samples_caps.csv", selectedAll);
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/outputs_caps.txt"))) {
      pw.println(n);
    }
    System.out.println("ProvenanceQuerySink: " + n + " outputs, " + samples
        + " provenance queries answered online (coin p=" + sampleProb
        + ", checksum " + checksumTotal + ", semantics " + semantics
        + ", predicate " + predicateAll + ")");
  }

  /** Timed query: scalar fold, or materialize/copy the M-long vector. */
  private long timedAnswer(T value) {
    HowMuch hm = hmFn.hm(value);
    if (vectorQuery) {
      long[] vec = hm.copyCounts();
      return LogicalSampler.hashLongs(vec);
    }
    return hm.total();
  }

  private void accumulate(long ts, long payloadHash, HowMuch hm) {
    Long key = Long.valueOf(ts);
    LogicalSampler.VerifyGroup g = verify.get(key);
    if (g == null) {
      g = new LogicalSampler.VerifyGroup();
      verify.put(key, g);
    }
    long[] vec = hm.copyCounts();
    long total = 0;
    for (int c = 0; c < vec.length; c++) {
      total += vec[c];
    }
    g.add(Long.valueOf(LogicalSampler.hashLongs(vec)), payloadHash,
        LogicalSampler.hashScalar(total));
  }

  private void writeSharedResults(List<ProvenanceQuerySink<T>> all, long n)
      throws java.io.IOException {
    // Union of the first occurrences of every subtask; an id seen on several subtasks keeps the lowest subtask's.
    HashMap<Long, Long> answersAll = new HashMap<Long, Long>();
    HashMap<Long, HowMuch> capturedAll = new HashMap<Long, HowMuch>();
    for (ProvenanceQuerySink<T> part : all) {
      for (Long id : part.answers.keySet()) {
        if (!answersAll.containsKey(id)) {
          answersAll.put(id, part.answers.get(id));
          capturedAll.put(id, part.captured.get(id));
        }
      }
    }
    int total = all.get(0).matcher.total();
    if (total != SHARED_REQUIRED) {
      throw new IllegalStateException(
          "manifest total " + total + " != " + SHARED_REQUIRED);
    }
    if (answersAll.size() != total) {
      throw new IllegalStateException(
          "manifest unmatched: remaining=" + (total - answersAll.size()));
    }
    ArrayList<Long> ids = new ArrayList<Long>(answersAll.keySet());
    Collections.sort(ids, new java.util.Comparator<Long>() { // unsigned, as in the manifest
      @Override
      public int compare(Long a, Long b) {
        return Long.compareUnsigned(a.longValue(), b.longValue());
      }
    });
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/query_answers_caps.csv"))) {
      pw.println("query_id,total_contributions");
      for (int i = 0; i < ids.size(); i++) {
        Long id = ids.get(i);
        pw.println(id.longValue() + "," + answersAll.get(id).longValue());
      }
    }
    long[] idArray = new long[ids.size()];
    Object[] queries = new Object[ids.size()];
    for (int i = 0; i < ids.size(); i++) {
      Long id = ids.get(i);
      idArray[i] = id.longValue();
      queries[i] = capturedAll.get(id);
    }
    QueryFixture.of(QueryFixture.METHOD_CAPS, channels, idArray, queries)
        .write(benchDir + "/query_fixture_caps.ser");
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/outputs_caps.txt"))) {
      pw.println(n);
    }
  }

  static void writeSelected(String path, LogicalSampler.SelectedSet set)
      throws java.io.IOException {
    try (PrintWriter pw = new PrintWriter(new FileWriter(path))) {
      pw.println("key_hash,count");
      ArrayList<long[]> rows = set.rows();
      for (int i = 0; i < rows.size(); i++) {
        long[] row = rows.get(i);
        pw.println(row[0] + "," + row[1]);
      }
    }
  }

  @SuppressWarnings("unused")
  private static long unusedLcgTouch() {
    return COIN_MULT + COIN_INC + COIN_SEED;
  }
}
