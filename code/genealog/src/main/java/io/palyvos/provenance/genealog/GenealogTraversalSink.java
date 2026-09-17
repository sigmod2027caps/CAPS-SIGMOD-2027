package io.palyvos.provenance.genealog;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import java.io.FileWriter;
import java.io.PrintWriter;
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
 * Per-tuple provenance-query benchmark for GeneaLog (how_much experiment).
 *
 * The sink keeps every output tuple in memory in timing and calibration
 * modes (an Entry per output; the extra allocation matches the submitted
 * benchmark). Graph retention is intentional and required for future
 * provenance queries: each Entry holds the output GenealogTuple, so the
 * provenance graph stays strongly reachable until close. The sink writes
 * nothing per tuple, so the job's runtime is pure
 * stream processing plus provenance maintenance — no file I/O. WHILE the
 * stream runs, a biased coin (seed 42, heads probability
 * {@code --traversalSampleProb}, the same coin sequence as CAPS's
 * ProvenanceQuerySink) is flipped for every output tuple; on heads the
 * per-tuple provenance query "how much did each source contribute to this
 * output" is answered and timed right there: a BFS traversal of the tuple's
 * provenance graph counting source hits with multiplicity.
 *
 * With {@code --traversalCalibrate} the job runs the calibration variant, which
 * times an increment instead of the traversal; the method column then reads
 * {@code genealog_timer}.
 *
 * Written to {@code --traversalBenchDir}:
 *   provenance_query_genealog.csv   method,outputs,samples,avg_ns,total_ms
 *   provenance_answers_genealog.csv ts,group_count,group_sum,group_sumsq (vs CAPS)
 *   outputs_genealog.txt            total sink outputs (memory model input)
 */
// Timestamp-grouped verification: the three methods emit the same output
// multiset but in different order, and the order of outputs sharing a
// timestamp is not even stable across two runs of the SAME job, so
// index-based or reservoir sampling cannot be compared across methods.
// Accumulating every output by timestamp makes every method cover exactly
// the same outputs regardless of order. The map is bounded by the number
// of distinct output timestamps, itself at most the number of distinct
// source timestamps (measured 47K-553K entries, 4-46 MB), not by output
// count.
//
// A submission is either timing or verification, never both. Timing (the
// default, --traversalCalibrate / --traversalSampleProb) retains every
// output as an Entry(ts, tuple) in an ArrayList so the provenance graph
// stays reachable for future provenance queries. That list is the
// published memory model; running scalars plus the coin-sampled queries
// are unchanged. Verification (--traversalVerify) must be
// a separate job: the traversal is the expensive operation the experiment
// exists to measure, so answering every output would otherwise land in the
// runtime column. Verification answers every output and accumulates
// {count, sumOfAnswers, sumOfSquares} per timestamp over ALL timestamps,
// no stride. Its timestamp-group map is enough; it need not also retain
// every tuple root because verification runtime is never published.
// Per-timestamp grouping over all timestamps is the comparison
// basis because the three methods emit the same output multiset in
// different order, and the order of outputs sharing a timestamp is not
// stable even across two runs of the same job.
//
// Sampling is now a 64-bit FNV-1a of the logical-output sample key, not
// the arrival-index LCG below. The timed query is total_contributions:
// BFS count of source leaves. GeneaLog cannot honestly answer per-source
// / per-path HMP vectors on these six dataflows — see the block at the
// bottom of this class. Verification therefore writes payload + scalar
// total digests only (vector columns are empty). Digests are
// probabilistic, never "exact".
public class GenealogTraversalSink<T extends GenealogTuple> extends RichSinkFunction<T> {

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
   * Incremented by the calibration variant inside the timed region. Volatile so
   * that neither the JIT nor the CPU can drop the write, which would leave the
   * bracket enclosing nothing at all.
   */
  private static volatile int dummy = 0;
  private static final int SHARED_REQUIRED = 1000;

  private final ExperimentSettings settings;
  private final SampleKeyFn<T> keyFn;

  private transient HashMap<Long, LogicalSampler.VerifyGroup> verify;
  /** Timing/calibration/shared: one Entry per sink output. Intentional graph retention. */
  private transient ArrayList<Entry> entries;
  private transient ProvenanceAggregateStrategy aggregateStrategy;
  private transient LogicalSampler sampler;
  private transient LogicalSampler.SelectedSet selectedKeys;
  private transient QueryIdManifest.Matcher matcher;
  private transient HashMap<Long, Long> answers;
  private transient HashMap<Long, GenealogBenchmarkTuple> captured;
  private transient long coinThreshold;
  private transient double sampleProb;
  private transient boolean calibrate;
  private transient boolean verifyMode;
  private transient boolean sharedIds;
  private transient int sampled;
  private transient long queryNanos;
  private transient long checksum;
  private transient long predicateChecksum;
  private transient long outputs;

  public GenealogTraversalSink(ExperimentSettings settings) {
    this(settings, timestampKey());
  }

  public GenealogTraversalSink(ExperimentSettings settings, SampleKeyFn<T> keyFn) {
    this.settings = settings;
    this.keyFn = keyFn;
  }

  static <T extends GenealogTuple> SampleKeyFn<T> timestampKey() {
    return new SampleKeyFn<T>() {
      @Override
      public void writeKey(T value, java.io.DataOutput out) throws java.io.IOException {
        out.writeLong(value.getTimestamp());
      }
    };
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    aggregateStrategy = settings.aggregateStrategySupplier().get();
    sampleProb = settings.traversalSampleProb();
    calibrate = settings.traversalCalibrate();
    verifyMode = settings.traversalVerify();
    sampler = new LogicalSampler();
    String queryIds = settings.traversalQueryIds();
    sharedIds = queryIds != null && !queryIds.isEmpty();
    sampled = 0;
    queryNanos = 0;
    checksum = 0;
    predicateChecksum = 0;
    outputs = 0;
    if (sharedIds) {
      matcher = QueryIdManifest.Matcher.load(queryIds);
      answers = new HashMap<Long, Long>();
      captured = new HashMap<Long, GenealogBenchmarkTuple>();
      entries = new ArrayList<Entry>();
      return;
    }
    coinThreshold = LogicalSampler.threshold53(sampleProb);
    if (verifyMode) {
      verify = new HashMap<Long, LogicalSampler.VerifyGroup>();
    } else {
      entries = new ArrayList<Entry>();
      selectedKeys = new LogicalSampler.SelectedSet();
    }
  }

  @Override
  public void invoke(T tuple, Context context) {
    outputs++;
    long keyHash = sampler.hashKey(keyFn, tuple);
    if (sharedIds) {
      if (matcher.selectFirst(keyHash)) {
        long answer = GenealogContributionQuery.count(tuple, aggregateStrategy);
        checksum += answer;
        Long boxed = Long.valueOf(keyHash);
        answers.put(boxed, Long.valueOf(answer));
        captured.put(boxed, GenealogGraphClone.cloneReachable(tuple, aggregateStrategy));
      }
      entries.add(new Entry(tuple.getTimestamp(), tuple));
      return;
    }
    predicateChecksum += keyHash;
    if (verifyMode) {
      accumulate(tuple.getTimestamp(), keyHash, GenealogContributionQuery.count(tuple, aggregateStrategy));
      return;
    }
    // Biased coin per output: on heads, answer + time the provenance query
    // NOW, on the live graph (same coin sequence as CAPS, seed 42).
    //
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
        answer = GenealogContributionQuery.count(tuple, aggregateStrategy);
      } else {
        long t0 = System.nanoTime();
        answer = GenealogContributionQuery.count(tuple, aggregateStrategy);
        queryNanos += System.nanoTime() - t0;
      }
      sampled++;
      checksum += answer;
      selectedKeys.add(keyHash);
    }
    // Result retained in the entries array for possible postprocessing.
    // Graph retention is intentional and required for future provenance queries.
    entries.add(new Entry(tuple.getTimestamp(), tuple));
  }

  @Override
  public void close() throws Exception {
    if (outputs == 0) {
      throw new IllegalStateException("no records reached the genealog traversal sink");
    }
    final String dir = settings.traversalBenchDir();
    if (sharedIds) {
      if (entries.size() != outputs) {
        throw new IllegalStateException(
            "retained entry count " + entries.size() + " != outputs " + outputs);
      }
      writeSharedResults(outputs);
      return;
    }
    if (verifyMode) {
      final long n = outputs;
      // --- verification: per-timestamp answer sums (order-independent) ---
      // Written from the online map in ascending timestamp order so the file
      // is deterministic and order-insensitive. The map is bounded by the
      // number of distinct output timestamps, itself at most the number of
      // distinct source timestamps (measured 47K-553K entries, 4-46 MB).
      //
      // vector_hi/vector_lo are left 0: GeneaLog does not recover channel
      // identity (see whyNoHmpVector below). payload + total digests are
      // 128-bit, probabilistic, never exact. semantics=total_contributions.
      ArrayList<Long> keys = new ArrayList<Long>(verify.keySet());
      Collections.sort(keys);
      try (PrintWriter pw = new PrintWriter(new FileWriter(dir + "/provenance_answers_genealog.csv"))) {
        pw.println("ts,group_count,vector_hi,vector_lo,payload_hi,payload_lo,total_hi,total_lo,semantics");
        for (int i = 0; i < keys.size(); i++) {
          long ts = keys.get(i).longValue();
          LogicalSampler.VerifyGroup g = verify.get(keys.get(i));
          long[] p = g.payloadDigest();
          long[] t = g.totalDigest();
          pw.println(ts + "," + g.count + ",0,0,"
              + p[0] + "," + p[1] + "," + t[0] + "," + t[1] + ","
              + LogicalSampler.TOTAL_CONTRIBUTIONS);
        }
      }
      try (PrintWriter pw = new PrintWriter(new FileWriter(dir + "/outputs_genealog.txt"))) {
        pw.println(n);
      }
      System.out.println("GenealogTraversalSink: " + n + " outputs, verification over "
          + keys.size() + " timestamps");
      return;
    }
    if (entries.size() != outputs) {
      throw new IllegalStateException(
          "retained entry count " + entries.size() + " != outputs " + outputs);
    }
    // Use the retained list for the reported output count. Do not clear it
    // before or during CSV writing; the list must stay live through close.
    final int n = entries.size();
    if (sampled == 0) {
      throw new IllegalStateException("coin never landed heads: sampleProb too small");
    }
    final int samples = sampled;
    final long nanos = queryNanos;

    try (PrintWriter pw = new PrintWriter(new FileWriter(dir + "/provenance_query_genealog.csv"))) {
      pw.println("method,outputs,samples,avg_ns,total_ms,checksum,query_semantics,predicate_checksum,unique_selected");
      pw.printf(Locale.ROOT, "%s,%d,%d,%.1f,%.3f,%d,%s,%d,%d%n",
          calibrate ? "genealog_timer" : "genealog",
          n, samples, (double) nanos / samples, nanos / 1e6, checksum,
          LogicalSampler.TOTAL_CONTRIBUTIONS, predicateChecksum, selectedKeys.unique());
    }
    try (PrintWriter pw = new PrintWriter(new FileWriter(dir + "/selected_samples_genealog.csv"))) {
      pw.println("key_hash,count");
      ArrayList<long[]> rows = selectedKeys.rows();
      for (int i = 0; i < rows.size(); i++) {
        long[] row = rows.get(i);
        pw.println(row[0] + "," + row[1]);
      }
    }
    try (PrintWriter pw = new PrintWriter(new FileWriter(dir + "/outputs_genealog.txt"))) {
      pw.println(n);
    }
    System.out.println("GenealogTraversalSink: " + n + " outputs kept, " + samples
        + " provenance queries answered online (coin p=" + sampleProb
        + ", checksum " + checksum + ", semantics "
        + LogicalSampler.TOTAL_CONTRIBUTIONS
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
    final String dir = settings.traversalBenchDir();
    ArrayList<Long> ids = matcher.matchedRows();
    try (PrintWriter pw = new PrintWriter(new FileWriter(dir + "/query_answers_genealog.csv"))) {
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
      GenealogBenchmarkTuple clone = captured.get(id);
      long recomputed = GenealogContributionQuery.count(clone, aggregateStrategy);
      if (recomputed != answers.get(id).longValue()) {
        throw new IllegalStateException("cloned answer " + recomputed
            + " != captured answer " + answers.get(id) + " for id " + id);
      }
      idArray[i] = id.longValue();
      queries[i] = clone;
    }
    QueryFixture.of(QueryFixture.METHOD_GENEALOG, 0, idArray, queries)
        .write(dir + "/query_fixture_genealog.ser");
    try (PrintWriter pw = new PrintWriter(new FileWriter(dir + "/outputs_genealog.txt"))) {
      pw.println(n);
    }
  }

  private static final class Entry {
    final long ts;
    final GenealogTuple tuple;

    Entry(long ts, GenealogTuple tuple) {
      this.ts = ts;
      this.tuple = tuple;
    }
  }

  private void accumulate(long ts, long payloadHash, long total) {
    Long key = Long.valueOf(ts);
    LogicalSampler.VerifyGroup g = verify.get(key);
    if (g == null) {
      g = new LogicalSampler.VerifyGroup();
      verify.put(key, g);
    }
    g.add(null, payloadHash, LogicalSampler.hashScalar(total));
  }

  /**
   * Why GeneaLog cannot recover CAPS/Ink source/path channels on these
   * six dataflows. The graph stores tuple lineage (SOURCE / MAP / JOIN /
   * AGGREGATE) and source payloads. It does not store the integer
   * channel ids CAPS and Ink assign at source-tag time. Mapping a source
   * leaf onto a channel would mean re-applying each dataflow's CAPS
   * tagging function (payload predicates and/or join-input-as-path).
   * That is not "recovering channels from the graph"; it is grafting
   * CAPS semantics onto GeneaLog leaves. We do not fake channel identity.
   *
   * <ul>
   *   <li>twitter_1: channels are PATH (PL = all-tweets agg, PR = verified
   *       agg). A verified tweet contributes on both paths. SOURCE tuples
   *       are tweets; the graph's JOIN U1/U2 are the two aggs, but nothing
   *       labels them as channel 0/1. Treating U1/U2 as PL/PR is
   *       dataflow-specific convention, not a stored channel.</li>
   *   <li>twitter_2: HAS_LOC / NO_LOC from tweet.hasLocation(). The source
   *       payload has a location field; recovering the channel is the CAPS
   *       predicate, not a graph attribute.</li>
   *   <li>taxi_1: PATH channels = branch*2 + borough. Branch is solo vs
   *       crowded (join input); borough is the virtual source. Needs both
   *       path identity and payload inspection. Neither is a channel id.</li>
   *   <li>taxi_2: SC / SK from card vs cash. Payment is a source-tuple
   *       field; the graph does not tag SC/SK.</li>
   *   <li>nexmark_1: PP / PA = persons vs auctions. Source tuples have
   *       isPerson; that is a type flag, not a CAPS channel stored on the
   *       edge.</li>
   *   <li>nexmark_2: same PP / PA on a person⋈auction join. U1/U2 are the
   *       two inputs; again, mapping them to channels is CAPS tagging.</li>
   * </ul>
   *
   * Therefore this sink answers total_contributions only. The vector HMP
   * comparison is CAPS vs Ink.
   */
  static final String whyNoHmpVector = "genealog-no-channel-identity";

  @SuppressWarnings("unused")
  private static long unusedLcgTouch() {
    return COIN_MULT + COIN_INC + COIN_SEED;
  }
}
