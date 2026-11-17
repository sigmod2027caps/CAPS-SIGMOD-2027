package caps;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import temporalindex.ArrayStore;
import temporalindex.Disorder;
import temporalindex.LongTrace;
import tieredindex.AugmentedRBTree;
import tieredindex.ShardGroup;
import tieredindex.ShardedIndex;
import tieredindex.HybridTier1;
import tieredindex.SortedBufferTier1;
import tieredindex.Tier1;
import tieredindex.TwoTierIndex;

/**
 * Base of every index-building sink (IndexingSink, QueryLengthSink, ExpirySink, SummarizeSink). It
 * replaces the slack-based reorder buffer of the released artifact with the watermark-driven
 * two-tier index.
 *
 * <pre>
 *   invoke(record)     TwoTierIndex.add:  ts &lt;= W  late, counted and written to late_*.csv, not indexed
 *                                         ts &gt;  W  Tier 1 (augmented red-black tree)
 *   accept(watermark)  TwoTierIndex.advanceWatermark: Tier-1 prefix &lt;= W migrates to Tier 2 in order
 *   close()            final flush (watermark Long.MAX_VALUE), then the shard joins its group
 * </pre>
 *
 * Every sink subtask owns one shard (a TwoTierIndex, and the baselines that must see the same
 * ordered stream, fed through the migration listener). The experiment harness runs a job on one
 * TaskManager JVM, so the shards of a sink meet in {@link ShardGroup}; the last subtask to close
 * assembles them (frontier = min shard watermark, answer = channel-wise sum) and runs the
 * benchmark in {@link #finish}. On several TaskManagers the same merge would live in a query
 * service in front of the shards; the per-output provenance queries never need it (the vector rides
 * on the tuple).
 *
 * The sink must be attached with {@link BenchSinks#attach}, which routes watermarks to
 * {@link #accept}. With window-end derived time (StampDerivedTime) every output arrives before the
 * watermark that certifies it, so no record is late; the statistics report it (late count and the
 * maximum of W - ts over the arrivals, which is negative when nothing is late).
 *
 * During the stream a small batch of provisional queries straddling the shard's current watermark
 * is run every tenth of the expected outputs (CAPS_EXPECTED_OUTPUTS, set by the experiment scripts);
 * their latency and finality are recorded. They run against the shard of the subtask that triggers
 * them, because other shards are owned by other threads; the merged queries are the post-job ones.
 *
 * Checkpointing (experiment E4): the shard is operator state. A checkpoint stores
 * <ul>
 *   <li>Tier 2 as immutable segment files (see {@link SegmentStore}): only the records migrated since the
 *       previous checkpoint are written, so the cost of a checkpoint does not grow with the index;</li>
 *   <li>in the Flink-managed operator state (one byte array per subtask): the references to the segments, Tier 1
 *       (the records ahead of the watermark), the watermark, the late counter and all statistics.</li>
 * </ul>
 * After a failure the restored shard has exactly the state it had when the barrier passed; the sources replay the
 * records that followed it, so each record contributes once. The final queries and the post-job merge run only in
 * an execution that sees the end of the input (a task that is torn down by a failure does not run them).
 * Sinks that keep replicas the index does not know about (ExpirySink, SummarizeSink) refuse to checkpoint.
 *
 * Files in benchDir written by this base: tiered_stats.csv (one row per shard),
 * provisional_queries.csv, tiered_queries.csv (final / provisional status of the benchmark
 * queries, filled by the subclasses through {@link #queryStatus}), late_&lt;sink&gt;_s&lt;i&gt;.csv (only if
 * a shard saw late records).
 */
public abstract class TieredIndexSink<T, S extends TieredIndexSink.Shard> extends RichSinkFunction<T>
    implements Consumer<Watermark>, CheckpointedFunction, Serializable {

  private static final long serialVersionUID = 1L;

  /** Provisional queries per batch. */
  static final int PROVISIONAL_BATCH = 20;

  protected final IndexingSink.TsFn<T> tsFn;
  protected final IndexingSink.HmFn<T> hmFn;
  protected final int channels;
  protected final String benchDir;
  protected final String sinkName;
  /** Length of the provisional queries (the dataflow's query window). */
  protected final long provLenMs;
  private final String token = UUID.randomUUID().toString();
  /** Outputs expected over the whole job (0 = unknown); package-visible so tests can set it. */
  long expectedOutputs;
  /** CAPS_TIER1: "hybrid" (default: ordered run plus red-black tree), "tree" (red-black tree only) or "buffer". */
  private final String tier1Kind;
  /** CAPS_INSERT_TIMING=1: nanoTime around every Tier-1 insert and split. Off in every timed run (it costs 2 clock reads per record). */
  private final boolean insertTiming;
  /** CAPS_TRACE=1: per-shard traces of the watermark steps and of sampled arrivals (experiment E3). */
  private final boolean trace;
  /** CAPS_PROVISIONAL=0: no provisional queries during the stream (timing runs). */
  private final boolean provisionalOn;
  /** CAPS_NO_SCAN=1: no ArrayStore scan baseline (timing runs: the overhead of CAPS alone). */
  private final boolean noScan;

  /** Directory of the Tier-2 segments (CAPS_SEGMENT_DIR, read on the client; default benchDir/segments). */
  String segmentDir;
  /** CAPS_INDEX_DIGEST=1: index_digest.csv with the record count, the totals and a digest of Tier 2 (E4 verification). */
  boolean digestOn;

  protected transient S shard;
  private transient SegmentStore segments;
  private transient ListState<byte[]> state;
  private transient byte[] restoredState;
  /** True once the end-of-input watermark was seen: close() then finishes the job, otherwise it is a teardown after a failure. */
  private transient boolean endSeen;
  transient boolean inTask;
  private transient int attempt;
  /** Checkpoint statistics, restored with the state (cumulative over the executions of the job). */
  transient long snapshots, snapshotNanos, fullSnapshotBytes;
  transient int restores;
  /** Time this execution spent restoring the shard (0 if it started from scratch). */
  transient long restoreNanos;
  /** Wall-clock time (epoch ms) at which this execution finished restoring (0 if it started from scratch). */
  transient long restoredAtMs;
  /** Index and number of this subtask; set by open() from the runtime context, by tests directly. */
  transient int subtask;
  transient int parallelism;
  private transient long provEvery;
  private transient long sinceProv;
  private transient long sampleEvery;
  private transient ArrayList<String[]> queryStatusRows;

  protected TieredIndexSink(IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn, int channels,
                            String benchDir, String sinkName, long provLenMs) {
    this.tsFn = tsFn;
    this.hmFn = hmFn;
    this.channels = channels;
    this.benchDir = benchDir;
    this.sinkName = sinkName;
    this.provLenMs = provLenMs;
    // Read on the client, serialized with the sink: the TaskManager's environment is not the script's.
    this.expectedOutputs = parseLong(System.getenv("CAPS_EXPECTED_OUTPUTS"));
    String kind = System.getenv("CAPS_TIER1");
    this.tier1Kind = kind == null || kind.trim().isEmpty() ? "hybrid" : kind.trim();
    this.insertTiming = "1".equals(System.getenv("CAPS_INSERT_TIMING"));
    this.trace = "1".equals(System.getenv("CAPS_TRACE"));
    this.provisionalOn = !"0".equals(System.getenv("CAPS_PROVISIONAL"));
    this.noScan = "1".equals(System.getenv("CAPS_NO_SCAN"));
    String seg = System.getenv("CAPS_SEGMENT_DIR");
    this.segmentDir = seg == null || seg.trim().isEmpty() ? benchDir + "/segments" : seg.trim();
    this.digestOn = "1".equals(System.getenv("CAPS_INDEX_DIGEST"));
  }

  private static long parseLong(String s) {
    return s == null || s.isEmpty() ? 0 : Long.parseLong(s.trim());
  }

  /** One shard per subtask: the index plus whatever a subclass needs to see the same ordered stream. */
  protected abstract S newShard(int subtask, TwoTierIndex index);

  /** Runs once, in the last subtask to close, with all shards flushed (final watermark) and ordered by subtask. */
  protected abstract void finish(List<S> shards) throws Exception;

  /** True if the sink needs the ArrayStore scan baseline in every shard. */
  protected boolean withArray() {
    return false;
  }

  // per-subtask path
  @Override
  public void open(Configuration parameters) throws Exception {
    try {
      subtask = getRuntimeContext().getIndexOfThisSubtask();
      parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
      attempt = getRuntimeContext().getAttemptNumber();
      inTask = true;
    } catch (IllegalStateException e) { // used outside a Flink task (unit tests): keep what the test set
      if (parallelism == 0)
        parallelism = 1;
    }
    Tier1 tier1 = "buffer".equals(tier1Kind) ? new SortedBufferTier1(channels)
        : "tree".equals(tier1Kind) ? new AugmentedRBTree(channels) : new HybridTier1(channels);
    TwoTierIndex index = new TwoTierIndex(channels, tier1);
    index.timing = insertTiming;
    shard = newShard(subtask, index);
    if (withArray() && !noScan)
      shard.array = new ArrayStore(channels);
    if (trace) {
      shard.steps = new LongTrace(6, 1 << 20);
      shard.samples = new LongTrace(3, 1 << 18);
      shard.pending = new PriorityQueue<>((a, b) -> Long.compare(a[0], b[0]));
      sampleEvery = expectedOutputs > 0 ? Math.max(1, expectedOutputs / (20000L * parallelism)) : 64;
    }
    index.listener = shard;
    shard.sink = this;
    provEvery = provLenMs <= 0 || !provisionalOn ? 0
        : expectedOutputs > 0 ? Math.max(1, expectedOutputs / (10L * parallelism)) : 1 << 16;
    sinceProv = 0;
    segments = new SegmentStore(segmentDir + "/" + sinkName + "/s" + subtask, channels);
    if (restoredState != null) {
      long t0 = System.nanoTime();
      restoreFrom(restoredState);
      restoreNanos = System.nanoTime() - t0;
      restoredAtMs = System.currentTimeMillis();
      restoredState = null;
    }
  }

  @Override
  public void invoke(T value, Context context) throws Exception {
    long ts = tsFn.ts(value);
    long[] v = hmFn.hm(value).counts; // add copies it: object reuse is safe
    Shard s = shard;
    s.records++;
    long w = s.index.watermark();
    if (w != Long.MIN_VALUE && w - ts > s.maxWMinusTs)
      s.maxWMinusTs = w - ts;
    if (!s.index.add(ts, v)) {
      s.late(ts, w, v);
      return;
    }
    if (s.array != null && provisionalOn)
      s.arrived(ts, v); // the exact record of what is still in Tier 1, for the oracle of the provisional batches
    if (trace && s.records % sampleEvery == 0)
      s.pending.add(new long[] {ts, Disorder.nowUs()});
    if (ts < s.minTs)
      s.minTs = ts;
    if (ts > s.maxTs)
      s.maxTs = ts;
    if (provEvery > 0 && ++sinceProv >= provEvery && w != Long.MIN_VALUE) {
      sinceProv = 0;
      provisionalBatch(s);
    }
  }

  /** Watermark callback, delivered by {@link WatermarkTriggeringSink}. */
  @Override
  public void accept(Watermark mark) {
    Shard s = shard;
    s.watermarks++;
    long w = mark.getTimestamp();
    if (w == Long.MAX_VALUE)
      endSeen = true;
    if (!trace) {
      s.index.advanceWatermark(w);
      return;
    }
    long before = s.index.tier1Size();
    long now = Disorder.nowUs();
    s.index.advanceWatermark(w);
    if (w != Long.MAX_VALUE) // the end-of-input watermark is not a step of the stream
      s.steps.add(now, w, before, s.index.tier1Size(), s.records, s.index.migratedCount);
    // sampled arrivals the watermark has just certified: delay from arrival at the sink to migration into Tier 2
    while (!s.pending.isEmpty() && s.pending.peek()[0] <= w) {
      long[] x = s.pending.poll();
      s.samples.add(x[0], x[1], now);
    }
  }

  @Override
  public void close() throws Exception {
    if (shard == null)
      return;
    if (inTask && !endSeen) { // torn down by a failure: the restored execution will finish the job
      shard.closeLate();
      return;
    }
    shard.index.advanceWatermark(Long.MAX_VALUE); // end of input; Flink's own MAX watermark has done it already
    shard.closeLate();
    if (trace)
      shard.writeTraces(benchDir, sinkName);
    List<S> all = ShardGroup.arrive(token + "#" + attempt, parallelism, subtask, shard);
    if (all == null)
      return;
    long records = 0;
    for (S s : all)
      records += s.index.tier2Size();
    if (records == 0)
      throw new IllegalStateException("no records reached the " + sinkName + " sink");
    queryStatusRows = new ArrayList<>();
    checkProvisional(all);
    writeStats(all);
    if (digestOn) {
      writeDigest(all);
      writeRecoveryStats(all);
    }
    finish(all);
    writeQueryStatus();
  }

  // checkpoint and restore
  /** Replicas of the shard that the checkpoint does not cover make a checkpoint wrong; such sinks say so. */
  protected boolean checkpointable() {
    return true;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    state = context.getOperatorStateStore().getListState(
        new ListStateDescriptor<>("caps-index-shard", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    if (context.isRestored())
      for (byte[] b : state.get()) {
        if (restoredState != null)
          throw new UnsupportedOperationException("rescaling a checkpointed index sink is not supported"
              + " (the shards would have to be merged and re-split)");
        restoredState = b;
      }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    state.clear();
    state.add(snapshotBytes());
  }

  /**
   * The checkpoint of this subtask: writes the Tier-2 records migrated since the previous checkpoint as a new
   * segment file and returns the rest of the state (references, Tier 1, watermark, statistics).
   */
  byte[] snapshotBytes() throws IOException {
    if (!checkpointable())
      throw new UnsupportedOperationException(sinkName + " keeps replicas that its checkpoint does not cover");
    long t0 = System.nanoTime();
    Shard s = shard;
    TwoTierIndex x = s.index;
    segments.flush(x);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeInt(1); // format
    out.writeInt(subtask);
    out.writeInt(channels);
    segments.write(out);
    x.writeState(out);
    tieredindex.Tier1.Entries t1 = x.tier1Entries();
    out.writeInt(t1.n);
    for (int i = 0; i < t1.n; i++)
      out.writeLong(t1.ts[i]);
    for (int i = 0; i < t1.n * channels; i++)
      out.writeLong(t1.vals[i]);
    s.writeState(out);
    out.writeLong(sinceProv);
    // what a full snapshot of Tier 2 would have written at this checkpoint (report only)
    long full = fullSnapshotBytes + (long) x.tier2Size() * 8L * (1 + channels);
    out.writeLong(snapshots + 1);
    out.writeLong(snapshotNanos + (System.nanoTime() - t0));
    out.writeLong(full);
    out.writeInt(restores);
    out.flush();
    snapshots++;
    snapshotNanos += System.nanoTime() - t0;
    fullSnapshotBytes = full;
    return bytes.toByteArray();
  }

  /** Inverse of {@link #snapshotBytes}: the shard (just created by open) takes the state of the checkpoint. */
  void restoreFrom(byte[] b) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(b));
    if (in.readInt() != 1)
      throw new IOException("unknown checkpoint format");
    if (in.readInt() != subtask || in.readInt() != channels)
      throw new IOException("checkpoint of another subtask or channel count");
    Shard s = shard;
    TwoTierIndex x = s.index;
    segments.read(in, x); // Tier 2, with the listener: the scan baseline and replicas are rebuilt
    x.readState(in);
    int n = in.readInt();
    long[] ts = new long[n];
    long[] vals = new long[n * channels];
    for (int i = 0; i < n; i++)
      ts[i] = in.readLong();
    for (int i = 0; i < vals.length; i++)
      vals[i] = in.readLong();
    x.restoreTier1(new tieredindex.Tier1.Entries(n, channels, ts, vals));
    s.readState(in);
    sinceProv = in.readLong();
    snapshots = in.readLong();
    snapshotNanos = in.readLong();
    fullSnapshotBytes = in.readLong();
    restores = in.readInt() + 1;
  }

  /** recovery_stats.csv: what the checkpoints of each shard cost (cumulative over the executions of the job). */
  private void writeRecoveryStats(List<S> all) throws IOException {
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/recovery_stats.csv"))) {
      pw.println("sink,shard,snapshots,snapshot_ms,restores,restore_ms_last_execution,restored_at_ms,segments,"
          + "segment_bytes_written_last_execution,segment_bytes_total,full_snapshot_bytes_total");
      for (S s : all) {
        TieredIndexSink<?, ?> k = s.sink;
        long segmentBytes = 0;
        for (long l : k.segments.lengths)
          segmentBytes += 8 + l * 8L * (1 + channels);
        pw.printf(Locale.ROOT, "%s,%d,%d,%.3f,%d,%.3f,%d,%d,%d,%d,%d%n", sinkName, s.subtask, k.snapshots,
            k.snapshotNanos / 1e6, k.restores, k.restoreNanos / 1e6, k.restoredAtMs, k.segments.lengths.size(),
            k.segments.bytesWritten, segmentBytes, k.fullSnapshotBytes);
      }
    }
  }

  /** index_digest.csv: per shard the Tier-2 record count, the per-channel totals and a digest of the records. */
  private void writeDigest(List<S> all) throws IOException {
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/index_digest.csv"))) {
      StringBuilder h = new StringBuilder("sink,shard,tier2_records,tier1_records,watermark,digest");
      for (int c = 0; c < channels; c++)
        h.append(",total_c").append(c);
      pw.println(h);
      for (S s : all) {
        TwoTierIndex x = s.index;
        long[] total = new long[channels];
        if (x.tier2Size() > 0) {
          double[] t = x.tier2().howMuch(Long.MIN_VALUE, Long.MAX_VALUE);
          for (int c = 0; c < channels; c++)
            total[c] = Math.round(t[c]);
        }
        StringBuilder r = new StringBuilder().append(sinkName).append(',').append(s.subtask).append(',')
            .append(x.tier2Size()).append(',').append(x.tier1Size()).append(',').append(x.watermark())
            .append(',').append(Long.toHexString(x.tier2Digest()));
        for (long t : total)
          r.append(',').append(t);
        pw.println(r);
      }
    }
  }

  // provisional queries during the stream
  /**
   * PROVISIONAL_BATCH queries of length provLenMs whose range contains the shard's watermark W in
   * its interior (from in (W - len, W), to = from + len > W), so each is answered from both tiers
   * and is not final.
   */
  private void provisionalBatch(Shard s) {
    long w = s.index.watermark();
    Provisional p = new Provisional();
    p.afterRecords = s.records;
    p.watermark = w;
    p.tier1 = s.index.tier1Size();
    p.n = PROVISIONAL_BATCH;
    p.from = new long[p.n];
    p.to = new long[p.n];
    p.counts = new long[p.n][];
    for (int i = 0; i < p.n; i++) {
      p.from[i] = w - provLenMs + provLenMs * (i + 1L) / (p.n + 1L);
      p.to[i] = p.from[i] + provLenMs;
    }
    long checksum = 0;
    long t0 = System.nanoTime();
    for (int i = 0; i < p.n; i++) {
      tieredindex.Answer a = s.index.query(p.from[i], p.to[i]);
      checksum += a.counts[0];
      p.counts[i] = a.counts;
      if (!a.isFinal)
        p.provisional++;
      if (a.defined)
        p.defined++;
    }
    p.nanos = System.nanoTime() - t0;
    s.checksum += checksum;
    if (s.array != null)
      checkAgainstArrivals(s, p);
    s.provisional.add(p);
  }

  /**
   * Oracle for the provisional answers of a batch (outside the timed region): the answer to [from, to] must equal the
   * scan baseline over the records Tier 2 holds (ts <= W, the replica of the migrated stream) plus the sum over the
   * arrived records above W that were not migrated yet, kept in a plain list. A Tier-1 range sum that undercounts
   * would pass the dominance check of checkProvisional; it cannot pass this one. Mismatches count as violations.
   */
  private void checkAgainstArrivals(Shard s, Provisional p) {
    long w = p.watermark;
    s.dropMigrated(w);
    long[] expect = new long[channels];
    for (int i = 0; i < p.n; i++) {
      java.util.Arrays.fill(expect, 0);
      long hi = Math.min(p.to[i], w);
      if (p.from[i] <= hi) {
        long[] x = s.array.howMuch(p.from[i], hi);
        for (int c = 0; c < channels; c++)
          expect[c] += x[c];
      }
      for (int j = 0; j < s.arrN; j++)
        if (s.arrTs[j] >= p.from[i] && s.arrTs[j] <= p.to[i])
          for (int c = 0; c < channels; c++)
            expect[c] += s.arrVals[j * channels + c];
      if (!java.util.Arrays.equals(expect, p.counts[i]))
        p.violations++;
    }
  }

  // statistics
  private void checkProvisional(List<S> all) {
    for (S s : all)
      for (Provisional p : s.provisional)
        for (int i = 0; i < p.n; i++) {
          tieredindex.Answer fin = s.index.query(p.from[i], p.to[i]);
          // Later records can only add: the final answer dominates the provisional one in every channel.
          boolean ok = fin.isFinal;
          for (int c = 0; c < channels; c++)
            ok &= fin.counts[c] >= p.counts[i][c];
          if (!ok)
            p.violations++;
        }
  }

  private void writeStats(List<S> all) throws IOException {
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/tiered_stats.csv"))) {
      pw.println("sink,shard,parallelism,records,indexed,late,migrated,tier1_peak,tier1_final,"
          + "insert_ns,split_ns,tier2_append_ns,listener_ns,watermarks,max_w_minus_ts_ms,min_ts,max_ts,"
          + "behind_watermark,nonmonotone_appends");
      for (S s : all) {
        TwoTierIndex x = s.index;
        pw.printf(Locale.ROOT, "%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s,%d,%d,%d,%d%n",
            sinkName, s.subtask, all.size(), s.records, s.records - x.lateCount, x.lateCount,
            x.migratedCount, x.peakTier1, x.tier1Size(), x.insertNanos, x.splitNanos,
            x.migrationNanos, x.listenerNanos, s.watermarks,
            s.maxWMinusTs == Long.MIN_VALUE ? "NA" : Long.toString(s.maxWMinusTs),
            s.minTs, s.maxTs, s.behindWatermark, s.nonMonotone);
      }
    }
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/provisional_queries.csv"))) {
      pw.println("sink,shard,batch,after_records,watermark,tier1_size,queries,avg_us,provisional,"
          + "defined,monotone_violations");
      for (S s : all) {
        int b = 0;
        for (Provisional p : s.provisional)
          pw.printf(Locale.ROOT, "%s,%d,%d,%d,%d,%d,%d,%.3f,%d,%d,%d%n", sinkName, s.subtask, b++,
              p.afterRecords, p.watermark, p.tier1, p.n, p.nanos / 1000.0 / p.n, p.provisional,
              p.defined, p.violations);
      }
    }
    long late = 0;
    int bad = 0;
    for (S s : all) {
      late += s.index.lateCount;
      for (Provisional p : s.provisional)
        bad += p.violations + (p.n - p.provisional);
    }
    if (bad > 0)
      throw new IllegalStateException(bad + " provisional queries were final or exceeded their final answer");
    long peak = 0;
    for (S s : all)
      peak = Math.max(peak, s.index.peakTier1);
    System.out.println(sinkName + " sink: " + all.size() + " shard(s), late=" + late
        + " peak tier1 (max shard)=" + peak);
  }

  /** Records the finality of the answers of one family of benchmark queries (written to tiered_queries.csv). */
  protected void queryStatus(String query, int n, int finals, int defined) {
    queryStatusRows.add(new String[] {query, Integer.toString(n), Integer.toString(finals),
        Integer.toString(n - finals), Integer.toString(defined), Integer.toString(n - defined)});
  }

  private void writeQueryStatus() throws IOException {
    try (PrintWriter pw = new PrintWriter(new FileWriter(benchDir + "/tiered_queries.csv"))) {
      pw.println("query,n,final,provisional,defined,undefined");
      for (String[] r : queryStatusRows)
        pw.println(String.join(",", r));
    }
  }

  // helpers for the subclasses
  protected static ShardedIndex assemble(List<? extends Shard> shards) {
    TwoTierIndex[] idx = new TwoTierIndex[shards.size()];
    for (int i = 0; i < idx.length; i++)
      idx[i] = shards.get(i).index;
    return new ShardedIndex(idx);
  }

  protected static ArrayStore[] arrays(List<? extends Shard> shards) {
    ArrayStore[] a = new ArrayStore[shards.size()];
    for (int i = 0; i < a.length; i++)
      a[i] = shards.get(i).array;
    return a;
  }

  protected static long minTs(List<? extends Shard> shards) {
    long m = Long.MAX_VALUE;
    for (Shard s : shards)
      m = Math.min(m, s.minTs);
    return m;
  }

  protected static long maxTs(List<? extends Shard> shards) {
    long m = Long.MIN_VALUE;
    for (Shard s : shards)
      m = Math.max(m, s.maxTs);
    return m;
  }

  /** Channel-wise sum of the scan baseline over the shards (they partition the outputs). */
  protected static long[] scan(ArrayStore[] arrays, long from, long to, int channels) {
    long[] sum = new long[channels];
    for (ArrayStore a : arrays) {
      long[] x = a.howMuch(from, to);
      for (int c = 0; c < channels; c++)
        sum[c] += x[c];
    }
    return sum;
  }

  protected static int scanCount(ArrayStore[] arrays, long from, long to) {
    int n = 0;
    for (ArrayStore a : arrays)
      n += a.countIn(from, to);
    return n;
  }

  /** Fixed-length windows: uniformly random start, end = start + lenMs (clamped to the stream span). */
  protected static long[][] randomRanges(int n, long minTs, long maxTs, long lenMs,
                                         java.util.Random rnd) {
    long len = Math.min(lenMs, maxTs - minTs);
    long[][] ranges = new long[n][2];
    for (int i = 0; i < n; i++) {
      long start = minTs + (long) (rnd.nextDouble() * (maxTs - minTs - len));
      ranges[i][0] = start;
      ranges[i][1] = start + len;
    }
    return ranges;
  }

  // shard state
  /** State of one sink subtask. Subclasses add their replicas and override migrated(). */
  public static class Shard implements TwoTierIndex.Listener {
    public final int subtask;
    public final TwoTierIndex index;
    /** The scan baseline, fed by the same ordered stream as Tier 2 (null unless the sink asks for it). */
    public ArrayStore array;
    public long minTs = Long.MAX_VALUE;
    public long maxTs = Long.MIN_VALUE;
    public long records;
    public long watermarks;
    /** Largest (watermark at arrival - timestamp): negative if every arrival precedes its watermark. */
    public long maxWMinusTs = Long.MIN_VALUE;
    public long checksum;
    /** Records migrated to Tier 2 with a timestamp above the watermark that certified them (impossible by construction). */
    public long behindWatermark;
    /** Tier-2 appends whose timestamp is below that of the previous append (the append-only invariant). */
    public long nonMonotone;
    private long lastMigrated = Long.MIN_VALUE;
    LongTrace steps;
    LongTrace samples;
    PriorityQueue<long[]> pending;
    public final List<Provisional> provisional = new ArrayList<>();
    TieredIndexSink<?, ?> sink;
    private PrintWriter late;
    private boolean lateAppend;
    private long[] row;
    // The accepted arrivals that may still be in Tier 1 (the oracle of the provisional batches); trimmed to ts > W at every batch.
    long[] arrTs = new long[1024];
    long[] arrVals;
    int arrN;
    int arrChannels;

    void arrived(long ts, long[] v) {
      if (arrVals == null) {
        arrChannels = v.length;
        arrVals = new long[arrTs.length * arrChannels];
      }
      if (arrN == arrTs.length) {
        arrTs = java.util.Arrays.copyOf(arrTs, arrN * 2);
        arrVals = java.util.Arrays.copyOf(arrVals, arrN * 2 * arrChannels);
      }
      arrTs[arrN] = ts;
      System.arraycopy(v, 0, arrVals, arrN * arrChannels, arrChannels);
      arrN++;
    }

    /** Keeps only the arrivals with ts > w: the others were migrated and the scan baseline has them. */
    void dropMigrated(long w) {
      int k = 0;
      for (int j = 0; j < arrN; j++)
        if (arrTs[j] > w) {
          if (k != j) {
            arrTs[k] = arrTs[j];
            System.arraycopy(arrVals, j * arrChannels, arrVals, k * arrChannels, arrChannels);
          }
          k++;
        }
      arrN = k;
    }

    public Shard(int subtask, TwoTierIndex index) {
      this.subtask = subtask;
      this.index = index;
    }

    @Override
    public void migrated(long ts, long[] vals, int off) {
      if (ts > index.watermark())
        behindWatermark++;
      if (ts < lastMigrated)
        nonMonotone++;
      lastMigrated = ts;
      if (array == null)
        return;
      if (row == null)
        row = new long[index.channels()];
      System.arraycopy(vals, off, row, 0, row.length);
      array.append(ts, row);
    }

    void late(long ts, long w, long[] v) throws IOException {
      if (late == null) {
        // after a restore the file of the earlier execution is continued (it already has its header)
        late = new PrintWriter(new FileWriter(sink.benchDir + "/late_" + sink.sinkName + "_s"
            + subtask + ".csv", lateAppend));
        if (!lateAppend) {
          StringBuilder h = new StringBuilder("ts,watermark");
          for (int c = 0; c < v.length; c++)
            h.append(",c").append(c);
          late.println(h);
        }
      }
      StringBuilder sb = new StringBuilder().append(ts).append(',').append(w);
      for (long x : v)
        sb.append(',').append(x);
      late.println(sb);
    }

    void writeTraces(String dir, String sinkName) throws IOException {
      steps.write(dir + "/tier1_steps_" + sinkName + "_s" + subtask + ".csv",
          "proc_us,watermark,tier1_before,tier1_after,arrivals,migrated");
      samples.write(dir + "/finality_samples_" + sinkName + "_s" + subtask + ".csv", "ts,arrival_us,final_us");
    }

    /** The counters of the shard and its provisional batches (the index itself is written by the sink). */
    void writeState(DataOutputStream out) throws IOException {
      out.writeLong(minTs);
      out.writeLong(maxTs);
      out.writeLong(records);
      out.writeLong(watermarks);
      out.writeLong(maxWMinusTs);
      out.writeLong(checksum);
      out.writeLong(behindWatermark);
      out.writeLong(nonMonotone);
      out.writeLong(lastMigrated);
      out.writeInt(provisional.size());
      for (Provisional p : provisional) {
        out.writeLong(p.afterRecords);
        out.writeLong(p.watermark);
        out.writeInt(p.tier1);
        out.writeInt(p.n);
        out.writeLong(p.nanos);
        out.writeInt(p.provisional);
        out.writeInt(p.defined);
        out.writeInt(p.violations);
        for (int i = 0; i < p.n; i++) {
          out.writeLong(p.from[i]);
          out.writeLong(p.to[i]);
          out.writeInt(p.counts[i].length);
          for (long c : p.counts[i])
            out.writeLong(c);
        }
      }
    }

    void readState(DataInputStream in) throws IOException {
      minTs = in.readLong();
      maxTs = in.readLong();
      records = in.readLong();
      watermarks = in.readLong();
      maxWMinusTs = in.readLong();
      checksum = in.readLong();
      behindWatermark = in.readLong();
      nonMonotone = in.readLong();
      lastMigrated = in.readLong();
      int batches = in.readInt();
      provisional.clear();
      for (int b = 0; b < batches; b++) {
        Provisional p = new Provisional();
        p.afterRecords = in.readLong();
        p.watermark = in.readLong();
        p.tier1 = in.readInt();
        p.n = in.readInt();
        p.nanos = in.readLong();
        p.provisional = in.readInt();
        p.defined = in.readInt();
        p.violations = in.readInt();
        p.from = new long[p.n];
        p.to = new long[p.n];
        p.counts = new long[p.n][];
        for (int i = 0; i < p.n; i++) {
          p.from[i] = in.readLong();
          p.to[i] = in.readLong();
          p.counts[i] = new long[in.readInt()];
          for (int c = 0; c < p.counts[i].length; c++)
            p.counts[i][c] = in.readLong();
        }
        provisional.add(p);
      }
      lateAppend = index.lateCount > 0;
    }

    void closeLate() {
      if (late != null)
        late.close();
    }
  }

  /** One batch of provisional queries run during the stream. */
  public static final class Provisional {
    long afterRecords;
    long watermark;
    int tier1;
    int n;
    long nanos;
    int provisional;
    int defined;
    int violations;
    long[] from;
    long[] to;
    long[][] counts;
  }
}
