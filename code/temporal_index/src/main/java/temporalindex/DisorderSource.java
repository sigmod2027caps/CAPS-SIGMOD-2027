package temporalindex;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.concurrent.locks.LockSupport;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

// Parallel, replayable file source with controlled disorder and a slow-partition knob (experiment E3).
//
// Reader j of p reads the lines i of the file with i mod p = j (it scans the whole file for the line breaks,
// but parses only its own lines), so every reader sees all of event time and p readers are p independent
// "partitions" with their own watermarks. The input files are sorted by timestamp.
//
// Disorder: line i gets a delay d_i drawn from [0, D] (a hash of the seed and i, so it is the same at every
// parallelism). The event time of the record stays its timestamp ts_i; the arrival order is that of
// (ts_i + d_i): a record is held in a buffer and released as soon as no later line can overtake it, which is
// when the file has advanced to ts_i + d_i. Every record is therefore emitted after at most D of event time of
// reordering, and the buffer holds at most the records of D ms. D = 0 is the plain file order (no buffer).
//
// Slow partition: reader CAPS_SLOW_SUBTASK sleeps (factor - 1) times the time of every batch of records, so it
// runs `factor` times slower than its natural pace and its watermark lags the other readers'.
//
// Idleness (tau): a sleep that lasts tau or longer marks the reader temporarily idle after tau of processing
// time without a record (SourceContext.markAsTemporarilyIdle; Flink 1.10 has no WatermarkStrategy.withIdleness).
// The next record makes it active again.
//
// Checkpointing (experiment E4): the reader is a CheckpointedFunction. Its state is the position of the
// next unread line of its own (byte offset and line number), the tie-break counter and the number of records emitted,
// and the records of the disorder buffer (read but not yet emitted, serialized with the stream's TypeSerializer). The
// position moves only together with the emission of the record(s) it covers, both under the checkpoint lock, so every
// snapshot is a consistent cut. A checkpointed reader takes the lock once per batch of up to CHECKPOINT_BATCH lines
// (a batch also ends before a sleep and at the end of the file), not once per line: it parses the lines of a batch
// outside the lock, then places them and moves the position past all of them in one locked step. The position never
// runs ahead of the emitted records (the lines still waiting in a batch are not yet part of it, and a restored reader
// reads them again). Taking the lock per line cost taxi_2 a third of its runtime. A restored reader re-reads from the
// stored offset and emits exactly the records that followed the cut, in the same order (the delays are a function of
// the seed and of the line number). Idleness (tau) is not applied after a restore: the replay must not let processing
// time decide which records a watermark covers.
//
// Pacing (CAPS_PACE_SECONDS): the reader follows a schedule, the position p of the file at paceStart + p / size *
// paceSeconds, and sleeps whenever it is more than PACE_MIN_WAIT_MS ahead of it. The check is made for every line, so
// the event time between two sleeps is a few lines, not a batch: the watermark then trails the data by the watermark
// interval of processing time (times the event time per second of the schedule), which is what E3 needs to make
// delta commensurable with the lag of the watermark.
//
// Fault injection (CAPS_FAIL_AT): the reader of subtask 0 throws once, in its first execution attempt.
//
// Per reader the source records what the analysis needs (only if CAPS_TRACE_DIR is set): how many records came
// later than delta behind the newest one (violations of the declared out-of-orderness), the largest such
// disorder, and a trace (processing time, newest event time emitted, records emitted) about every fifth of a
// watermark interval, from which the delay to finality is measured from the moment event time was ingested.
public class DisorderSource<T> extends RichParallelSourceFunction<T> implements CheckpointedFunction {

  /** Turns one line of the file into a record (null: the line is skipped). */
  public abstract static class Parser<T> implements Serializable {
    public void open(DisorderConfig cfg) throws Exception {}

    /** lineNo is the 0-based number of the line in the file (all lines count). */
    public abstract T parse(String line, long lineNo);
  }

  public interface TsFn<T> extends Serializable {
    long ts(T record);
  }

  private static final long serialVersionUID = 1L;
  private static final int MAX_BUFFER = 1 << 24;
  /** Lines a checkpointed reader places per acquisition of the checkpoint lock. */
  static final int CHECKPOINT_BATCH = 1000;
  /** A paced reader that is less than this many ms ahead of its schedule does not sleep (a sleep costs about 0.06 ms). */
  private static final double PACE_MIN_WAIT_MS = 0.1;

  private final String path;
  private final Parser<T> parser;
  private final TsFn<T> tsFn;
  private final DisorderConfig cfg;
  private final String name;
  /** Serializer of the records in the disorder buffer (null: the source cannot be checkpointed while the buffer is not empty). */
  private final TypeSerializer<T> serializer;
  private volatile boolean running = true;

  private transient PriorityQueue<Pending<T>> buffer;
  private transient long seq, emitted, violations, maxTs, maxDisorder, maxBuffer, idleMarks, sleptNs;
  private transient long firstUs, lastTraceUs, batchStartNs;
  private transient long[] trace; // 3 longs per row: processing time (us), newest event time, emitted
  private transient int traceRows;
  private transient boolean slow;
  // Position of the next unread line of this reader (updated with each of its lines, under the checkpoint lock).
  private transient long nextOffset, nextLine;
  /** CAPS_PACE_SECONDS: wall-clock time (epoch ms) of the first start of the reader; part of the checkpoint, so a replay catches up to the schedule. */
  private transient long paceStartMs;
  private transient ListState<byte[]> state;
  private transient byte[] restoredState;
  private transient boolean replaying;
  // The lines parsed but not yet placed (a checkpointed reader): record, line number, offset after the line
  private transient ArrayList<T> batchRecs;
  private transient long[] batchLine, batchEnd;
  private transient int batchN;
  private transient long wallBaseMs, nanoBase;

  public DisorderSource(String name, String path, Parser<T> parser, TsFn<T> tsFn, DisorderConfig cfg) {
    this(name, path, parser, tsFn, cfg, null);
  }

  public DisorderSource(String name, String path, Parser<T> parser, TsFn<T> tsFn, DisorderConfig cfg,
                        TypeSerializer<T> serializer) {
    this.name = name;
    this.path = path;
    this.parser = parser;
    this.tsFn = tsFn;
    this.cfg = cfg;
    this.serializer = serializer;
  }

  private static final class Pending<T> {
    final long key, seq;
    final T rec;

    Pending(long key, long seq, T rec) {
      this.key = key;
      this.seq = seq;
      this.rec = rec;
    }
  }

  @Override
  public void run(SourceContext<T> ctx) throws Exception {
    final int p = getRuntimeContext().getNumberOfParallelSubtasks();
    final int me = getRuntimeContext().getIndexOfThisSubtask();
    parser.open(cfg);
    buffer = new PriorityQueue<>(1024, (a, b) -> a.key != b.key ? Long.compare(a.key, b.key) : Long.compare(a.seq, b.seq));
    seq = emitted = violations = maxDisorder = maxBuffer = idleMarks = sleptNs = 0;
    maxTs = Long.MIN_VALUE;
    trace = new long[3 * 1024];
    traceRows = 0;
    slow = cfg.slowFactor > 1 && me == Math.min(cfg.slowSubtask, p - 1) && p > 1;
    firstUs = lastTraceUs = Disorder.nowUs();
    batchStartNs = System.nanoTime();

    final Object lock = ctx.getCheckpointLock();
    // Only a checkpointed reader needs its lines, emissions and position to form atomic steps. Taking the checkpoint lock
    // around them nests it with the lock that collect() takes, which costs a third of the runtime of a cheap dataflow
    // (taxi_2 without provenance: 8.0 s against 5.9 s), so an unchecked reader does without.
    final boolean checkpointed = cfg.checkpointMs > 0;
    batchRecs = new ArrayList<>(CHECKPOINT_BATCH);
    batchLine = new long[CHECKPOINT_BATCH];
    batchEnd = new long[CHECKPOINT_BATCH];
    batchN = 0;
    wallBaseMs = System.currentTimeMillis();
    nanoBase = System.nanoTime();
    final boolean injectHere = cfg.failAt > 0 && me == 0 && getRuntimeContext().getAttemptNumber() == 0
        && (cfg.failSource.isEmpty() || cfg.failSource.equals(name));
    long filePos = 0; // file offset of buf[0]
    long lineNo = 0;
    if (restoredState != null) { // resume at the stored position, with the stored buffer
      restore(restoredState);
      restoredState = null;
      filePos = nextOffset;
      lineNo = nextLine;
      replaying = true;
    }
    nextOffset = filePos;
    nextLine = lineNo;
    final long fileBytes = Math.max(1, new java.io.File(path).length());
    if (paceStartMs == 0)
      paceStartMs = wallBaseMs;

    try (FileInputStream in = new FileInputStream(path)) {
      in.getChannel().position(filePos);
      byte[] buf = new byte[1 << 20];
      byte[] carry = new byte[1 << 10];
      int carryLen = 0;
      int n;
      while (running && (n = in.read(buf)) > 0) {
        int start = 0;
        for (int i = 0; i < n && running; i++) {
          if (buf[i] != '\n')
            continue;
          if (lineNo % p == me) {
            String s;
            if (carryLen == 0)
              s = new String(buf, start, i - start, StandardCharsets.UTF_8);
            else {
              carry = ensure(carry, carryLen + i - start);
              System.arraycopy(buf, start, carry, carryLen, i - start);
              s = new String(carry, 0, carryLen + i - start, StandardCharsets.UTF_8);
            }
            T rec = parse(s, lineNo);
            if (rec != null) {
              if (checkpointed) { // placed with the next batch, under the lock
                batchRecs.add(rec);
                batchLine[batchN] = lineNo;
                batchEnd[batchN++] = filePos + i + 1;
                if (batchN == CHECKPOINT_BATCH)
                  flushBatch(ctx, lock);
              } else // nothing reads the position: no need for the lock (collect takes it anyway)
                place(ctx, rec, lineNo);
            }
            if (cfg.paceSeconds > 0) {
              // E4, E3: the file is read on a schedule, position p at paceStart + p / size * paceSeconds; a reader that is
              // behind the schedule (the replay after a restore) reads at full speed until it has caught up
              double due = paceStartMs + (double) (filePos + i + 1) / fileBytes * cfg.paceSeconds * 1000.0;
              double wait = due - nowMs();
              if (wait >= PACE_MIN_WAIT_MS) {
                flushBatch(ctx, lock); // nothing waits in a batch while the reader sleeps
                sleepNs((long) (wait * 1e6));
              }
            }
            if (injectHere && emitted >= cfg.failAt)
              throw new IllegalStateException("CAPS fault injection: reader " + name + " fails after " + emitted + " records");
          }
          carryLen = 0;
          lineNo++;
          start = i + 1;
        }
        flushBatch(ctx, lock); // the end of a chunk of the file
        if (start < n) {
          carry = ensure(carry, carryLen + n - start);
          System.arraycopy(buf, start, carry, carryLen, n - start);
          carryLen += n - start;
        }
        filePos += n;
      }
      if (running && carryLen > 0 && lineNo % p == me) { // last line without a line break
        T rec = parse(new String(carry, 0, carryLen, StandardCharsets.UTF_8), lineNo);
        synchronized (lock) {
          if (rec != null)
            place(ctx, rec, lineNo);
          nextOffset = filePos;
          nextLine = lineNo + 1;
        }
      }
    }
    synchronized (lock) {
      while (running && !buffer.isEmpty()) {
        Pending<T> x = buffer.poll();
        emit(ctx, x.rec, tsFn.ts(x.rec));
      }
    }
    addTrace(Disorder.nowUs());
    if (!cfg.traceDir.isEmpty())
      writeFiles(me);
  }

  /** Places the parsed lines of the batch and moves the position past them, in one step under the checkpoint lock. */
  private void flushBatch(SourceContext<T> ctx, Object lock) {
    if (batchN == 0)
      return;
    synchronized (lock) {
      for (int k = 0; k < batchN; k++)
        place(ctx, batchRecs.get(k), batchLine[k]);
      nextOffset = batchEnd[batchN - 1];
      nextLine = batchLine[batchN - 1] + 1;
    }
    batchRecs.clear();
    batchN = 0;
  }

  /** Wall-clock time in ms (epoch) with the resolution of nanoTime. */
  private double nowMs() {
    return wallBaseMs + (System.nanoTime() - nanoBase) / 1e6;
  }

  /** Sleeps for ns nanoseconds (parkNanos: a sleep of well under a millisecond is possible). */
  private void sleepNs(long ns) throws InterruptedException {
    long end = System.nanoTime() + ns;
    long left;
    while (running && (left = end - System.nanoTime()) > 0) {
      LockSupport.parkNanos(left);
      if (Thread.interrupted())
        throw new InterruptedException();
    }
  }

  private static byte[] ensure(byte[] b, int len) {
    return len <= b.length ? b : Arrays.copyOf(b, Math.max(len, 2 * b.length));
  }

  /** The record of a line, or null if the line is empty or the parser skips it. */
  private T parse(String raw, long lineNo) {
    String s = raw.trim();
    return s.isEmpty() ? null : parser.parse(s, lineNo);
  }

  /** Emits the record, or holds it back in the disorder buffer (called with the checkpoint lock). */
  private void place(SourceContext<T> ctx, T rec, long lineNo) {
    long ts = tsFn.ts(rec);
    if (cfg.disorderMs == 0) {
      emit(ctx, rec, ts);
      return;
    }
    long delay = Zipf.mix(cfg.disorderSeed ^ (lineNo * 0x9E3779B97F4A7C15L)) >>> 1;
    delay %= cfg.disorderMs + 1;
    buffer.add(new Pending<>(ts + delay, seq++, rec));
    if (buffer.size() > maxBuffer) {
      maxBuffer = buffer.size();
      if (maxBuffer > MAX_BUFFER)
        throw new IllegalStateException("disorder buffer exceeds " + MAX_BUFFER + " records");
    }
    // The file is sorted: no later line has a key below its own timestamp, which is >= ts.
    while (!buffer.isEmpty() && buffer.peek().key <= ts) {
      Pending<T> x = buffer.poll();
      emit(ctx, x.rec, tsFn.ts(x.rec));
    }
  }

  private void emit(SourceContext<T> ctx, T rec, long ts) {
    ctx.collect(rec);
    emitted++;
    if (ts > maxTs)
      maxTs = ts;
    else {
      long behind = maxTs - ts;
      if (behind > maxDisorder)
        maxDisorder = behind;
      if (behind > cfg.deltaMs)
        violations++;
    }
    if ((emitted & 127) == 0 && !cfg.traceDir.isEmpty()) { // the trace is only kept if it is written
      long now = Disorder.nowUs();
      if (now - lastTraceUs >= cfg.wmIntervalMs * 200)
        addTrace(now);
    }
    if (slow && emitted % cfg.slowBatch == 0) {
      long elapsed = System.nanoTime() - batchStartNs;
      pause(ctx, (long) ((cfg.slowFactor - 1) * elapsed));
      batchStartNs = System.nanoTime();
    }
  }

  private void pause(SourceContext<T> ctx, long ns) {
    if (ns <= 0)
      return;
    long idleNs = replaying ? 0 : cfg.idleMs * 1_000_000L; // tau is off while replaying
    try {
      if (idleNs > 0 && ns >= idleNs) {
        sleep(idleNs);
        ctx.markAsTemporarilyIdle(); // after tau without a record
        idleMarks++;
        sleep(ns - idleNs);
      } else
        sleep(ns);
    } catch (InterruptedException e) {
      running = false;
      Thread.currentThread().interrupt();
    }
  }

  private void sleep(long ns) throws InterruptedException {
    sleptNs += ns;
    Thread.sleep(ns / 1_000_000L, (int) (ns % 1_000_000L));
  }

  private void addTrace(long nowUs) {
    lastTraceUs = nowUs;
    if (3 * (traceRows + 1) > trace.length)
      trace = Arrays.copyOf(trace, 2 * trace.length);
    trace[3 * traceRows] = nowUs;
    trace[3 * traceRows + 1] = maxTs;
    trace[3 * traceRows + 2] = emitted;
    traceRows++;
  }

  private void writeFiles(int me) throws IOException {
    try (PrintWriter pw = new PrintWriter(new FileWriter(cfg.traceDir + "/src_stats_" + name + "_s" + me + ".csv"))) {
      pw.println("source,subtask,emitted,violations,max_disorder_ms,max_buffer,idle_marks,sleep_ms,first_us,last_us");
      pw.printf(Locale.ROOT, "%s,%d,%d,%d,%d,%d,%d,%d,%d,%d%n", name, me, emitted, violations, maxDisorder,
          maxBuffer, idleMarks, sleptNs / 1_000_000L, firstUs, lastTraceUs);
    }
    try (PrintWriter pw = new PrintWriter(new FileWriter(cfg.traceDir + "/src_trace_" + name + "_s" + me + ".csv"))) {
      pw.println("proc_us,max_ts,emitted");
      for (int r = 0; r < traceRows; r++)
        pw.println(trace[3 * r] + "," + trace[3 * r + 1] + "," + trace[3 * r + 2]);
    }
  }

  // checkpoint and restore
  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    state = context.getOperatorStateStore().getListState(
        new ListStateDescriptor<>("caps-source-position", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    if (context.isRestored()) {
      for (byte[] b : state.get()) {
        if (restoredState != null)
          throw new IllegalStateException("source " + name + ": rescaling a checkpointed reader is not supported");
        restoredState = b;
      }
    }
  }

  /** Called by Flink under the checkpoint lock, between two steps of run(). */
  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    if (cfg.checkpointMs <= 0)
      throw new IllegalStateException("source " + name + " is checkpointed but CAPS_CHECKPOINT_MS is 0: its lines and "
          + "position are only atomic steps for a checkpointed reader");
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeInt(1); // format
    out.writeLong(nextOffset);
    out.writeLong(nextLine);
    out.writeLong(seq);
    out.writeLong(emitted);
    out.writeLong(paceStartMs);
    int pending = buffer == null ? 0 : buffer.size();
    out.writeInt(pending);
    if (pending > 0) {
      if (serializer == null)
        throw new IllegalStateException("source " + name + " has a disorder buffer but no serializer");
      DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
      for (Pending<T> x : buffer) { // heap order is irrelevant: (key, seq) is a total order
        out.writeLong(x.key);
        out.writeLong(x.seq);
        serializer.serialize(x.rec, view);
      }
    }
    out.flush();
    state.clear();
    state.add(bytes.toByteArray());
  }

  private void restore(byte[] b) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(b));
    if (in.readInt() != 1)
      throw new IOException("source " + name + ": unknown checkpoint format");
    nextOffset = in.readLong();
    nextLine = in.readLong();
    seq = in.readLong();
    emitted = in.readLong();
    paceStartMs = in.readLong();
    int pending = in.readInt();
    DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in);
    for (int i = 0; i < pending; i++) {
      long key = in.readLong();
      long sq = in.readLong();
      buffer.add(new Pending<>(key, sq, serializer.deserialize(view)));
    }
  }

  @Override
  public void cancel() {
    running = false;
  }
}
