package caps;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * The verification sink of the recovery experiment (E4): an at-least-once file sink that writes
 * the deterministic identity of every output it receives.
 *
 * output id = (operator, window end, key, ordinal)      one tab-separated row per output:
 * &lt;operator&gt; TAB &lt;window end = the output's timestamp&gt; TAB &lt;key&gt; TAB &lt;ordinal&gt; TAB &lt;64-bit hash of the whole output row&gt;
 *
 * The window end is the derived time of the output (StampDerivedTime), the key is the grouping key of the last
 * windowed operator (plus the auction id for the join of nexmark_2). A window join can emit several outputs for one
 * (window end, key) (taxi_1 pairs every solo count with every crowded count of the taxi), so the ordinal numbers the
 * outputs of one (window end, key) in the order they leave the operator: a window fires all outputs of a key in one
 * uninterrupted burst, in the order of the window's state (arrival order of the joined tuples, which is event-time
 * order per key), and a checkpoint barrier never falls inside a burst. The ordinal is therefore a function of the data
 * and of the query only, and a replay after a failure re-emits the same ids with the same hashes. A consumer deduplicates
 * by the id; the hash shows that the re-emitted output has the same content. The sink itself is idempotent in
 * that sense and not transactional: the file is opened in append mode and rows written after the last completed
 * checkpoint are written again by the replay. A transactional external sink would extend Flink's
 * TwoPhaseCommitSinkFunction (pre-commit at the checkpoint barrier, commit when the checkpoint completes) and would
 * need no deduplication; this experiment does not implement that.
 *
 * Rows are written in blocks of whole rows with one write call each, and the buffer is flushed at every checkpoint,
 * so the file never has a torn row even after kill -9 and everything before a completed checkpoint is in it.
 */
public class OutputLogSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {

  public interface KeyFn<T> extends Serializable {
    String key(T tuple);
  }

  private static final long serialVersionUID = 1L;
  private static final int FLUSH_CHARS = 1 << 15;

  private final String dir;
  private final String operator;
  private final IndexingSink.TsFn<T> tsFn;
  private final KeyFn<T> keyFn;
  private transient FileOutputStream out;
  private transient StringBuilder buf;
  private transient long lastTs;
  private transient String lastKey;
  private transient int ordinal;

  /** dir: where outputs_s&lt;i&gt;.log is written; keyFn: the grouping key that is part of the output id. */
  public OutputLogSink(String dir, String operator, IndexingSink.TsFn<T> tsFn, KeyFn<T> keyFn) {
    this.dir = dir;
    this.operator = operator;
    this.tsFn = tsFn;
    this.keyFn = keyFn;
  }

  /**
   * Adds the log sink next to the index sink if CAPS_OUTPUT_LOG=1. The log is outputs_s&lt;i&gt;.log in the bench
   * directory args[offset + 1] (the argument after the bench mode), one file per subtask.
   */
  public static <T> void attachIfEnabled(DataStream<T> stream, String[] args, int offset, String operator,
                                         IndexingSink.TsFn<T> tsFn, KeyFn<T> keyFn) {
    if (!"1".equals(System.getenv("CAPS_OUTPUT_LOG")))
      return;
    stream.addSink(new OutputLogSink<>(args[offset + 1], operator, tsFn, keyFn))
        .setParallelism(stream.getExecutionEnvironment().getParallelism())
        .name("SINK-LOG");
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    int subtask = getRuntimeContext().getIndexOfThisSubtask();
    out = new FileOutputStream(dir + "/outputs_s" + subtask + ".log", true); // append: the earlier execution's rows stay
    buf = new StringBuilder(2 * FLUSH_CHARS);
  }

  @Override
  public void invoke(T value, Context context) throws IOException {
    long ts = tsFn.ts(value);
    String key = keyFn.key(value);
    if (lastKey != null && ts == lastTs && key.equals(lastKey))
      ordinal++;
    else {
      ordinal = 0;
      lastTs = ts;
      lastKey = key;
    }
    buf.append(operator).append('\t').append(ts).append('\t').append(key).append('\t').append(ordinal).append('\t')
        .append(Long.toHexString(hash(value.toString()))).append('\n');
    if (buf.length() >= FLUSH_CHARS)
      flush();
  }

  private void flush() throws IOException {
    if (buf.length() == 0)
      return;
    out.write(buf.toString().getBytes(StandardCharsets.UTF_8));
    buf.setLength(0);
  }

  /** FNV-1a over the characters of the row text. */
  static long hash(String s) {
    long h = 0xCBF29CE484222325L;
    for (int i = 0; i < s.length(); i++)
      h = (h ^ s.charAt(i)) * 0x100000001B3L;
    return h;
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws IOException {
    flush(); // everything before the barrier is in the file when the checkpoint completes
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {}

  @Override
  public void close() throws IOException {
    if (out == null)
      return;
    flush();
    out.close();
  }
}
