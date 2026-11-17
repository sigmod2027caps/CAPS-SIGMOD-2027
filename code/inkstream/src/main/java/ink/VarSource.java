package ink;

import java.io.Serializable;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * Mints the variables of the source tuples that one parallel instance of an operator tags first (see
 * {@link InkMeta}). The ids must be unique over all instances of the operator: with a counter per instance two
 * subtasks would hand out the same id to different tuples and the polynomial would merge them, so a polynomial would
 * have fewer variables than the tuple has contributors. The counter of subtask {@code s} of {@code p} therefore
 * only visits the global sequence numbers {@code s, s + p, s + 2p, ...}. At {@code p = 1} this is the counter of
 * the unparallelized job. The ids are a function of the position of a tuple in its subtask's input only, so they are
 * the same in every run of the job.
 *
 * <p>Use it from the {@code open} of a rich function: {@code vars.open(getRuntimeContext())}, then
 * {@code vars.next(channel)} for every source tuple.
 *
 * At parallelism above 1, the variable ids of Green stay unique over subtasks.
 */
public final class VarSource implements Serializable {

  private static final long serialVersionUID = 1L;

  private final int channels;
  private long seq;
  private int subtask;
  private int parallelism = 1;

  public VarSource(int channels) {
    this.channels = channels;
  }

  /** Binds the instance to its subtask; the counter restarts. */
  public void open(RuntimeContext context) {
    open(context.getIndexOfThisSubtask(), context.getNumberOfParallelSubtasks());
  }

  public void open(int subtask, int parallelism) {
    this.subtask = subtask;
    this.parallelism = parallelism;
    this.seq = 0;
  }

  /** The annotation of the next source tuple of this subtask, on the given channel. */
  public InkMeta next(int channel) {
    return InkMeta.source(channel, channels, seq++, subtask, parallelism);
  }
}
