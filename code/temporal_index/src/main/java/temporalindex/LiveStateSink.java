package temporalindex;

import java.util.Map;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.accumulators.LongMaximum;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

// Tail sink of the live_memory mode: counts the arrivals and, when the last sink subtask closes, publishes the
// numbers of LiveState as Flink accumulators (maximum-merged), which the client reads from the JobExecutionResult.
// By then every window operator that feeds a sink subtask has finished (the sink closes after the end of its input),
// and the last sink subtask to close is behind all of them. Subclasses observe the outputs (GeneaLog counts the
// graph nodes reachable from them) and add numbers with extras().
// Peak live provenance state (experiments/live_memory); see LiveState.
public class LiveStateSink<T> extends RichSinkFunction<T> {

  private static final long serialVersionUID = 1L;
  public static final String RECORDS_NAME = "live.sink_records";
  public static final String BYTES_NAME = "live.sink_bytes";

  /** Annotation bytes of one output (null: not counted). The sum is what the sink retains (CAPS: N*M*8). */
  final LiveState.Sizer<? super T> annotation;
  transient LongCounter records;
  transient LongCounter bytes;

  public LiveStateSink() {
    this(null);
  }

  public LiveStateSink(LiveState.Sizer<? super T> annotation) {
    this.annotation = annotation;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    records = getRuntimeContext().getLongCounter(RECORDS_NAME);
    bytes = getRuntimeContext().getLongCounter(BYTES_NAME);
  }

  @Override
  public void invoke(T value, Context context) throws Exception {
    records.add(1L);
    if (annotation != null)
      bytes.add(annotation.bytes(value));
    observe(value);
  }

  /** Hook: one output of the dataflow. */
  protected void observe(T value) throws Exception {}

  /** Hook: numbers to publish next to the registry's (name -> value), called once, by the last sink subtask. */
  protected void extras(Map<String, Long> out) throws Exception {}

  @Override
  public void close() throws Exception {
    if (!LiveState.sinkClosed(getRuntimeContext().getNumberOfParallelSubtasks()))
      return;
    Map<String, Long> out = LiveState.report();
    extras(out);
    for (Map.Entry<String, Long> e : out.entrySet()) {
      LongMaximum acc = new LongMaximum();
      acc.add(e.getValue());
      getRuntimeContext().addAccumulator(LiveState.ACC_PREFIX + e.getKey(), acc);
    }
  }
}
