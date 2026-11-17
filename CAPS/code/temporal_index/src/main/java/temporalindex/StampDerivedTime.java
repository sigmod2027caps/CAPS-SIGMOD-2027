package temporalindex;

import java.io.Serializable;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Derived time of an output: the timestamp the engine gave the record, written back into the tuple.
 * Flink stamps the result of a window with window.maxTimestamp(), the right boundary of the window
 * instance, so placing this operator right after a window aggregate or window join makes the tuple's
 * timestamp field equal that boundary (not the firing time, not the newest contributor).
 * Map, filter, flatmap and union keep the record timestamp, so downstream they stay in agreement.
 * Time semantics: every method runs this operator after every window operator; the two-tier index then sees outputs in non-decreasing derived time per subtask.
 */
public class StampDerivedTime<T> extends ProcessFunction<T, T> {

  /** Writes the derived time into the tuple's timestamp field. */
  public interface TsSetter<T> extends Serializable {
    void set(T tuple, long ts);
  }

  private static final long serialVersionUID = 1L;

  private final TsSetter<T> setter;

  public StampDerivedTime(TsSetter<T> setter) {
    this.setter = setter;
  }

  @Override
  public void processElement(T value, Context ctx, Collector<T> out) {
    Long ts = ctx.timestamp();
    if (ts == null) {
      throw new IllegalStateException("record without an event timestamp: " + value);
    }
    setter.set(value, ts);
    out.collect(value);
  }
}
