package caps;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

// memory_volume tail sink: count arrivals only — no HowMuch stringification, no query work.
public final class CapsCardinalityDiscardSink<T> extends RichSinkFunction<T> {

  private static final long serialVersionUID = 1L;

  public static final String RECORDS_NAME = "caps.sink.records";

  transient LongCounter records;

  @Override
  public void open(Configuration parameters) {
    records = getRuntimeContext().getLongCounter(RECORDS_NAME);
  }

  @Override
  public void invoke(T value, Context context) {
    // Value is ignored on purpose; touching HowMuch here would dominate the mode.
    records.add(1L);
  }
}
