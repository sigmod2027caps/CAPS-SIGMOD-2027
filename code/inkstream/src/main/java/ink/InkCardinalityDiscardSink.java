package ink;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * No-I/O cardinality sink for dedicated {@code memory_volume} mode.
 * Counts sink arrivals without stringifying Green polynomials or running
 * query timing. The final logical operator is probed once before this sink.
 */
public final class InkCardinalityDiscardSink<T> extends RichSinkFunction<T> {

  private static final long serialVersionUID = 1L;

  public static final String RECORDS_NAME = "ink.sink.records";

  transient LongCounter records;

  @Override
  public void open(Configuration parameters) {
    records = getRuntimeContext().getLongCounter(RECORDS_NAME);
  }

  // Counts sink arrivals only — no polynomial toString, no query timing.
  @Override
  public void invoke(T value, Context context) {
    records.add(1L);
  }
}
