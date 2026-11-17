package temporalindex;

import java.io.IOException;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

// Pass-through operator that logs every watermark it forwards (processing time in microseconds, watermark) to
// wm_probe_<name>_s<subtask>.csv. Put in front of a shuffle to a global sink it records the watermark of every
// upstream subtask, from which the inter-subtask watermark spread and the frontier (the minimum) are computed.
// Used by experiments/disorder: the measurement of the inter-subtask watermark spread that bounds Tier-1 occupancy.
public class WatermarkProbe<T> extends AbstractStreamOperator<T> implements OneInputStreamOperator<T, T> {

  private static final long serialVersionUID = 1L;
  private final String name;
  private final String dir;
  private transient LongTrace trace;

  public WatermarkProbe(String name, String dir) {
    this.name = name;
    this.dir = dir;
    this.chainingStrategy = ChainingStrategy.ALWAYS;
  }

  @Override
  public void open() throws Exception {
    super.open();
    trace = new LongTrace(2, 1 << 20);
  }

  @Override
  public void processElement(StreamRecord<T> element) {
    output.collect(element);
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    if (mark.getTimestamp() != Long.MAX_VALUE)
      trace.add(Disorder.nowUs(), mark.getTimestamp());
    super.processWatermark(mark);
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (trace != null && !dir.isEmpty())
      try {
        trace.write(dir + "/wm_probe_" + name + "_s" + getRuntimeContext().getIndexOfThisSubtask() + ".csv",
            "proc_us,watermark");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
  }
}
