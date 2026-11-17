package temporalindex;

import java.io.FileWriter;
import java.io.PrintWriter;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

// Consumer of the late-record side output of one window operator: counts the records (a metric, and a file
// late_op_<op>_s<subtask>.csv in the trace directory if there is one, written also when the count is 0).
// Used by experiments/disorder: counts the late-record side output of a window operator, only with CAPS_COUNT_LATE=1.
public class LateCounter<T> extends RichSinkFunction<T> {

  private static final long serialVersionUID = 1L;
  private final String op;
  private final String dir;
  private transient Counter counter;
  private transient long n;
  private transient int subtask;

  public LateCounter(String op, String dir) {
    this.op = op;
    this.dir = dir;
  }

  @Override
  public void open(Configuration parameters) {
    counter = getRuntimeContext().getMetricGroup().counter("lateSideOutput");
    subtask = getRuntimeContext().getIndexOfThisSubtask();
  }

  @Override
  public void invoke(T value, Context context) {
    n++;
    counter.inc();
  }

  @Override
  public void close() throws Exception {
    if (dir.isEmpty())
      return;
    try (PrintWriter pw = new PrintWriter(new FileWriter(dir + "/late_op_" + op + "_s" + subtask + ".csv"))) {
      pw.println("operator,subtask,late");
      pw.println(op + "," + subtask + "," + n);
    }
  }
}
