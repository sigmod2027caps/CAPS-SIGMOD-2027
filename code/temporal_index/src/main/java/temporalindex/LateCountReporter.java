package temporalindex;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;

// Metric reporter of the in-process runtime (set up by Disorder.environment when CAPS_METRICS_FILE is set): keeps
// the final value of the per-operator counters that the late-data analysis needs and writes them to the file
// given by `metrics.reporter.caps.file` when the mini cluster shuts down. Flink's window operator counts the
// late records it drops in numLateRecordsDropped (the only record of late records at a window join in 1.10).
public class LateCountReporter implements MetricReporter {

  private static final String[] WANTED = {"numRecordsIn", "numRecordsOut", "numLateRecordsDropped", "lateSideOutput",
      "recordsMissedWindow"};

  private String file;
  private final Map<Metric, String[]> live = new ConcurrentHashMap<>();
  private final Map<String, Long> done = new ConcurrentHashMap<>(); // "operator,subtask,metric" -> value

  @Override
  public void open(MetricConfig config) {
    file = config.getString("file", "");
  }

  @Override
  public void close() {
    for (Map.Entry<Metric, String[]> e : live.entrySet())
      done.put(key(e.getValue()), ((Counter) e.getKey()).getCount());
    if (file.isEmpty())
      return;
    try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
      pw.println("operator,subtask,metric,value");
      done.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> pw.println(e.getKey() + "," + e.getValue()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void notifyOfAddedMetric(Metric metric, String name, MetricGroup group) {
    if (!(metric instanceof Counter))
      return;
    for (String w : WANTED)
      if (w.equals(name)) {
        Map<String, String> vars = group.getAllVariables();
        String op = vars.get("<operator_name>");
        if (op == null)
          return;
        live.put(metric, new String[] {op.replace(',', ';'), vars.getOrDefault("<subtask_index>", "0"), name});
        return;
      }
  }

  @Override
  public void notifyOfRemovedMetric(Metric metric, String name, MetricGroup group) {
    String[] k = live.remove(metric);
    if (k != null)
      done.put(key(k), ((Counter) metric).getCount());
  }

  private static String key(String[] k) {
    return k[0] + "," + k[1] + "," + k[2];
  }
}
