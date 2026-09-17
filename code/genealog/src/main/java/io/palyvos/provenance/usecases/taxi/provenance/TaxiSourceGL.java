package io.palyvos.provenance.usecases.taxi.provenance;

import io.palyvos.provenance.util.CountStat;
import io.palyvos.provenance.util.ExperimentSettings;
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads taxis.txt (taxi_id,ts_ms,passengers,zone,payment,fare,tip) and emits
 * one {@link TaxiRideTupleGL} per row.
 */
public class TaxiSourceGL extends RichSourceFunction<TaxiRideTupleGL> {

  private static final Logger LOG = LoggerFactory.getLogger(TaxiSourceGL.class);

  private final ExperimentSettings settings;
  private volatile boolean enabled;
  private transient CountStat throughputStatistic;

  public TaxiSourceGL(ExperimentSettings settings) {
    this.settings = settings;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.throughputStatistic =
        new CountStat(
            settings.throughputFile("SOURCE", getRuntimeContext().getIndexOfThisSubtask()),
            settings.autoFlush());
    enabled = true;
  }

  @Override
  public void run(SourceContext<TaxiRideTupleGL> ctx) throws Exception {
    String inputFile = settings.inputFile();
    LOG.info("TaxiSourceGL reading from {}", inputFile);

    for (int rep = 0; rep < settings.sourceRepetitions() && enabled; rep++) {
      try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
        String line = br.readLine();
        while (enabled && line != null) {
          String trimmed = line.trim();
          if (!trimmed.isEmpty()) {
            try {
              TaxiRideTupleGL tuple = TaxiRideTupleGL.fromReading(trimmed);
              throughputStatistic.increase(1);
              ctx.collect(tuple);
            } catch (Exception e) {
              LOG.warn("Skipping unparseable line: {}", trimmed, e);
            }
          }
          line = br.readLine();
        }
      }
    }
    LOG.info("TaxiSourceGL finished");
    throughputStatistic.close();
  }

  @Override
  public void cancel() {
    enabled = false;
  }
}
