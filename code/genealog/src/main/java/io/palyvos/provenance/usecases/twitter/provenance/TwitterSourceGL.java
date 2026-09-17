package io.palyvos.provenance.usecases.twitter.provenance;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.CountStat;
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the tweets CSV (with header), skips the header line,
 * and emits one {@link TwitterTweetTupleGL} per row.
 */
public class TwitterSourceGL extends RichSourceFunction<TwitterTweetTupleGL> {

  private static final Logger LOG = LoggerFactory.getLogger(TwitterSourceGL.class);

  private final ExperimentSettings settings;
  private volatile boolean enabled;
  private transient CountStat throughputStatistic;

  public TwitterSourceGL(ExperimentSettings settings) {
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
  public void run(SourceContext<TwitterTweetTupleGL> ctx) throws Exception {
    String inputFile = settings.inputFile();
    LOG.info("TwitterSourceGL reading from {}", inputFile);

    for (int rep = 0; rep < settings.sourceRepetitions() && enabled; rep++) {
      try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
        String line = br.readLine();
        while (enabled && line != null) {
          String trimmed = line.trim();
          if (!trimmed.isEmpty()) {
            try {
              TwitterTweetTupleGL tuple = TwitterTweetTupleGL.fromReading(trimmed);
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
    LOG.info("TwitterSourceGL finished");
    throughputStatistic.close();
  }

  @Override
  public void cancel() {
    enabled = false;
  }
}
