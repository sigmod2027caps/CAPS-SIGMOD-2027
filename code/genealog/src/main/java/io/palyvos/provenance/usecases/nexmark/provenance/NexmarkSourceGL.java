package io.palyvos.provenance.usecases.nexmark.provenance;

import io.palyvos.provenance.util.CountStat;
import io.palyvos.provenance.util.ExperimentSettings;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads persons.txt or auctions.txt from the directory of
 * {@code settings.inputFile()} and emits one {@link NexmarkEventTupleGL}
 * per row.
 */
public class NexmarkSourceGL extends RichSourceFunction<NexmarkEventTupleGL> {

  private static final Logger LOG = LoggerFactory.getLogger(NexmarkSourceGL.class);

  private final ExperimentSettings settings;
  private final boolean persons;
  private volatile boolean enabled;
  private transient CountStat throughputStatistic;

  public NexmarkSourceGL(ExperimentSettings settings, boolean persons) {
    this.settings = settings;
    this.persons = persons;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    String name = persons ? "SOURCE-PERSONS" : "SOURCE-AUCTIONS";
    this.throughputStatistic =
        new CountStat(
            settings.throughputFile(name, getRuntimeContext().getIndexOfThisSubtask()),
            settings.autoFlush());
    enabled = true;
  }

  @Override
  public void run(SourceContext<NexmarkEventTupleGL> ctx) throws Exception {
    File dir = new File(settings.inputFile()).getParentFile();
    File inputFile = new File(dir, persons ? "persons.txt" : "auctions.txt");
    LOG.info("NexmarkSourceGL reading from {}", inputFile);

    for (int rep = 0; rep < settings.sourceRepetitions() && enabled; rep++) {
      try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
        String line = br.readLine();
        while (enabled && line != null) {
          String trimmed = line.trim();
          if (!trimmed.isEmpty()) {
            try {
              NexmarkEventTupleGL tuple = persons
                  ? NexmarkEventTupleGL.fromPersonReading(trimmed)
                  : NexmarkEventTupleGL.fromAuctionReading(trimmed);
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
    LOG.info("NexmarkSourceGL finished");
    throughputStatistic.close();
  }

  @Override
  public void cancel() {
    enabled = false;
  }
}
