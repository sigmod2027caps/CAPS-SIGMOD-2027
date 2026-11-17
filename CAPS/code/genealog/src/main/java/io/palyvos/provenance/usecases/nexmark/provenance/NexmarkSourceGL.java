package io.palyvos.provenance.usecases.nexmark.provenance;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import temporalindex.Disorder;
import temporalindex.DisorderConfig;
import temporalindex.DisorderSource;
import io.palyvos.provenance.util.CountStat;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.ProvenanceActivator;
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

  /**
   * The persons or the auctions source of the GeneaLog jobs: the same reader and watermark assigner as the other three
   * methods (see {@link io.palyvos.provenance.usecases.taxi.provenance.TaxiSourceGL#read}).
   */
  public static SingleOutputStreamOperator<NexmarkEventTupleGL> read(
      StreamExecutionEnvironment env, DisorderConfig cfg, ExperimentSettings settings, boolean persons) {
    if (settings.sourceRepetitions() != 1)
      throw new IllegalArgumentException("DisorderSource reads the input once: --sourceRepetitions must be 1");
    File dir = new File(settings.inputFile()).getParentFile();
    return Disorder.source(env, cfg, persons ? "SOURCE-PERSONS" : "SOURCE-AUCTIONS",
        new File(dir, persons ? "persons.txt" : "auctions.txt").getPath(), new Parser(persons, settings.genealogActivator()),
        t -> t.getTimestamp(), NexmarkEventTupleGL.class);
  }

  /** One row of persons.txt or auctions.txt as a source tuple (an unparseable row is skipped with a warning). */
  public static class Parser extends DisorderSource.Parser<NexmarkEventTupleGL> {
    private final boolean persons;
    // Class-initialization order, not data: the activator's enum class (and its constant bodies, whose lambdas
    // follow this source in its chain) must be initialized by the thread that deserializes this source, the head of the
    // task. Without the field the first task thread to need it is whichever deserializes a lambda of the constant
    // body first; two tasks doing so at once deadlock in the JVM (the enum's static initializer creates the body class,
    // whose initialization needs the enum: seen once in twitter_2). The released sources held ExperimentSettings.
    private final ProvenanceActivator activator;

    public Parser(boolean persons, ProvenanceActivator activator) {
      this.persons = persons;
      this.activator = activator;
    }

    @Override
    public NexmarkEventTupleGL parse(String line, long lineNo) {
      try {
        return persons ? NexmarkEventTupleGL.fromPersonReading(line) : NexmarkEventTupleGL.fromAuctionReading(line);
      } catch (RuntimeException e) {
        LOG.warn("Skipping unparseable line: {}", line, e);
        return null;
      }
    }
  }
}
