package io.palyvos.provenance.usecases.twitter.provenance;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import temporalindex.Disorder;
import temporalindex.DisorderConfig;
import temporalindex.DisorderSource;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.ProvenanceActivator;
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

  /** The source of the GeneaLog jobs: the same reader and watermark assigner as the other three methods (see {@link TaxiSourceGL#read}). */
  public static SingleOutputStreamOperator<TwitterTweetTupleGL> read(
      StreamExecutionEnvironment env, DisorderConfig cfg, ExperimentSettings settings) {
    if (settings.sourceRepetitions() != 1)
      throw new IllegalArgumentException("DisorderSource reads the input once: --sourceRepetitions must be 1");
    return Disorder.source(env, cfg, "SOURCE", settings.inputFile(), new Parser(settings.genealogActivator()),
        t -> t.getTimestamp(), TwitterTweetTupleGL.class);
  }

  /** One row of the tweets file as a source tuple (an unparseable row is skipped with a warning). */
  public static class Parser extends DisorderSource.Parser<TwitterTweetTupleGL> {
    // Class-initialization order, not data: the activator's enum class (and its constant bodies, whose lambdas
    // follow this source in its chain) must be initialized by the thread that deserializes this source, the head of the
    // task. Without the field the first task thread to need it is whichever deserializes a lambda of the constant
    // body first; two tasks doing so at once deadlock in the JVM (the enum's static initializer creates the body class,
    // whose initialization needs the enum: seen once in twitter_2). The released sources held ExperimentSettings.
    private final ProvenanceActivator activator;

    public Parser(ProvenanceActivator activator) {
      this.activator = activator;
    }

    @Override
    public TwitterTweetTupleGL parse(String line, long lineNo) {
      try {
        return TwitterTweetTupleGL.fromReading(line);
      } catch (RuntimeException e) {
        LOG.warn("Skipping unparseable line: {}", line, e);
        return null;
      }
    }
  }
}
