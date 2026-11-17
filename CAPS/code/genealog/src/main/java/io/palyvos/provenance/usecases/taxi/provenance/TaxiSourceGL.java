package io.palyvos.provenance.usecases.taxi.provenance;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import temporalindex.Disorder;
import temporalindex.DisorderConfig;
import temporalindex.DisorderSource;
import io.palyvos.provenance.util.CountStat;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.ProvenanceActivator;
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

  /**
   * The source of the GeneaLog jobs: the same reader, watermark assigner and watermark interval as the CAPS, Green and
   * no-provenance jobs ({@link Disorder#source}: the parallel, replayable DisorderSource followed by the periodic
   * bounded-out-of-orderness assigner), so that the four methods differ only in what they do with the tuples.
   */
  public static SingleOutputStreamOperator<TaxiRideTupleGL> read(
      StreamExecutionEnvironment env, DisorderConfig cfg, ExperimentSettings settings) {
    if (settings.sourceRepetitions() != 1)
      throw new IllegalArgumentException("DisorderSource reads the input once: --sourceRepetitions must be 1");
    return Disorder.source(env, cfg, "SOURCE", settings.inputFile(), new Parser(settings.genealogActivator()),
        t -> t.getTimestamp(), TaxiRideTupleGL.class);
  }

  /** One row of taxis.txt as a source tuple (an unparseable row is skipped with a warning, as the released source did). */
  public static class Parser extends DisorderSource.Parser<TaxiRideTupleGL> {
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
    public TaxiRideTupleGL parse(String line, long lineNo) {
      try {
        return TaxiRideTupleGL.fromReading(line);
      } catch (RuntimeException e) {
        LOG.warn("Skipping unparseable line: {}", line, e);
        return null;
      }
    }
  }
}
