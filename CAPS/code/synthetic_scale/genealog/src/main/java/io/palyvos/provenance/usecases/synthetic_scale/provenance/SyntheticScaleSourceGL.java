package io.palyvos.provenance.usecases.synthetic_scale.provenance;

import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.util.ExperimentSettings;
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/** Reads synthetic_scale.txt: id,timestamp,sourceId,key,value. */
public class SyntheticScaleSourceGL extends RichSourceFunction<SyntheticScaleTupleGL> {

  private final ExperimentSettings settings;
  private volatile boolean enabled = true;

  public SyntheticScaleSourceGL(ExperimentSettings settings) {
    this.settings = settings;
  }

  @Override
  public void run(SourceContext<SyntheticScaleTupleGL> ctx) throws Exception {
    try (BufferedReader br = new BufferedReader(new FileReader(settings.inputFile()))) {
      String line;
      while (enabled && (line = br.readLine()) != null) {
        if (line.isEmpty()) continue;
        String[] parts = line.split(",", 5);
        SyntheticScaleTupleGL t = new SyntheticScaleTupleGL(
            Long.parseLong(parts[0]),
            Long.parseLong(parts[1]),
            Integer.parseInt(parts[2]),
            Integer.parseInt(parts[3]),
            Long.parseLong(parts[4]));
        t.initGenealog(GenealogTupleType.SOURCE);
        ctx.collect(t);
      }
    }
  }

  @Override
  public void cancel() {
    enabled = false;
  }
}
