package caps;

import java.io.Serializable;
import java.util.function.Consumer;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * A sink operator that hands every watermark it receives to the sink function: Flink 1.10's
 * SinkFunction has no watermark callback, and DataStream.addSink would swallow the watermarks. The
 * two-tier index needs them to know which timestamps are complete. The watermark that reaches a sink
 * subtask is already the minimum over its input channels. At the end of the input Flink delivers
 * Long.MAX_VALUE, which flushes the index.
 * Same idea as io.palyvos.provenance.genealog.WatermarkTriggeringSink of the Ananke framework (same
 * Flink version); this copy only inlines the argument checks so that caps needs no commons-lang3.
 * The watermark it forwards is what certifies a Tier-1 prefix and moves it into the append-only Tier 2 (Paper Section 5.4, TwoTierIndex).
 */
public class WatermarkTriggeringSink<IN> extends StreamSink<IN> {

  private final Consumer<Watermark> watermarkConsumer;

  /** Wraps the sink function; watermarkConsumer (serializable) receives every watermark that reaches this subtask. */
  public WatermarkTriggeringSink(SinkFunction<IN> sinkFunction,
      Consumer<Watermark> watermarkConsumer) {
    super(sinkFunction);
    if (watermarkConsumer == null) {
      throw new IllegalArgumentException("watermarkConsumer");
    }
    if (!(watermarkConsumer instanceof Serializable)) {
      throw new IllegalArgumentException("watermarkConsumer must be serializable");
    }
    this.watermarkConsumer = watermarkConsumer;
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    super.processWatermark(mark);
    watermarkConsumer.accept(mark);
  }

  /** Like DataStream.addSink(sinkFunction).setParallelism(parallelism).name(name), plus the watermark hook. */
  public static <T> void addSink(DataStream<T> stream, String name, int parallelism,
      SinkFunction<T> sinkFunction, Consumer<Watermark> watermarkConsumer) {
    if (sinkFunction instanceof InputTypeConfigurable) {
      ((InputTypeConfigurable) sinkFunction)
          .setInputType(stream.getType(), stream.getExecutionConfig());
    }
    StreamSink<T> sinkOperator = new WatermarkTriggeringSink<>(sinkFunction, watermarkConsumer);
    SinkTransformation<T> transformation = new SinkTransformation<>(stream.getTransformation(),
        name, sinkOperator, parallelism);
    stream.getExecutionEnvironment().addOperator(transformation);
  }
}
