package caps;

import java.util.function.Consumer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import temporalindex.SampleKeyFn;
import temporalindex.WatermarkProbe;

/**
 * Picks the benchmark sink from args[offset]: mode name, then benchDir, then mode-specific tail:
 * queries [benchDir] [numQueries] [queryLenMs]            two-tier index, scan baseline (IndexingSink)
 * queries_slackbuffer [benchDir] [numQueries] [queryLenMs] [slackMs]
 * released slack-buffer sink, p = 1, verification only
 * querylen [benchDir] [numQueries] [lensMsCsv]  expiry [benchDir] [horizonMs] [queryLenMs]
 * summarize [benchDir] [horizonMs] [widthsMsCsv]   provenance* (per-output queries, see ProvenanceQuerySink)
 * attach() adds the sink to the job: sinks that need watermarks go through WatermarkTriggeringSink,
 * all of them run at the job's parallelism (the index sinks keep one shard per subtask).
 * Timing and verification are seperate submissions - verify must not land in the runtime column.
 * provenance_verify is a separate submission: the sink answers every output
 * and writes per-timestamp {count, sumOfAnswers}. That work must not run
 * during timing - for GeneaLog it is a graph traversal, and the cost would
 * land in the runtime column. sampleProb is accepted so the arg list stays
 * parallel to provenance / provenance_timer; the coin is not flipped.
 * The sink side of every experiment of Paper Section 7 (runtime, per-output queries, temporal queries, expiry, summarize); the
 * index modes are the two-tier index (Section 5.4); Green's counterpart is ink.InkBenchSinks.
 */
public final class BenchSinks {

  private BenchSinks() {}

  /**
   * Adds the sink to the job at the job's parallelism. A sink that implements Consumer&lt;Watermark&gt;
   * (the two-tier index sinks) receives the watermarks of its input.
   */
  public static <T> void attach(DataStream<T> stream, RichSinkFunction<T> sink, String name) {
    int parallelism = stream.getExecutionEnvironment().getParallelism();
    String sinkP = System.getenv("CAPS_SINK_PARALLELISM"); // E3: 1 = one global sink fed by all subtasks
    if (sinkP != null && !sinkP.trim().isEmpty())
      parallelism = Integer.parseInt(sinkP.trim());
    String traceDir = System.getenv().getOrDefault("CAPS_TRACE_DIR", "");
    if ("1".equals(System.getenv("CAPS_TRACE")) && !traceDir.isEmpty() && sink instanceof Consumer)
      stream = stream.transform("WM-PROBE", stream.getType(), new WatermarkProbe<T>("sink", traceDir));
    if (sink instanceof SlackBufferIndexingSink) { // reference sink of the verification: one subtask by design
      parallelism = 1;
    }
    if (sink instanceof Consumer) {
      @SuppressWarnings("unchecked")
      Consumer<Watermark> watermarkConsumer = (Consumer<Watermark>) sink;
      WatermarkTriggeringSink.addSink(stream, name, parallelism, sink, watermarkConsumer);
    } else {
      stream.addSink(sink).setParallelism(parallelism).name(name);
    }
  }

  public static <T> RichSinkFunction<T> create(String[] args, int offset,
      IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn, int channels) {
    return create(args, offset, tsFn, hmFn, null, channels);
  }

  public static <T> RichSinkFunction<T> create(String[] args, int offset,
      IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn, SampleKeyFn<T> keyFn,
      int channels) {
    String mode = args[offset];
    String benchDir = args[offset + 1];
    switch (mode) {
      case "queries":
        return new IndexingSink<>(tsFn, hmFn, channels, benchDir,
            Integer.parseInt(args[offset + 2]), Long.parseLong(args[offset + 3]));
      case "queries_slackbuffer":
        return new SlackBufferIndexingSink<>(tsFn, hmFn, channels, benchDir,
            Integer.parseInt(args[offset + 2]), Long.parseLong(args[offset + 4]),
            Long.parseLong(args[offset + 3]));
      case "querylen": {
        String[] lensCsv = args[offset + 3].split(",");
        long[] lens = new long[lensCsv.length];
        for (int i = 0; i < lensCsv.length; i++) {
          lens[i] = Long.parseLong(lensCsv[i]);
        }
        return new QueryLengthSink<>(tsFn, hmFn, channels, benchDir,
            Integer.parseInt(args[offset + 2]), lens);
      }
      case "expiry":
        return new ExpirySink<>(tsFn, hmFn, channels, benchDir,
            Long.parseLong(args[offset + 2]), Long.parseLong(args[offset + 3]));
      case "summarize":
        String[] parts = args[offset + 3].split(",");
        long[] widths = new long[parts.length];
        for (int i = 0; i < parts.length; i++) {
          widths[i] = Long.parseLong(parts[i]);
        }
        return new SummarizeSink<>(tsFn, hmFn, channels, benchDir,
            Long.parseLong(args[offset + 2]), widths);
      case "provenance":
        return provenanceSink(tsFn, hmFn, keyFn, channels, benchDir,
            Double.parseDouble(args[offset + 2]), false, false, false);
      // Calibration twin of "provenance": identical job, but the timed region
      // encloses an increment instead of the query, so the run reports what the
      // System.nanoTime() bracket costs at the sampling points.
      case "provenance_timer":
        return provenanceSink(tsFn, hmFn, keyFn, channels, benchDir,
            Double.parseDouble(args[offset + 2]), true, false, false);
      case "provenance_verify":
        return provenanceSink(tsFn, hmFn, keyFn, channels, benchDir,
            Double.parseDouble(args[offset + 2]), false, true, true);
      case "provenance_vector":
        return provenanceSink(tsFn, hmFn, keyFn, channels, benchDir,
            Double.parseDouble(args[offset + 2]), false, false, true);
      case "provenance_vector_timer":
        return provenanceSink(tsFn, hmFn, keyFn, channels, benchDir,
            Double.parseDouble(args[offset + 2]), true, false, true);
      case "provenance_ids": {
        SampleKeyFn<T> key = keyFn != null ? keyFn : ProvenanceQuerySink.timestampKey(tsFn);
        return new ProvenanceQuerySink<T>(tsFn, hmFn, key, channels, benchDir,
            args[offset + 2], Integer.parseInt(args[offset + 3]));
      }
      case "provenance_shared": {
        SampleKeyFn<T> key = keyFn != null ? keyFn : ProvenanceQuerySink.timestampKey(tsFn);
        return new ProvenanceQuerySink<T>(tsFn, hmFn, key, channels, benchDir,
            args[offset + 2]);
      }
      default:
        throw new IllegalArgumentException("unknown bench mode: " + mode);
    }
  }

  // Timestamp-only key when the job did not pass a richer SampleKeyFn.
  private static <T> ProvenanceQuerySink<T> provenanceSink(
      IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn, SampleKeyFn<T> keyFn,
      int channels, String benchDir, double sampleProb,
      boolean calibrate, boolean verifyMode, boolean vectorQuery) {
    SampleKeyFn<T> key = keyFn != null ? keyFn : ProvenanceQuerySink.timestampKey(tsFn);
    return new ProvenanceQuerySink<T>(tsFn, hmFn, key, channels, benchDir,
        sampleProb, calibrate, verifyMode, vectorQuery);
  }

}
