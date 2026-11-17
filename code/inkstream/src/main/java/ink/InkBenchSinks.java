package ink;

import java.util.function.Consumer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import temporalindex.SampleKeyFn;

/**
 * Factory for the polynomial-baseline bench sinks, mirroring
 * {@code caps.BenchSinks} but offering only the two modes this
 * experiment needs: {@code provenance} and its calibration twin
 * {@code provenance_timer}. The unused {@code slackMs} argument is
 * accepted so the dataflow call sites stay parallel to CAPS.
 */
// Also provenance_verify: a separate submission that answers every
// output and writes per-timestamp {count, sumOfAnswers}. Must not be
// mixed with timing - the fold cost would otherwise land in the runtime
// column. sampleProb is accepted so the arg list stays parallel; the
// coin is not flipped.
//
// provenance is total_contributions (scalar fold, no vector alloc).
// provenance_vector / provenance_vector_timer materialize long[M].
// provenance_verify writes vector + payload + total digests.
// provenance_shared [benchDir] [manifestPath] captures first-occurrence IDs.
// queries [benchDir] [numQueries] [queryLenMs]   the temporal-query baseline (GreenIndexingSink): outputs go
//   into the shared watermark-driven Tier 1, then into a timestamp-ordered structure holding the polynomials;
//   how-much and versioning queries project the polynomials in range
// querylen [benchDir] [numQueries] [lensMsCsv]   the same over a sweep of query window lengths
// attach() adds the sink to the job: the index sinks need the watermarks (caps.WatermarkTriggeringSink) and run
// at the job's parallelism with one shard per subtask; the per-output sinks stay on one subtask.
// The Green baseline in every experiment that compares methods: per-output queries (Figure 9a), temporal queries (Figures 9b and 10).
public final class InkBenchSinks {

  private InkBenchSinks() {}

  /** Adds the sink to the job: index sinks through WatermarkTriggeringSink at the job's parallelism, the others on one subtask. */
  public static <T> void attach(DataStream<T> stream, RichSinkFunction<T> sink, String name) {
    if (sink instanceof Consumer) {
      @SuppressWarnings("unchecked")
      Consumer<Watermark> watermarkConsumer = (Consumer<Watermark>) sink;
      caps.WatermarkTriggeringSink.addSink(stream, name,
          stream.getExecutionEnvironment().getParallelism(), sink, watermarkConsumer);
    } else {
      stream.addSink(sink).setParallelism(1).name(name);
    }
  }

  public static <T> RichSinkFunction<T> create(String[] args, int offset,
      InkQuerySink.TsFn<T> tsFn, InkQuerySink.MetaFn<T> metaFn,
      int channels, long slackMs) {
    return create(args, offset, tsFn, metaFn, null, channels, slackMs);
  }

  public static <T> RichSinkFunction<T> create(String[] args, int offset,
      InkQuerySink.TsFn<T> tsFn, InkQuerySink.MetaFn<T> metaFn,
      SampleKeyFn<T> keyFn, int channels, long slackMs) {
    String mode = args[offset];
    String benchDir = args[offset + 1];
    SampleKeyFn<T> key = keyFn != null ? keyFn : InkQuerySink.timestampKey(tsFn);
    switch (mode) {
      case "queries":
        return new GreenIndexingSink<T>(tsFn, metaFn, channels, benchDir,
            Integer.parseInt(args[offset + 2]), Long.parseLong(args[offset + 3]));
      case "querylen": {
        String[] lensCsv = args[offset + 3].split(",");
        long[] lens = new long[lensCsv.length];
        for (int i = 0; i < lens.length; i++)
          lens[i] = Long.parseLong(lensCsv[i]);
        return new GreenIndexingSink<T>(tsFn, metaFn, channels, benchDir,
            Integer.parseInt(args[offset + 2]), lens);
      }
      case "provenance":
        return new InkQuerySink<T>(tsFn, metaFn, key, channels, benchDir,
            Double.parseDouble(args[offset + 2]), false, false, false);
      // Calibration twin of "provenance": identical job, but the timed region
      // encloses an increment instead of the query, so the run reports what the
      // System.nanoTime() bracket costs at the sampling points.
      case "provenance_timer":
        return new InkQuerySink<T>(tsFn, metaFn, key, channels, benchDir,
            Double.parseDouble(args[offset + 2]), true, false, false);
      case "provenance_verify":
        return new InkQuerySink<T>(tsFn, metaFn, key, channels, benchDir,
            Double.parseDouble(args[offset + 2]), false, true, true);
      case "provenance_vector":
        return new InkQuerySink<T>(tsFn, metaFn, key, channels, benchDir,
            Double.parseDouble(args[offset + 2]), false, false, true);
      case "provenance_vector_timer":
        return new InkQuerySink<T>(tsFn, metaFn, key, channels, benchDir,
            Double.parseDouble(args[offset + 2]), true, false, true);
      case "provenance_shared":
        // Writes query_fixture_green.ser for the JMH harness; CAPS/GeneaLog use the same manifest.
        return new InkQuerySink<T>(tsFn, metaFn, key, channels, benchDir,
            args[offset + 2]);
      default:
        throw new IllegalArgumentException("unknown ink bench mode: " + mode);
    }
  }
}
