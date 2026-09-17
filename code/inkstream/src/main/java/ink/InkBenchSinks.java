package ink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
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
// mixed with timing — the fold cost would otherwise land in the runtime
// column. sampleProb is accepted so the arg list stays parallel; the
// coin is not flipped.
//
// provenance is total_contributions (scalar fold, no vector alloc).
// provenance_vector / provenance_vector_timer materialize long[M].
// provenance_verify writes vector + payload + total digests.
// provenance_shared [benchDir] [manifestPath] captures first-occurrence IDs.
public final class InkBenchSinks {

  private InkBenchSinks() {}

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
