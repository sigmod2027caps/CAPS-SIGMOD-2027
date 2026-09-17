package caps;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import temporalindex.SampleKeyFn;

// Picks the benchmark sink from args[offset]: mode name, then benchDir, then mode-specific tail.
// Timing and verification are seperate submissions — verify must not land in the runtime column.
// provenance_verify is a separate submission: the sink answers every output
// and writes per-timestamp {count, sumOfAnswers}. That work must not run
// during timing — for GeneaLog it is a graph traversal, and the cost would
// land in the runtime column. sampleProb is accepted so the arg list stays
// parallel to provenance / provenance_timer; the coin is not flipped.
public final class BenchSinks {

  private BenchSinks() {}

  public static <T> RichSinkFunction<T> create(String[] args, int offset,
      IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn, int channels, long slackMs) {
    return create(args, offset, tsFn, hmFn, null, channels, slackMs);
  }

  public static <T> RichSinkFunction<T> create(String[] args, int offset,
      IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn, SampleKeyFn<T> keyFn,
      int channels, long slackMs) {
    String mode = args[offset];
    String benchDir = args[offset + 1];
    switch (mode) {
      case "queries":
        return new IndexingSink<>(tsFn, hmFn, channels, benchDir,
            Integer.parseInt(args[offset + 2]), slackMs,
            Long.parseLong(args[offset + 3]));
      case "querylen": {
        String[] lensCsv = args[offset + 3].split(",");
        long[] lens = new long[lensCsv.length];
        for (int i = 0; i < lensCsv.length; i++) {
          lens[i] = Long.parseLong(lensCsv[i]);
        }
        return new QueryLengthSink<>(tsFn, hmFn, channels, benchDir,
            Integer.parseInt(args[offset + 2]), slackMs, lens);
      }
      case "expiry":
        return new ExpirySink<>(tsFn, hmFn, channels, benchDir,
            Long.parseLong(args[offset + 2]), slackMs,
            Long.parseLong(args[offset + 3]));
      case "summarize":
        String[] parts = args[offset + 3].split(",");
        long[] widths = new long[parts.length];
        for (int i = 0; i < parts.length; i++) {
          widths[i] = Long.parseLong(parts[i]);
        }
        return new SummarizeSink<>(tsFn, hmFn, channels, benchDir,
            Long.parseLong(args[offset + 2]), widths, slackMs);
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
