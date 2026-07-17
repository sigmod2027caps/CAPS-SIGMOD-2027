package caps;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public final class BenchSinks {

  private BenchSinks() {}

  public static <T> RichSinkFunction<T> create(String[] args, int offset,
      IndexingSink.TsFn<T> tsFn, IndexingSink.HmFn<T> hmFn, int channels, long slackMs) {
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
        return new ProvenanceQuerySink<>(tsFn, hmFn, channels, benchDir,
            Double.parseDouble(args[offset + 2]));
      default:
        throw new IllegalArgumentException("unknown bench mode: " + mode);
    }
  }
}
