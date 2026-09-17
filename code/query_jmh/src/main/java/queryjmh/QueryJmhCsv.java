package queryjmh;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Locale;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import temporalindex.QueryFixture;

public final class QueryJmhCsv {

  public static final String HEADER =
      "method,score_ns,error_ns,samples,unit,checksum,fixture_count,id_checksum";
  public static final int REQUIRED_SAMPLES = 5;
  public static final String REQUIRED_UNIT = "ns/op";

  private QueryJmhCsv() {}

  public static void write(File out, String method, Collection<RunResult> results,
      long checksum, int fixtureCount, long idChecksum) throws IOException {
    if (results == null || results.size() != 1) {
      throw new IllegalArgumentException("expected exactly one JMH RunResult");
    }
    write(out, method, results.iterator().next(), checksum, fixtureCount, idChecksum);
  }

  public static void write(File out, String method, RunResult result, long checksum,
      int fixtureCount, long idChecksum) throws IOException {
    Result<?> primary = result.getPrimaryResult();
    writeCanonical(out, method, primary.getScore(), primary.getScoreError(),
        primary.getSampleCount(), primary.getScoreUnit(), checksum, fixtureCount, idChecksum);
  }

  public static void writeCanonical(File out, String method, double score, double error,
      long samples, String unit, long checksum, int fixtureCount, long idChecksum)
      throws IOException {
    if (!REQUIRED_UNIT.equals(unit)) {
      throw new IllegalArgumentException("unit must be " + REQUIRED_UNIT + " but was " + unit);
    }
    if (samples != REQUIRED_SAMPLES) {
      throw new IllegalArgumentException("samples must be " + REQUIRED_SAMPLES + " but was " + samples);
    }
    if (fixtureCount != QueryFixture.REQUIRED_COUNT) {
      throw new IllegalArgumentException(
          "fixture_count must be " + QueryFixture.REQUIRED_COUNT + " but was " + fixtureCount);
    }
    // checksum and id_checksum let downstream scripts recieve the same fixture we timed.
    PrintWriter pw = new PrintWriter(new FileWriter(out));
    try {
      pw.println(HEADER);
      pw.printf(Locale.ROOT, "%s,%s,%s,%d,%s,%s,%d,%s%n",
          method, Double.toString(score), Double.toString(error), Long.valueOf(samples), unit,
          Long.toString(checksum), Integer.valueOf(fixtureCount), Long.toString(idChecksum));
    } finally {
      pw.close();
    }
  }
}
