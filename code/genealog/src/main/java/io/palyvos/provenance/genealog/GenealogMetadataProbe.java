package io.palyvos.provenance.genealog;

import io.palyvos.provenance.util.ExperimentSettings;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashSet;
import java.util.Map;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Identity map that sums output-record counts and a fixed 32-byte GeneaLog
 * payload (uid, U1, U2, next — no headers). Fails fast if a probed tuple
 * has null {@link GenealogData}, matching CAPS/Green. Enabled only when
 * {@code --metadataVolumeDir} is set; the probe itself is not a logical
 * operator.
 */
public final class GenealogMetadataProbe<T extends GenealogTuple>
    extends RichMapFunction<T, T> {

  private static final long serialVersionUID = 1L;

  public static final String CSV_FILENAME = "genealog_metadata_volume.csv";
  public static final String SINK_RECORDS_FILENAME = "sink_records.txt";
  public static final String TOTAL = "TOTAL";
  public static final String RECORDS_PREFIX = "genealog.meta.records.";
  public static final String BYTES_PREFIX = "genealog.meta.bytes.";
  public static final int PAYLOAD_BYTES = 32;

  private final String operator;

  transient LongCounter records;
  transient LongCounter bytes;

  public GenealogMetadataProbe(String operator) {
    this.operator = operator;
  }

  public static boolean enabled(ExperimentSettings settings) {
    if (settings == null) {
      return false;
    }
    String dir = settings.metadataVolumeDir();
    return dir != null && !dir.isEmpty();
  }

  public static String recordsName(String operator) {
    return RECORDS_PREFIX + operator;
  }

  public static String bytesName(String operator) {
    return BYTES_PREFIX + operator;
  }

  public static String[] expectedOperators(String dataflow) {
    if ("twitter_1".equals(dataflow)) {
      return new String[] {
          "SOURCE", "MAP-ALL", "AGG-ALL", "FILTER-VERIFIED",
          "MAP-VERIFIED", "AGG-VERIFIED", "JOIN"
      };
    }
    if ("twitter_2".equals(dataflow)) {
      return new String[] {"SOURCE", "MAP", "AGG-6H"};
    }
    if ("taxi_1".equals(dataflow)) {
      return new String[] {
          "SOURCE", "FILTER-SOLO", "MAP-SOLO", "AGG-SOLO", "FILTER-COUNT-SOLO",
          "FILTER-CROWDED", "MAP-CROWDED", "AGG-CROWDED", "FILTER-COUNT-CROWDED",
          "JOIN"
      };
    }
    if ("taxi_2".equals(dataflow)) {
      return new String[] {
          "SOURCE", "FILTER-CARD", "MAP-CARD", "AGG-CARD",
          "FILTER-CASH", "MAP-CASH", "AGG-CASH", "JOIN"
      };
    }
    if ("nexmark_1".equals(dataflow)) {
      return new String[] {"SOURCE-PERSONS", "SOURCE-AUCTIONS", "JOIN", "AGG-COUNT"};
    }
    if ("nexmark_2".equals(dataflow)) {
      return new String[] {
          "SOURCE-PERSONS", "SOURCE-AUCTIONS", "FILTER-STATE", "FILTER-CATEGORY",
          "JOIN"
      };
    }
    throw new IllegalArgumentException("unknown dataflow: " + dataflow);
  }

  public static <T extends GenealogTuple> DataStream<T> attach(
      DataStream<T> stream, boolean enabled, String operator) {
    if (!enabled) {
      return stream;
    }
    validateOperator(operator);
    return stream
        .map(new GenealogMetadataProbe<T>(operator))
        .name("PROBE-" + operator);
  }

  public static void writeIfEnabled(JobExecutionResult result,
      ExperimentSettings settings, String dataflow) throws IOException {
    if (!enabled(settings)) {
      return;
    }
    if (result == null) {
      throw new IllegalArgumentException("metadata volume requires JobExecutionResult");
    }
    File csv = new File(settings.metadataVolumeDir(), CSV_FILENAME);
    Map<String, Object> acc = result.getAllAccumulatorResults();
    writeCanonicalCsv(acc, csv, dataflow, expectedOperators(dataflow));
    writeDiscardSinkRecords(acc,
        new File(settings.metadataVolumeDir(), SINK_RECORDS_FILENAME),
        GenealogCardinalityDiscardSink.RECORDS_NAME);
  }

  public static void writeDiscardSinkRecords(Map<String, Object> accumulators,
      File file, String accumulatorName) throws IOException {
    if (accumulators == null || !accumulators.containsKey(accumulatorName)) {
      throw new IllegalArgumentException("missing discard sink records: " + accumulatorName);
    }
    File parent = file.getParentFile();
    if (parent != null) {
      parent.mkdirs();
    }
    PrintWriter pw = new PrintWriter(new FileWriter(file));
    try {
      pw.println(toLong(accumulators.get(accumulatorName)));
    } finally {
      pw.close();
    }
  }

  public static void writeCanonicalCsv(Map<String, Object> accumulators, File csv,
      String dataflow, String[] expected) throws IOException {
    if (dataflow == null || dataflow.isEmpty()) {
      throw new IllegalArgumentException("dataflow required");
    }
    if (expected == null || expected.length == 0) {
      throw new IllegalArgumentException("expected probes required");
    }
    LinkedHashSet<String> wanted = new LinkedHashSet<String>();
    for (int i = 0; i < expected.length; i++) {
      String op = expected[i];
      validateOperator(op);
      if (!wanted.add(op)) {
        throw new IllegalArgumentException("duplicate expected probe: " + op);
      }
    }

    LinkedHashSet<String> found = new LinkedHashSet<String>();
    if (accumulators != null) {
      for (String key : accumulators.keySet()) {
        if (key != null && key.startsWith(RECORDS_PREFIX)) {
          found.add(key.substring(RECORDS_PREFIX.length()));
        } else if (key != null && key.startsWith(BYTES_PREFIX)) {
          found.add(key.substring(BYTES_PREFIX.length()));
        }
      }
    }
    for (String op : found) {
      if (!wanted.contains(op)) {
        throw new IllegalArgumentException("unexpected probe: " + op);
      }
    }
    for (String op : wanted) {
      if (accumulators == null
          || !accumulators.containsKey(recordsName(op))
          || !accumulators.containsKey(bytesName(op))) {
        throw new IllegalArgumentException("missing expected probe: " + op);
      }
    }

    File parent = csv.getParentFile();
    if (parent != null) {
      parent.mkdirs();
    }
    PrintWriter pw = new PrintWriter(new FileWriter(csv));
    try {
      pw.println("dataflow,operator,records,bytes");
      long recTotal = 0L;
      long byteTotal = 0L;
      for (int i = 0; i < expected.length; i++) {
        String op = expected[i];
        long rec = toLong(accumulators.get(recordsName(op)));
        long byt = toLong(accumulators.get(bytesName(op)));
        pw.println(dataflow + "," + op + "," + rec + "," + byt);
        recTotal += rec;
        byteTotal += byt;
      }
      pw.println(dataflow + "," + TOTAL + "," + recTotal + "," + byteTotal);
    } finally {
      pw.close();
    }
  }

  @Override
  public void open(Configuration parameters) {
    records = getRuntimeContext().getLongCounter(recordsName(operator));
    bytes = getRuntimeContext().getLongCounter(bytesName(operator));
  }

  @Override
  public T map(T value) {
    if (value == null || value.getGenealogData() == null) {
      throw new IllegalArgumentException("probe " + operator + " saw null GenealogData");
    }
    records.add(1L);
    bytes.add(PAYLOAD_BYTES);
    return value;
  }

  private static void validateOperator(String operator) {
    if (operator == null || operator.isEmpty() || TOTAL.equals(operator)
        || operator.startsWith("PROBE-")) {
      throw new IllegalArgumentException("invalid probe operator: " + operator);
    }
  }

  private static long toLong(Object value) {
    if (value instanceof Long) {
      return ((Long) value).longValue();
    }
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    throw new IllegalArgumentException("non-numeric accumulator: " + value);
  }
}
