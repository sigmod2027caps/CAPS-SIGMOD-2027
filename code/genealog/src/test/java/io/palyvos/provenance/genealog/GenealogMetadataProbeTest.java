package io.palyvos.provenance.genealog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.palyvos.provenance.util.ExperimentSettings;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GenealogMetadataProbeTest {

  private static final String[] DATAFLOWS = {
      "twitter_1", "twitter_2", "taxi_1", "taxi_2", "nexmark_1", "nexmark_2"
  };

  private static final String[][] JOB_SOURCES = {
      {"twitter_1", "io/palyvos/provenance/usecases/twitter/provenance/queries/TwitterVerifiedJoin.java"},
      {"twitter_2", "io/palyvos/provenance/usecases/twitter/provenance/queries/TwitterLocationAvg.java"},
      {"taxi_1", "io/palyvos/provenance/usecases/taxi/provenance/queries/TaxiPassengerJoin.java"},
      {"taxi_2", "io/palyvos/provenance/usecases/taxi/provenance/queries/TaxiPaymentAvg.java"},
      {"nexmark_1", "io/palyvos/provenance/usecases/nexmark/provenance/queries/NexmarkMonitorNewUsers.java"},
      {"nexmark_2", "io/palyvos/provenance/usecases/nexmark/provenance/queries/NexmarkLocalItemSuggestion.java"},
  };

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void countsExactRecordsAndFixedThirtyTwoPayloadBytes() throws Exception {
    GenealogMetadataProbe<Rec> probe = new GenealogMetadataProbe<Rec>("MAP");
    LongCounter records = new LongCounter();
    LongCounter bytes = new LongCounter();
    probe.records = records;
    probe.bytes = bytes;

    Rec a = Rec.annotated();
    Rec b = Rec.annotated();
    assertSame(a, probe.map(a));
    assertSame(b, probe.map(b));

    assertEquals(2L, records.getLocalValue().longValue());
    assertEquals(64L, bytes.getLocalValue().longValue());
  }

  @Test
  public void failsFastOnNullGenealogData() throws Exception {
    GenealogMetadataProbe<Rec> probe = new GenealogMetadataProbe<Rec>("MAP");
    LongCounter records = new LongCounter();
    LongCounter bytes = new LongCounter();
    probe.records = records;
    probe.bytes = bytes;

    try {
      probe.map(Rec.bare());
      fail("null GenealogData must fail fast");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("null GenealogData"));
    }
    assertEquals(0L, records.getLocalValue().longValue());
    assertEquals(0L, bytes.getLocalValue().longValue());
  }

  @Test
  public void writesCanonicalPerOperatorAndTotalCsv() throws Exception {
    Map<String, Object> acc = new LinkedHashMap<String, Object>();
    acc.put(GenealogMetadataProbe.recordsName("SOURCE"), Long.valueOf(10L));
    acc.put(GenealogMetadataProbe.bytesName("SOURCE"), Long.valueOf(320L));
    acc.put(GenealogMetadataProbe.recordsName("AGG-6H"), Long.valueOf(2L));
    acc.put(GenealogMetadataProbe.bytesName("AGG-6H"), Long.valueOf(64L));

    File csv = new File(tmp.newFolder("bench"), GenealogMetadataProbe.CSV_FILENAME);
    GenealogMetadataProbe.writeCanonicalCsv(acc, csv, "twitter_2",
        new String[] {"SOURCE", "AGG-6H"});

    assertEquals(
        "dataflow,operator,records,bytes\n"
            + "twitter_2,SOURCE,10,320\n"
            + "twitter_2,AGG-6H,2,64\n"
            + "twitter_2,TOTAL,12,384\n",
        readFile(csv));
  }

  @Test
  public void rejectsDuplicateAndMissingExpectedProbes() throws Exception {
    File csv = new File(tmp.getRoot(), "dup.csv");
    Map<String, Object> acc = new HashMap<String, Object>();
    acc.put(GenealogMetadataProbe.recordsName("SOURCE"), Long.valueOf(1L));
    acc.put(GenealogMetadataProbe.bytesName("SOURCE"), Long.valueOf(32L));

    try {
      GenealogMetadataProbe.writeCanonicalCsv(acc, csv, "twitter_2",
          new String[] {"SOURCE", "SOURCE"});
      fail("duplicate expected probe");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("duplicate"));
    }

    try {
      GenealogMetadataProbe.writeCanonicalCsv(acc, csv, "twitter_2",
          new String[] {"SOURCE", "MAP"});
      fail("missing expected probe");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("missing"));
    }

    acc.put(GenealogMetadataProbe.recordsName("EXTRA"), Long.valueOf(1L));
    acc.put(GenealogMetadataProbe.bytesName("EXTRA"), Long.valueOf(32L));
    try {
      GenealogMetadataProbe.writeCanonicalCsv(acc, csv, "twitter_2",
          new String[] {"SOURCE"});
      fail("unexpected extra probe");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("unexpected"));
    }
  }

  @Test
  public void metadataVolumeDirEnablesWriteAndIsNoopOtherwise() throws Exception {
    File bench = tmp.newFolder("vol");
    ExperimentSettings enabled = ExperimentSettings.newInstance(new String[] {
        "--statisticsFolder", tmp.getRoot().getAbsolutePath(),
        "--outputFile", "sink",
        "--sourcesNumber", "1",
        "--metadataVolumeDir", bench.getAbsolutePath()
    });
    assertTrue(GenealogMetadataProbe.enabled(enabled));

    ExperimentSettings disabled = ExperimentSettings.newInstance(new String[] {
        "--statisticsFolder", tmp.getRoot().getAbsolutePath(),
        "--outputFile", "sink",
        "--sourcesNumber", "1"
    });
    assertFalse(GenealogMetadataProbe.enabled(disabled));
    GenealogMetadataProbe.writeIfEnabled(null, disabled, "twitter_2");
    assertFalse(new File(bench, GenealogMetadataProbe.CSV_FILENAME).exists());
  }

  @Test
  public void writeIfEnabledPersistsDiscardSinkRecords() throws Exception {
    Map<String, Object> acc = twitter2Accumulators();
    acc.put(GenealogCardinalityDiscardSink.RECORDS_NAME, Long.valueOf(3L));
    File bench = tmp.newFolder("sink-write");
    ExperimentSettings settings = ExperimentSettings.newInstance(new String[] {
        "--statisticsFolder", tmp.getRoot().getAbsolutePath(),
        "--outputFile", "sink",
        "--sourcesNumber", "1",
        "--metadataVolumeDir", bench.getAbsolutePath()
    });
    GenealogMetadataProbe.writeIfEnabled(
        new JobExecutionResult(new JobID(), 1L, asResultAcc(acc)), settings, "twitter_2");
    assertEquals("3\n", readFile(new File(bench, "sink_records.txt")));
  }

  @Test
  public void writeIfEnabledRejectsMissingDiscardSinkRecords() throws Exception {
    File bench = tmp.newFolder("sink-missing");
    ExperimentSettings settings = ExperimentSettings.newInstance(new String[] {
        "--statisticsFolder", tmp.getRoot().getAbsolutePath(),
        "--outputFile", "sink",
        "--sourcesNumber", "1",
        "--metadataVolumeDir", bench.getAbsolutePath()
    });
    try {
      GenealogMetadataProbe.writeIfEnabled(
          new JobExecutionResult(new JobID(), 1L, asResultAcc(twitter2Accumulators())),
          settings, "twitter_2");
      fail("missing discard sink");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().toLowerCase().contains("sink"));
    }
  }

  @Test
  public void attachDisabledIsIdentityAndDoesNotAddLogicalOutput() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Rec> stream = env.fromElements(Rec.annotated());
    assertSame(stream, GenealogMetadataProbe.attach(stream, false, "SOURCE"));
  }

  @Test
  public void localExecutionMergesAccumulatorsAndWritesCanonicalCsv() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    DataStream<Rec> stream = env.fromElements(Rec.annotated(), Rec.annotated());
    stream = GenealogMetadataProbe.attach(stream, true, "SOURCE");
    JobExecutionResult result = env.execute("genealog-metadata-probe-merge");
    assertEquals(2L, ((Number) result.getAccumulatorResult(
        GenealogMetadataProbe.recordsName("SOURCE"))).longValue());
    assertEquals(64L, ((Number) result.getAccumulatorResult(
        GenealogMetadataProbe.bytesName("SOURCE"))).longValue());

    File bench = tmp.newFolder("local-exec");
    File csv = new File(bench, GenealogMetadataProbe.CSV_FILENAME);
    GenealogMetadataProbe.writeCanonicalCsv(result.getAllAccumulatorResults(), csv,
        "twitter_2", new String[] {"SOURCE"});
    assertEquals(
        "dataflow,operator,records,bytes\n"
            + "twitter_2,SOURCE,2,64\n"
            + "twitter_2,TOTAL,2,64\n",
        readFile(csv));
  }

  @Test
  public void expectedOperatorsAreGenealogSpecificNotCopiedFromCaps() {
    assertEquals(Arrays.asList(
            "SOURCE", "MAP-ALL", "AGG-ALL", "FILTER-VERIFIED",
            "MAP-VERIFIED", "AGG-VERIFIED", "JOIN"),
        Arrays.asList(GenealogMetadataProbe.expectedOperators("twitter_1")));
    assertEquals(Arrays.asList("SOURCE", "MAP", "AGG-6H"),
        Arrays.asList(GenealogMetadataProbe.expectedOperators("twitter_2")));
    assertEquals(Arrays.asList(
            "SOURCE", "FILTER-SOLO", "MAP-SOLO", "AGG-SOLO", "FILTER-COUNT-SOLO",
            "FILTER-CROWDED", "MAP-CROWDED", "AGG-CROWDED", "FILTER-COUNT-CROWDED", "JOIN"),
        Arrays.asList(GenealogMetadataProbe.expectedOperators("taxi_1")));
    assertEquals(Arrays.asList(
            "SOURCE", "FILTER-CARD", "MAP-CARD", "AGG-CARD",
            "FILTER-CASH", "MAP-CASH", "AGG-CASH", "JOIN"),
        Arrays.asList(GenealogMetadataProbe.expectedOperators("taxi_2")));
    assertEquals(Arrays.asList("SOURCE-PERSONS", "SOURCE-AUCTIONS", "JOIN", "AGG-COUNT"),
        Arrays.asList(GenealogMetadataProbe.expectedOperators("nexmark_1")));
    assertEquals(Arrays.asList(
            "SOURCE-PERSONS", "SOURCE-AUCTIONS", "FILTER-STATE", "FILTER-CATEGORY", "JOIN"),
        Arrays.asList(GenealogMetadataProbe.expectedOperators("nexmark_2")));

    assertTrue(Arrays.asList(GenealogMetadataProbe.expectedOperators("twitter_1"))
        .contains("SOURCE"));
    assertTrue(Arrays.asList(GenealogMetadataProbe.expectedOperators("twitter_1"))
        .contains("FILTER-VERIFIED"));
    assertFalse(Arrays.asList(GenealogMetadataProbe.expectedOperators("taxi_1"))
        .contains("MUX-BOROUGH"));

    for (int i = 0; i < DATAFLOWS.length; i++) {
      String[] ops = GenealogMetadataProbe.expectedOperators(DATAFLOWS[i]);
      assertTrue(DATAFLOWS[i], ops.length >= 1);
      assertEquals(DATAFLOWS[i] + " final output once",
          lastLogicalOutput(DATAFLOWS[i]), ops[ops.length - 1]);
      for (int j = 0; j < ops.length; j++) {
        assertFalse(ops[j], ops[j].startsWith("PROBE-"));
        assertFalse(ops[j], GenealogMetadataProbe.TOTAL.equals(ops[j]));
        assertFalse(ops[j], ops[j].contains("SINK"));
      }
    }
  }

  @Test
  public void eachDataflowContainsItsCompleteExpectedProbeSet() throws Exception {
    for (int i = 0; i < JOB_SOURCES.length; i++) {
      String dataflow = JOB_SOURCES[i][0];
      String src = readJobSource(JOB_SOURCES[i][1]);
      assertTrue(dataflow + " must detect metadataVolumeDir",
          src.contains("GenealogMetadataProbe.enabled(")
              || src.contains("metadataVolumeDir()"));
      assertTrue(dataflow + " must write CSV after execute",
          src.contains("GenealogMetadataProbe.writeIfEnabled("));
      assertTrue(dataflow + " must use discard sink in metadata-volume mode",
          src.contains("new GenealogCardinalityDiscardSink"));
      assertTrue(dataflow + " must keep traversal sink for query timing",
          src.contains("new GenealogTraversalSink"));
      String[] expected = GenealogMetadataProbe.expectedOperators(dataflow);
      for (int j = 0; j < expected.length; j++) {
        String op = expected[j];
        assertTrue(dataflow + " missing probe " + op,
            src.contains("GenealogMetadataProbe.attach(") && attachMentions(src, op));
        assertEquals(dataflow + " probe " + op + " must appear once",
            1, countAttach(src, op));
      }
      assertFalse(dataflow + " must not treat probes as logical outputs",
          src.contains(".name(\"PROBE-"));
      assertFalse(dataflow + " must not probe MUX-BOROUGH",
          attachMentions(src, "MUX-BOROUGH"));
    }
  }

  private static String lastLogicalOutput(String dataflow) {
    if ("twitter_2".equals(dataflow)) {
      return "AGG-6H";
    }
    if ("nexmark_1".equals(dataflow)) {
      return "AGG-COUNT";
    }
    return "JOIN";
  }

  private static boolean attachMentions(String src, String operator) {
    return countAttach(src, operator) > 0;
  }

  private static int countAttach(String src, String operator) {
    String needle = "\"" + operator + "\"";
    int count = 0;
    int from = 0;
    while (true) {
      int attach = src.indexOf("GenealogMetadataProbe.attach(", from);
      if (attach < 0) {
        return count;
      }
      int end = src.indexOf(";", attach);
      if (end < 0) {
        return count;
      }
      String call = src.substring(attach, end);
      if (call.contains(needle)) {
        count++;
      }
      from = attach + 1;
    }
  }

  private static String readJobSource(String relative) throws Exception {
    File src = new File("src/main/java/" + relative);
    if (!src.isFile()) {
      src = new File("competitors/ananke/src/main/java/" + relative);
    }
    return readFile(src);
  }

  private static Map<String, OptionalFailure<Object>> asResultAcc(
      Map<String, Object> acc) {
    Map<String, OptionalFailure<Object>> out =
        new LinkedHashMap<String, OptionalFailure<Object>>();
    for (Map.Entry<String, Object> e : acc.entrySet()) {
      out.put(e.getKey(), OptionalFailure.of(e.getValue()));
    }
    return out;
  }

  private static Map<String, Object> twitter2Accumulators() {
    Map<String, Object> acc = new LinkedHashMap<String, Object>();
    acc.put(GenealogMetadataProbe.recordsName("SOURCE"), Long.valueOf(20L));
    acc.put(GenealogMetadataProbe.bytesName("SOURCE"), Long.valueOf(640L));
    acc.put(GenealogMetadataProbe.recordsName("MAP"), Long.valueOf(10L));
    acc.put(GenealogMetadataProbe.bytesName("MAP"), Long.valueOf(320L));
    acc.put(GenealogMetadataProbe.recordsName("AGG-6H"), Long.valueOf(3L));
    acc.put(GenealogMetadataProbe.bytesName("AGG-6H"), Long.valueOf(96L));
    return acc;
  }

  private static String readFile(File src) throws Exception {
    BufferedReader in = new BufferedReader(
        new InputStreamReader(new FileInputStream(src), StandardCharsets.UTF_8));
    try {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = in.readLine()) != null) {
        sb.append(line).append('\n');
      }
      return sb.toString();
    } finally {
      in.close();
    }
  }

  static final class Rec implements GenealogTuple, java.io.Serializable {
    private static final long serialVersionUID = 1L;
    private GenealogData gdata;

    static Rec annotated() {
      Rec rec = new Rec();
      rec.gdata = new GenealogData();
      rec.gdata.init(GenealogTupleType.SOURCE);
      return rec;
    }

    static Rec bare() {
      return new Rec();
    }

    @Override
    public void initGenealog(GenealogTupleType tupleType) {
      gdata = new GenealogData();
      gdata.init(tupleType);
    }

    @Override
    public GenealogData getGenealogData() {
      return gdata;
    }

    @Override
    public long getTimestamp() {
      return 0L;
    }

    @Override
    public void setTimestamp(long timestamp) {
    }

    @Override
    public long getStimulus() {
      return 0L;
    }

    @Override
    public void setStimulus(long stimulus) {
    }
  }
}
