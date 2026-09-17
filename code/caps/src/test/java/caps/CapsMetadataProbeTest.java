package caps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

public class CapsMetadataProbeTest {

  private static final String[] DATAFLOWS = {
      "twitter_1", "twitter_2", "taxi_1", "taxi_2", "nexmark_1", "nexmark_2"
  };

  private static final String[][] JOB_SOURCES = {
      {"twitter_1", "TwitterVerifiedJoinCaps.java"},
      {"twitter_2", "TwitterLocationAvgCaps.java"},
      {"taxi_1", "TaxiPassengerJoinCaps.java"},
      {"taxi_2", "TaxiPaymentAvgCaps.java"},
      {"nexmark_1", "NexmarkMonitorNewUsersCaps.java"},
      {"nexmark_2", "NexmarkLocalItemSuggestionCaps.java"},
  };

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void countsExactRecordsAndPayloadBytesChannelsTimesEight() throws Exception {
    CapsMetadataProbe<Rec> probe = new CapsMetadataProbe<Rec>("MAP", Rec.HM);
    LongCounter records = new LongCounter();
    LongCounter bytes = new LongCounter();
    probe.records = records;
    probe.bytes = bytes;

    Rec twoCh = Rec.of(HowMuch.of(1L, 0L));
    Rec fourCh = Rec.of(HowMuch.of(1L, 0L, 2L, 0L));
    Rec oneCh = Rec.of(HowMuch.of(3L));
    assertSame(twoCh, probe.map(twoCh));
    assertSame(fourCh, probe.map(fourCh));
    assertSame(oneCh, probe.map(oneCh));

    assertEquals(3L, records.getLocalValue().longValue());
    assertEquals(16L + 32L + 8L, bytes.getLocalValue().longValue());
  }

  @Test
  public void writesCanonicalPerOperatorAndTotalCsv() throws Exception {
    Map<String, Object> acc = new LinkedHashMap<String, Object>();
    acc.put(CapsMetadataProbe.recordsName("MAP"), Long.valueOf(10L));
    acc.put(CapsMetadataProbe.bytesName("MAP"), Long.valueOf(160L));
    acc.put(CapsMetadataProbe.recordsName("AGG"), Long.valueOf(2L));
    acc.put(CapsMetadataProbe.bytesName("AGG"), Long.valueOf(32L));

    File csv = new File(tmp.newFolder("bench"), CapsMetadataProbe.CSV_FILENAME);
    CapsMetadataProbe.writeCanonicalCsv(acc, csv, "twitter_2",
        new String[] {"MAP", "AGG"});

    assertEquals(
        "dataflow,operator,records,bytes\n"
            + "twitter_2,MAP,10,160\n"
            + "twitter_2,AGG,2,32\n"
            + "twitter_2,TOTAL,12,192\n",
        readFile(csv));
  }

  @Test
  public void rejectsDuplicateAndMissingExpectedProbes() throws Exception {
    File csv = new File(tmp.getRoot(), "dup.csv");
    Map<String, Object> acc = new HashMap<String, Object>();
    acc.put(CapsMetadataProbe.recordsName("MAP"), Long.valueOf(1L));
    acc.put(CapsMetadataProbe.bytesName("MAP"), Long.valueOf(16L));

    try {
      CapsMetadataProbe.writeCanonicalCsv(acc, csv, "twitter_2",
          new String[] {"MAP", "MAP"});
      fail("duplicate expected probe");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("duplicate"));
    }

    try {
      CapsMetadataProbe.writeCanonicalCsv(acc, csv, "twitter_2",
          new String[] {"MAP", "AGG"});
      fail("missing expected probe");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("missing"));
    }

    acc.put(CapsMetadataProbe.recordsName("EXTRA"), Long.valueOf(1L));
    acc.put(CapsMetadataProbe.bytesName("EXTRA"), Long.valueOf(16L));
    try {
      CapsMetadataProbe.writeCanonicalCsv(acc, csv, "twitter_2",
          new String[] {"MAP"});
      fail("unexpected extra probe");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("unexpected"));
    }
  }

  @Test
  public void memoryVolumeModeIsDedicatedAndWriteIsNoopOtherwise() throws Exception {
    assertTrue(CapsMetadataProbe.enabled(
        new String[] {"in", "out", CapsMetadataProbe.MODE, tmp.getRoot().getAbsolutePath()}, 2));
    assertFalse(CapsMetadataProbe.enabled(
        new String[] {"in", "out", "provenance", tmp.getRoot().getAbsolutePath(), "0.01"}, 2));
    assertFalse(CapsMetadataProbe.enabled(new String[] {"in", "out"}, 2));

    File bench = tmp.newFolder("noop");
    CapsMetadataProbe.writeIfEnabled(null,
        new String[] {"in", "out", "provenance", bench.getAbsolutePath(), "0.01"},
        2, "twitter_2");
    assertFalse(new File(bench, CapsMetadataProbe.CSV_FILENAME).exists());
  }

  @Test
  public void writeIfEnabledPersistsDiscardSinkRecords() throws Exception {
    Map<String, Object> acc = twitter2Accumulators();
    acc.put(CapsCardinalityDiscardSink.RECORDS_NAME, Long.valueOf(3L));
    File bench = tmp.newFolder("sink-write");
    CapsMetadataProbe.writeIfEnabled(
        new JobExecutionResult(new JobID(), 1L, asResultAcc(acc)),
        new String[] {"in", "out", CapsMetadataProbe.MODE, bench.getAbsolutePath()},
        2, "twitter_2");
    assertEquals("3\n", readFile(new File(bench, "sink_records.txt")));
  }

  @Test
  public void writeIfEnabledRejectsMissingDiscardSinkRecords() throws Exception {
    File bench = tmp.newFolder("sink-missing");
    try {
      CapsMetadataProbe.writeIfEnabled(
          new JobExecutionResult(new JobID(), 1L, asResultAcc(twitter2Accumulators())),
          new String[] {"in", "out", CapsMetadataProbe.MODE, bench.getAbsolutePath()},
          2, "twitter_2");
      fail("missing discard sink");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().toLowerCase().contains("sink"));
    }
  }

  @Test
  public void attachDisabledIsIdentityAndDoesNotAddLogicalOutput() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Rec> stream = env.fromElements(Rec.of(HowMuch.of(1L, 0L)));
    assertSame(stream, CapsMetadataProbe.attach(stream, false, "MAP", Rec.HM));
  }

  @Test
  public void localExecutionMergesAccumulatorsAndWritesCanonicalCsv() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    DataStream<Rec> stream = env.fromElements(
        Rec.of(HowMuch.of(1L, 0L)),
        Rec.of(HowMuch.of(0L, 1L)));
    stream = CapsMetadataProbe.attach(stream, true, "MAP", Rec.HM);
    JobExecutionResult result = env.execute("caps-metadata-probe-merge");
    assertEquals(2L, ((Number) result.getAccumulatorResult(
        CapsMetadataProbe.recordsName("MAP"))).longValue());
    assertEquals(32L, ((Number) result.getAccumulatorResult(
        CapsMetadataProbe.bytesName("MAP"))).longValue());

    File bench = tmp.newFolder("local-exec");
    File csv = new File(bench, CapsMetadataProbe.CSV_FILENAME);
    CapsMetadataProbe.writeCanonicalCsv(result.getAllAccumulatorResults(), csv,
        "twitter_2", new String[] {"MAP"});
    assertEquals(
        "dataflow,operator,records,bytes\n"
            + "twitter_2,MAP,2,32\n"
            + "twitter_2,TOTAL,2,32\n",
        readFile(csv));
  }

  @Test
  public void expectedOperatorsCoverEveryProvenanceBearingOutputOnce() {
    assertEquals(Arrays.asList("MAP-ALL", "AGG-ALL", "MAP-VERIFIED", "AGG-VERIFIED", "JOIN"),
        Arrays.asList(CapsMetadataProbe.expectedOperators("twitter_1")));
    assertEquals(Arrays.asList("MAP", "AGG-6H"),
        Arrays.asList(CapsMetadataProbe.expectedOperators("twitter_2")));
    assertEquals(Arrays.asList(
            "MAP-SOLO", "AGG-SOLO", "FILTER-COUNT-SOLO",
            "MAP-CROWDED", "AGG-CROWDED", "FILTER-COUNT-CROWDED", "JOIN"),
        Arrays.asList(CapsMetadataProbe.expectedOperators("taxi_1")));
    assertEquals(Arrays.asList("MAP-CARD", "AGG-CARD", "MAP-CASH", "AGG-CASH", "JOIN"),
        Arrays.asList(CapsMetadataProbe.expectedOperators("taxi_2")));
    assertEquals(Arrays.asList("JOIN", "AGG-COUNT"),
        Arrays.asList(CapsMetadataProbe.expectedOperators("nexmark_1")));
    assertEquals(Arrays.asList("JOIN"),
        Arrays.asList(CapsMetadataProbe.expectedOperators("nexmark_2")));

    for (int i = 0; i < DATAFLOWS.length; i++) {
      String[] ops = CapsMetadataProbe.expectedOperators(DATAFLOWS[i]);
      assertTrue(DATAFLOWS[i], ops.length >= 1);
      assertEquals(DATAFLOWS[i] + " final output once",
          lastLogicalOutput(DATAFLOWS[i]), ops[ops.length - 1]);
      for (int j = 0; j < ops.length; j++) {
        assertFalse(ops[j], ops[j].startsWith("PROBE-"));
        assertFalse(ops[j], CapsMetadataProbe.TOTAL.equals(ops[j]));
        assertFalse(ops[j], ops[j].contains("SINK"));
        assertFalse(ops[j], ops[j].startsWith("SOURCE"));
        assertFalse(ops[j], ops[j].equals("FILTER-VERIFIED"));
        assertFalse(ops[j], ops[j].equals("FILTER-SOLO"));
        assertFalse(ops[j], ops[j].equals("FILTER-CROWDED"));
        assertFalse(ops[j], ops[j].equals("FILTER-CARD"));
        assertFalse(ops[j], ops[j].equals("FILTER-CASH"));
        assertFalse(ops[j], ops[j].equals("FILTER-STATE"));
        assertFalse(ops[j], ops[j].equals("FILTER-CATEGORY"));
      }
    }
  }

  @Test
  public void eachDataflowContainsItsCompleteExpectedProbeSet() throws Exception {
    int checked = 0;
    for (int i = 0; i < JOB_SOURCES.length; i++) {
      String dataflow = JOB_SOURCES[i][0];
      String src = readJobSource(JOB_SOURCES[i][1]);
      // A repo may ship a subset of the paper's dataflows, since the Twitter
      // input cannot be redistributed. Absent sources are skipped, not failed.
      if (src == null) {
        continue;
      }
      checked++;
      assertTrue(dataflow + " must detect memory_volume",
          src.contains("CapsMetadataProbe.enabled("));
      assertTrue(dataflow + " must write CSV after execute",
          src.contains("CapsMetadataProbe.writeIfEnabled("));
      assertTrue(dataflow + " must keep BenchSinks",
          src.contains("BenchSinks.create("));
      assertTrue(dataflow + " must keep FileSink answers",
          src.contains("new FileSink("));
      assertTrue(dataflow + " must use discard sink in memory_volume",
          src.contains("new CapsCardinalityDiscardSink"));
      String[] expected = CapsMetadataProbe.expectedOperators(dataflow);
      for (int j = 0; j < expected.length; j++) {
        String op = expected[j];
        assertTrue(dataflow + " missing probe " + op,
            src.contains("CapsMetadataProbe.attach(") && attachMentions(src, op));
        assertEquals(dataflow + " probe " + op + " must appear once",
            1, countAttach(src, op));
      }
      assertFalse(dataflow + " must not treat probes as logical outputs",
          src.contains(".name(\"PROBE-"));
    }
    assertTrue("no job source found to check", checked > 0);
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
      int attach = src.indexOf("CapsMetadataProbe.attach(", from);
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

  /** The job source, or null when this repo does not ship that dataflow. */
  private static String readJobSource(String fileName) throws Exception {
    File src = new File("../" + jobDir(fileName) + "/src/main/java/" + fileName);
    if (!src.isFile()) {
      src = new File("competitors/caps/" + jobDir(fileName) + "/src/main/java/" + fileName);
    }
    return src.isFile() ? readFile(src) : null;
  }

  private static String jobDir(String fileName) {
    if (fileName.startsWith("TwitterVerified")) {
      return "twitter_1";
    }
    if (fileName.startsWith("TwitterLocation")) {
      return "twitter_2";
    }
    if (fileName.startsWith("TaxiPassenger")) {
      return "taxi_1";
    }
    if (fileName.startsWith("TaxiPayment")) {
      return "taxi_2";
    }
    if (fileName.startsWith("NexmarkMonitor")) {
      return "nexmark_1";
    }
    return "nexmark_2";
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
    acc.put(CapsMetadataProbe.recordsName("MAP"), Long.valueOf(10L));
    acc.put(CapsMetadataProbe.bytesName("MAP"), Long.valueOf(160L));
    acc.put(CapsMetadataProbe.recordsName("AGG-6H"), Long.valueOf(3L));
    acc.put(CapsMetadataProbe.bytesName("AGG-6H"), Long.valueOf(48L));
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

  static final class Rec implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    final HowMuch hm;

    Rec(HowMuch hm) {
      this.hm = hm;
    }

    static Rec of(HowMuch hm) {
      return new Rec(hm);
    }

    static final IndexingSink.HmFn<Rec> HM = new IndexingSink.HmFn<Rec>() {
      @Override
      public HowMuch hm(Rec tuple) {
        return tuple.hm;
      }
    };
  }
}
