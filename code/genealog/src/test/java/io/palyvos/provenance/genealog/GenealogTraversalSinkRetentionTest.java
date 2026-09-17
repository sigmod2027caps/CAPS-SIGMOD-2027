package io.palyvos.provenance.genealog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.palyvos.provenance.util.ExperimentSettings;
import java.io.File;
import java.lang.reflect.Field;
import java.util.Collection;
import org.apache.flink.configuration.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Timing-mode GeneaLog must retain every sink output as an Entry so the
 * provenance graph stays strongly reachable until close.
 */
public class GenealogTraversalSinkRetentionTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void timingModeRetainsEachOutputInEntryList() throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(new String[] {
        "--statisticsFolder", "/tmp",
        "--outputFile", "out",
        "--sourcesNumber", "1",
        "--traversalSampleProb", "0.0"
    });
    GenealogTraversalSink<StubSource> sink =
        new GenealogTraversalSink<StubSource>(settings);
    sink.open(new Configuration());

    StubSource[] outputs = new StubSource[] {
        source(10L), source(20L), source(30L)
    };
    for (int i = 0; i < outputs.length; i++) {
      sink.invoke(outputs[i], null);
    }

    assertRetained(sink, outputs);
  }

  @Test
  public void sharedModeRetainsEveryOutputAndQueriesSelectedIdOnce() throws Exception {
    File manifest = new File(tmp.getRoot(), "retain.txt");
    GenealogTraversalSinkSharedTest.writeGeneratedManifest(manifest, 2, 2);
    ExperimentSettings settings = ExperimentSettings.newInstance(new String[] {
        "--statisticsFolder", tmp.getRoot().getAbsolutePath(),
        "--outputFile", "out",
        "--sourcesNumber", "1",
        "--traversalBenchDir", tmp.newFolder("retain-bench").getAbsolutePath(),
        "--traversalQueryIds", manifest.getAbsolutePath(),
        "--traversalSampleProb", "1.0",
        "--traversalCalibrate"
    });
    GenealogTraversalSink<GenealogTraversalSinkSharedTest.StubTuple> sink =
        new GenealogTraversalSink<GenealogTraversalSinkSharedTest.StubTuple>(
            settings, GenealogTraversalSinkSharedTest.StubTuple.KEY);
    sink.open(new Configuration());

    GenealogTraversalSinkSharedTest.StubTuple first =
        GenealogTraversalSinkSharedTest.contributions(0L, 3);
    GenealogTraversalSinkSharedTest.StubTuple duplicate =
        GenealogTraversalSinkSharedTest.contributions(0L, 11);
    GenealogTraversalSinkSharedTest.StubTuple second =
        GenealogTraversalSinkSharedTest.contributions(1L, 1);
    GenealogTraversalSinkSharedTest.StubTuple nonselected =
        GenealogTraversalSinkSharedTest.contributions(99L, 4);
    GenealogTraversalSinkSharedTest.StubTuple[] outputs =
        new GenealogTraversalSinkSharedTest.StubTuple[] {
            first, duplicate, second, nonselected
        };
    for (int i = 0; i < outputs.length; i++) {
      sink.invoke(outputs[i], null);
    }

    assertRetained(sink, outputs);

    Field answersField = GenealogTraversalSink.class.getDeclaredField("answers");
    answersField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Long, Long> answers =
        (java.util.Map<Long, Long>) answersField.get(sink);
    assertNotNull("shared mode stores one answer per selected ID", answers);
    assertEquals(2, answers.size());
    assertEquals(Long.valueOf(3L),
        answers.get(Long.valueOf(GenealogTraversalSinkSharedTest.hash(first))));
    assertEquals(Long.valueOf(1L),
        answers.get(Long.valueOf(GenealogTraversalSinkSharedTest.hash(second))));
    assertTrue(!answers.containsKey(
        Long.valueOf(GenealogTraversalSinkSharedTest.hash(nonselected))));
  }

  private static void assertRetained(GenealogTraversalSink<?> sink, GenealogTuple[] outputs)
      throws Exception {
    Field entriesField = GenealogTraversalSink.class.getDeclaredField("entries");
    entriesField.setAccessible(true);
    Object retained = entriesField.get(sink);
    assertNotNull("must keep a retained-entry collection", retained);
    assertTrue("retained entries must be a Collection", retained instanceof Collection);
    Collection<?> entries = (Collection<?>) retained;
    assertEquals("one Entry per sink output, including nonselected and duplicates",
        outputs.length, entries.size());

    int i = 0;
    for (Object entry : entries) {
      assertNotNull("Entry allocation is required", entry);
      Field tsField = entry.getClass().getDeclaredField("ts");
      Field tupleField = entry.getClass().getDeclaredField("tuple");
      tsField.setAccessible(true);
      tupleField.setAccessible(true);
      assertEquals(outputs[i].getTimestamp(), tsField.getLong(entry));
      Object held = tupleField.get(entry);
      assertSame("output must remain strongly referenced by its Entry",
          outputs[i], held);
      i++;
    }
    assertEquals(outputs.length, i);
  }

  private static StubSource source(long ts) {
    StubSource t = new StubSource(ts);
    t.initGenealog(GenealogTupleType.SOURCE);
    return t;
  }

  private static final class StubSource implements GenealogTuple {
    private final long timestamp;
    private GenealogData gdata;

    StubSource(long timestamp) {
      this.timestamp = timestamp;
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
      return timestamp;
    }

    @Override
    public void setTimestamp(long timestamp) {
    }

    @Override
    public long getStimulus() {
      return 0;
    }

    @Override
    public void setStimulus(long stimulus) {
    }
  }
}
