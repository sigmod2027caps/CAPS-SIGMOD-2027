package caps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import temporalindex.LogicalSampler;
import temporalindex.QueryFixture;
import temporalindex.QueryIdManifest;

import caps.ProvenanceQuerySinkSharedTest.Rec;

// The per-output provenance sinks at several subtasks: the checks that used to live in close() of a
// single subtask now run over the merged partial results, and the merged files equal those of one subtask.
public class ProvenanceQuerySinkParallelTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  /** Runs `sink` copies, one per subtask, record i going to subtask i % k; k = 1 is the reference. */
  void run(String[] args, int k, int n, File dir) throws Exception {
    @SuppressWarnings("unchecked")
    ProvenanceQuerySink<Rec> proto = (ProvenanceQuerySink<Rec>) BenchSinks.create(
        args, 0, Rec.TS, Rec.HM, Rec.KEY, 2);
    // Java serialization stands in for the copy Flink ships to every subtask.
    ProvenanceQuerySink<Rec>[] sinks = (ProvenanceQuerySink<Rec>[]) new ProvenanceQuerySink[k];
    for (int s = 0; s < k; s++) {
      sinks[s] = TieredIndexSinkTest.copy(proto);
      setSubtask(sinks[s], s, k);
      sinks[s].open(new Configuration());
    }
    for (int i = 0; i < n; i++)
      sinks[i % k].invoke(Rec.of(mix(i % 4000), HowMuch.of(i % 5, 1 + i % 3)), null); // duplicates on purpose
    for (int s = 0; s < k; s++)
      sinks[s].close();
  }

  /** FNV-1a's high bits are poor for tiny integer keys; real keys are rich, the test keys are mixed. */
  static long mix(long z) {
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
    return (z ^ (z >>> 31)) & 0x7fffffffffffL;
  }

  static void setSubtask(ProvenanceQuerySink<Rec> sink, int s, int k) throws Exception {
    java.lang.reflect.Field a = ProvenanceQuerySink.class.getDeclaredField("subtask");
    java.lang.reflect.Field b = ProvenanceQuerySink.class.getDeclaredField("parallelism");
    a.setAccessible(true);
    b.setAccessible(true);
    a.setInt(sink, s);
    b.setInt(sink, k);
  }

  static List<String> lines(File f) throws Exception {
    return Files.readAllLines(f.toPath());
  }

  @Test
  public void timingModeMergesSamplesAndOutputs() throws Exception {
    File one = tmp.newFolder("t1");
    File four = tmp.newFolder("t4");
    run(new String[] {"provenance", one.getAbsolutePath(), "0.2"}, 1, 5000, one);
    run(new String[] {"provenance", four.getAbsolutePath(), "0.2"}, 4, 5000, four);
    String[] a = lines(new File(one, "provenance_query.csv")).get(1).split(",");
    String[] b = lines(new File(four, "provenance_query.csv")).get(1).split(",");
    assertEquals(a[1], b[1]); // outputs
    assertEquals(a[2], b[2]); // samples
    assertEquals(a[5], b[5]); // checksum
    assertEquals(a[7], b[7]); // predicate checksum
    assertEquals(a[8], b[8]); // unique selected
    assertEquals(lines(new File(one, "selected_samples_caps.csv")), lines(new File(four, "selected_samples_caps.csv")));
    assertEquals("5000", lines(new File(four, "outputs_caps.txt")).get(0));
  }

  @Test
  public void aSubtaskWithoutHeadsOrOutputsDoesNotFailTheJob() throws Exception {
    File four = tmp.newFolder("few");
    // 3 outputs over 4 subtasks: one subtask sees nothing, and p = 0.5 leaves some without heads
    run(new String[] {"provenance", four.getAbsolutePath(), "1.0"}, 4, 3, four);
    assertEquals("3", lines(new File(four, "outputs_caps.txt")).get(0));
  }

  @Test
  public void verifyModeMergesGroups() throws Exception {
    File one = tmp.newFolder("v1");
    File three = tmp.newFolder("v3");
    run(new String[] {"provenance_verify", one.getAbsolutePath(), "1.0"}, 1, 3000, one);
    run(new String[] {"provenance_verify", three.getAbsolutePath(), "1.0"}, 3, 3000, three);
    assertEquals(lines(new File(one, "provenance_answers.csv")), lines(new File(three, "provenance_answers.csv")));
  }

  @Test
  public void manifestIdsAreGeneratedAndMatchedAcrossSubtasks() throws Exception {
    File gen1 = tmp.newFolder("g1");
    File gen3 = tmp.newFolder("g3");
    File m1 = new File(tmp.getRoot(), "m1.txt");
    File m3 = new File(tmp.getRoot(), "m3.txt");
    run(new String[] {"provenance_ids", gen1.getAbsolutePath(), m1.getAbsolutePath(), "1000"}, 1, 5000, gen1);
    run(new String[] {"provenance_ids", gen3.getAbsolutePath(), m3.getAbsolutePath(), "1000"}, 3, 5000, gen3);
    assertEquals(lines(m1), lines(m3));
    assertEquals(1001, lines(m3).size());

    File shared = tmp.newFolder("s3");
    run(new String[] {"provenance_shared", shared.getAbsolutePath(), m3.getAbsolutePath()}, 3, 5000, shared);
    List<String> answers = lines(new File(shared, "query_answers_caps.csv"));
    assertEquals(1001, answers.size());
    QueryFixture f = QueryFixture.load(new File(shared, "query_fixture_caps.ser").getAbsolutePath(),
        QueryFixture.METHOD_CAPS);
    assertEquals(1000, f.ids().length);
  }

  @Test
  public void manifestIdMissingFromEverySubtaskFailsOnlyInTheMerge() throws Exception {
    File bench = tmp.newFolder("miss");
    File m = new File(tmp.getRoot(), "miss.txt");
    QueryIdManifest.Generator g = new QueryIdManifest.Generator(1000, 42L);
    LogicalSampler sampler = new LogicalSampler();
    for (int i = 0; i < 2000; i++)
      g.observe(sampler.hashKey(Rec.KEY, Rec.of(i, HowMuch.of(1))));
    g.write(m.getAbsolutePath());
    try {
      // outputs 0..1499 only: some manifest ids never occur anywhere
      run(new String[] {"provenance_shared", bench.getAbsolutePath(), m.getAbsolutePath()}, 3, 600, bench);
      fail("expected unmatched manifest ids");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("unmatched"));
    }
  }
}
