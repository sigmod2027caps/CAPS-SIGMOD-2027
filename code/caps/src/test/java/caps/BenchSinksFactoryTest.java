package caps;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import temporalindex.SampleKeyFn;

public class BenchSinksFactoryTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void parsesProvenanceIdsArgs() throws Exception {
    File bench = tmp.newFolder("ids-bench");
    File manifest = new File(tmp.getRoot(), "ids.txt");
    RichSinkFunction<Rec> sink = BenchSinks.create(
        new String[] {"provenance_ids", bench.getAbsolutePath(), manifest.getAbsolutePath(), "1000"},
        0, Rec.TS, Rec.HM, Rec.KEY, 2, 0L);
    assertTrue(sink instanceof ProvenanceQuerySink);
    ProvenanceQuerySink<Rec> q = (ProvenanceQuerySink<Rec>) sink;
    assertTrue(q.generatesIds());
    assertFalse(q.sharedIds());
    assertFalse(q.calibrates());
    assertFalse(q.verifies());
    assertFalse(q.vectorQuery());
    assertTrue(q.manifestPath().equals(manifest.getAbsolutePath()));
    assertTrue(q.generateCount() == 1000);
  }

  @Test
  public void parsesProvenanceSharedArgs() throws Exception {
    File bench = tmp.newFolder("shared-bench");
    File manifest = new File(tmp.getRoot(), "shared.txt");
    RichSinkFunction<Rec> sink = BenchSinks.create(
        new String[] {"provenance_shared", bench.getAbsolutePath(), manifest.getAbsolutePath()},
        0, Rec.TS, Rec.HM, Rec.KEY, 2, 0L);
    assertTrue(sink instanceof ProvenanceQuerySink);
    ProvenanceQuerySink<Rec> q = (ProvenanceQuerySink<Rec>) sink;
    assertTrue(q.sharedIds());
    assertFalse(q.generatesIds());
    assertFalse(q.calibrates());
    assertFalse(q.verifies());
    assertFalse(q.vectorQuery());
    assertTrue(q.manifestPath().equals(manifest.getAbsolutePath()));
  }

  @Test
  public void keepsOldProvenanceModesUnchanged() {
    String dir = tmp.getRoot().getAbsolutePath();
    assertOldMode("provenance", dir, false, false, false);
    assertOldMode("provenance_timer", dir, true, false, false);
    assertOldMode("provenance_verify", dir, false, true, true);
    assertOldMode("provenance_vector", dir, false, false, true);
    assertOldMode("provenance_vector_timer", dir, true, false, true);
  }

  @Test
  public void rejectsUnknownMode() {
    try {
      BenchSinks.create(new String[] {"nope", "d", "0.1"}, 0, Rec.TS, Rec.HM, Rec.KEY, 1, 0L);
      fail("expected unknown mode");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("unknown"));
    }
  }

  private static void assertOldMode(String mode, String dir, boolean calibrate,
      boolean verify, boolean vector) {
    RichSinkFunction<Rec> sink = BenchSinks.create(
        new String[] {mode, dir, "0.01"}, 0, Rec.TS, Rec.HM, Rec.KEY, 2, 0L);
    assertTrue(sink instanceof ProvenanceQuerySink);
    ProvenanceQuerySink<Rec> q = (ProvenanceQuerySink<Rec>) sink;
    assertFalse(q.generatesIds());
    assertFalse(q.sharedIds());
    assertTrue(q.calibrates() == calibrate);
    assertTrue(q.verifies() == verify);
    assertTrue(q.vectorQuery() == vector);
  }

  static final class Rec {
    final long id;
    final long ts;
    final HowMuch hm;

    Rec(long id, long ts, HowMuch hm) {
      this.id = id;
      this.ts = ts;
      this.hm = hm;
    }

    static final IndexingSink.TsFn<Rec> TS = new IndexingSink.TsFn<Rec>() {
      @Override
      public long ts(Rec tuple) {
        return tuple.ts;
      }
    };
    static final IndexingSink.HmFn<Rec> HM = new IndexingSink.HmFn<Rec>() {
      @Override
      public HowMuch hm(Rec tuple) {
        return tuple.hm;
      }
    };
    static final SampleKeyFn<Rec> KEY = new SampleKeyFn<Rec>() {
      @Override
      public void writeKey(Rec value, java.io.DataOutput out) throws java.io.IOException {
        out.writeLong(value.id);
      }
    };
  }
}
