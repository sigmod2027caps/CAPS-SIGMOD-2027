package ink;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import temporalindex.SampleKeyFn;

public class InkBenchSinksFactoryTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void parsesProvenanceSharedArgs() throws Exception {
    File bench = tmp.newFolder("shared-bench");
    File manifest = new File(tmp.getRoot(), "shared.txt");
    RichSinkFunction<Rec> sink = InkBenchSinks.create(
        new String[] {"provenance_shared", bench.getAbsolutePath(), manifest.getAbsolutePath()},
        0, Rec.TS, Rec.META, Rec.KEY, 2, 0L);
    assertTrue(sink instanceof InkQuerySink);
    InkQuerySink<Rec> q = (InkQuerySink<Rec>) sink;
    assertTrue(q.sharedIds());
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
  public void doesNotAddProvenanceIdsMode() {
    try {
      InkBenchSinks.create(
          new String[] {"provenance_ids", tmp.getRoot().getAbsolutePath(), "m.txt", "1000"},
          0, Rec.TS, Rec.META, Rec.KEY, 2, 0L);
      fail("Green must not grow a provenance_ids factory mode");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("unknown"));
    }
  }

  private static void assertOldMode(String mode, String dir, boolean calibrate,
      boolean verify, boolean vector) {
    RichSinkFunction<Rec> sink = InkBenchSinks.create(
        new String[] {mode, dir, "0.01"}, 0, Rec.TS, Rec.META, Rec.KEY, 2, 0L);
    assertTrue(sink instanceof InkQuerySink);
    InkQuerySink<Rec> q = (InkQuerySink<Rec>) sink;
    assertFalse(q.sharedIds());
    assertTrue(q.calibrates() == calibrate);
    assertTrue(q.verifies() == verify);
    assertTrue(q.vectorQuery() == vector);
  }

  static final class Rec {
    final long id;
    final long ts;
    final InkMeta meta;

    Rec(long id, long ts, InkMeta meta) {
      this.id = id;
      this.ts = ts;
      this.meta = meta;
    }

    static final InkQuerySink.TsFn<Rec> TS = new InkQuerySink.TsFn<Rec>() {
      @Override
      public long ts(Rec tuple) {
        return tuple.ts;
      }
    };
    static final InkQuerySink.MetaFn<Rec> META = new InkQuerySink.MetaFn<Rec>() {
      @Override
      public InkMeta meta(Rec tuple) {
        return tuple.meta;
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
