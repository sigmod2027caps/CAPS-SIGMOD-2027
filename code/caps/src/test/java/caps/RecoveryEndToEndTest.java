package caps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import temporalindex.Disorder;
import temporalindex.DisorderConfig;
import temporalindex.DisorderSource;
import temporalindex.StampDerivedTime;

// End to end in the local runtime: file source (with disorder) -> assigner -> keyed windows -> index sink and id log,
// checkpointing every few milliseconds, and one injected failure in the source. The restored job must end with the
// index of the run without a failure, and the ids of its outputs, deduplicated, must be those of that run.
public class RecoveryEndToEndTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  public static class Line implements Serializable {
    public String key;
    public long ts;
    public long a;
    public long b;
  }

  public static class Out implements Serializable {
    public String key;
    public long ts;
    public HowMuch hm;

    @Override
    public String toString() {
      return key + "," + ts + "," + hm;
    }
  }

  public static class Acc implements Serializable {
    public String key;
    public long a;
    public long b;
  }

  public static class P extends DisorderSource.Parser<Line> {
    @Override
    public Line parse(String line, long lineNo) {
      String[] f = line.split(",");
      Line l = new Line();
      l.ts = Long.parseLong(f[0]);
      l.key = f[1];
      l.a = Long.parseLong(f[2]);
      l.b = Long.parseLong(f[3]);
      return l;
    }
  }

  public static class Sum implements AggregateFunction<Line, Acc, Out> {
    @Override
    public Acc createAccumulator() {
      return new Acc();
    }

    @Override
    public Acc add(Line v, Acc acc) {
      acc.key = v.key;
      acc.a += v.a;
      acc.b += v.b;
      return acc;
    }

    @Override
    public Out getResult(Acc acc) {
      Out o = new Out();
      o.key = acc.key;
      o.hm = HowMuch.of(acc.a, acc.b);
      return o;
    }

    @Override
    public Acc merge(Acc x, Acc y) {
      x.a += y.a;
      x.b += y.b;
      return x;
    }
  }

  private String input(int n) throws Exception {
    File f = tmp.newFile();
    Random rnd = new Random(3);
    try (PrintWriter pw = new PrintWriter(f)) {
      for (int i = 0; i < n; i++)
        pw.println((1000 + i / 3) + ",k" + rnd.nextInt(40) + ",1," + rnd.nextInt(3));
    }
    return f.getPath();
  }

  /** Runs the job; failAt = 0 for none. Returns the bench directory. */
  private File job(String path, long failAt, long disorder) throws Exception {
    File dir = tmp.newFolder();
    DisorderConfig cfg = DisorderConfig.of(new HashMap<String, String>() {{
      put("CAPS_CHECKPOINT_MS", "10");
      put("CAPS_RESTART_DELAY_MS", "10");
      put("CAPS_DISORDER_MS", Long.toString(disorder));
      put("CAPS_DELTA_MS", Long.toString(disorder));
      put("CAPS_FAIL_AT", Long.toString(failAt));
      put("CAPS_WM_INTERVAL_MS", "5");
    }});
    StreamExecutionEnvironment env = Disorder.environment(cfg);
    DataStream<Line> src = Disorder.source(env, cfg, "SOURCE", path, new P(), l -> l.ts, Line.class);
    DataStream<Out> out = src.keyBy(l -> l.key)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(100)))
        .aggregate(new Sum())
        .process(new StampDerivedTime<Out>((t, ts) -> t.ts = ts))
        .returns(Out.class);
    IndexingSink<Out> sink = new IndexingSink<>(o -> o.ts, o -> o.hm, 2, dir.getAbsolutePath(), 200, 500);
    sink.digestOn = true;
    sink.segmentDir = new File(dir, "segments").getAbsolutePath();
    BenchSinks.attach(out, sink, "SINK-INDEX");
    out.addSink(new OutputLogSink<Out>(dir.getAbsolutePath(), "AGG", o -> o.ts, o -> o.key)).setParallelism(1);
    Disorder.execute(env, "recovery-e2e");
    return dir;
  }

  private static List<String> lines(File dir, String name) throws Exception {
    return Files.readAllLines(new File(dir, name).toPath());
  }

  private void check(long disorder) throws Exception {
    String path = input(400000);
    File ref = job(path, 0, disorder);
    File rec = job(path, 330000, disorder);
    String[] stats = lines(rec, "recovery_stats.csv").get(1).split(",");
    assertEquals("one restore", "1", stats[4]);
    assertTrue("checkpoints completed", Integer.parseInt(stats[2]) > 0);

    // the index: every benchmark answer and the digest of Tier 2 are those of the uninterrupted run
    assertEquals(lines(ref, "query_answers.csv"), lines(rec, "query_answers.csv"));
    assertEquals(lines(ref, "index_digest.csv"), lines(rec, "index_digest.csv"));

    // the id log: duplicates are re-emissions; the distinct rows are the rows of the run without a failure
    List<String> a = lines(ref, "outputs_s0.log");
    List<String> b = lines(rec, "outputs_s0.log");
    assertEquals(a.size(), new HashSet<>(a).size());
    Set<String> distinct = new HashSet<>(b);
    assertEquals(new HashSet<>(a), distinct);
    assertTrue(b.size() >= distinct.size()); // re-emitted duplicates, if any output was written after the last checkpoint
    Map<String, Integer> ids = new HashMap<>();
    for (String r : distinct)
      ids.merge(r.substring(0, r.lastIndexOf('\t')), 1, Integer::sum);
    assertEquals("one content per id", distinct.size(), ids.size());
  }

  @Test
  public void orderedInputRecoversToTheSameIndex() throws Exception {
    check(0);
  }

  @Test
  public void disorderedInputRecoversToTheSameIndex() throws Exception {
    check(40);
  }
}
