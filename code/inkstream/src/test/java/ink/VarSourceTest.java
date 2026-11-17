package ink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.junit.Test;

// The variable ids of Green must be unique over the parallel instances of the operator that mints them (otherwise the
// polynomials of a parallel job merge variables of different tuples and shrink), identical to the ids of the
// unparallelized job at p = 1, and deterministic.
public class VarSourceTest {

  private static final int CHANNELS = 4;

  @Test
  public void parallelismOneKeepsTheSequenceEncoding() {
    VarSource v = new VarSource(CHANNELS);
    v.open(0, 1);
    for (int i = 0; i < 100; i++) {
      int channel = i % CHANNELS;
      assertEquals(InkMeta.source(channel, CHANNELS, i).poly.toString(), v.next(channel).poly.toString());
      assertEquals(i * CHANNELS + channel, onlyVar(InkMeta.source(channel, CHANNELS, i)));
    }
  }

  @Test
  public void subtasksMintDisjointIdsAndKeepTheChannel() {
    int p = 5, perSubtask = 2000;
    Set<Integer> seen = new HashSet<>();
    for (int s = 0; s < p; s++) {
      VarSource v = new VarSource(CHANNELS);
      v.open(s, p);
      for (int i = 0; i < perSubtask; i++) {
        int channel = (i * 7 + s) % CHANNELS;
        int id = onlyVar(v.next(channel));
        assertEquals("the channel is the id modulo the channel count", channel, id % CHANNELS);
        assertTrue("id " + id + " minted twice", seen.add(id));
      }
    }
    assertEquals(p * perSubtask, seen.size());
    // dense: the ids of p subtasks with n tuples each cover [0, p * n * channels) without gaps in the global sequence
    int max = 0;
    for (int id : seen)
      max = Math.max(max, id);
    assertTrue(max < p * perSubtask * CHANNELS);
  }

  @Test
  public void aProductOfTuplesOfTwoSubtasksKeepsBothVariables() {
    VarSource a = new VarSource(2), b = new VarSource(2);
    a.open(0, 2);
    b.open(1, 2);
    Polynomial product = a.next(0).times(b.next(0)).poly; // the first tuple of each subtask, same channel
    assertEquals("two distinct variables, not x^2", 2, product.monomials[0].numVars());
    assertEquals(2, product.degree());
    assertEquals(2, product.totalVars());
  }

  // End to end in a local Flink job: the ids minted by the parallel instances of a rich map are all distinct, equal for
  // two runs, and the same set as the ids of the p = 1 job in size.
  @Test
  public void ruleHoldsInAParallelJobAndIsDeterministic() throws Exception {
    int n = 20000;
    Map<Long, Integer> p1 = run(n, 1);
    assertEquals(n, new HashSet<>(p1.values()).size());
    for (long x = 0; x < n; x++)
      assertEquals("p = 1 ids are the sequence", (int) (x * CHANNELS + x % CHANNELS), (int) p1.get(x));
    for (int p : new int[] {2, 4}) {
      Map<Long, Integer> first = run(n, p);
      Map<Long, Integer> second = run(n, p);
      assertEquals(n, new HashSet<>(first.values()).size());
      assertEquals("deterministic", first, second);
      for (Map.Entry<Long, Integer> e : first.entrySet())
        assertEquals((int) (e.getKey() % CHANNELS), e.getValue() % CHANNELS);
    }
  }

  private static final Map<Long, Integer> SEEN = new ConcurrentHashMap<>();

  private static Map<Long, Integer> run(int n, int p) throws Exception {
    SEEN.clear();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(p);
    env.generateSequence(0, n - 1).setParallelism(p)
        .map(new Tag()).setParallelism(p)
        .addSink(new DiscardingSink<Long>()).setParallelism(p);
    env.execute("var-source-test");
    return new HashMap<>(SEEN);
  }

  /** Tags the number x with the next variable on channel x mod CHANNELS and records x -> variable id. */
  public static class Tag extends RichMapFunction<Long, Long> {
    private final VarSource vars = new VarSource(CHANNELS);

    @Override
    public void open(Configuration parameters) {
      vars.open(getRuntimeContext());
    }

    @Override
    public Long map(Long x) {
      SEEN.put(x, onlyVar(vars.next((int) (x % CHANNELS))));
      return x;
    }
  }

  private static int onlyVar(InkMeta m) {
    assertEquals(1, m.poly.monomials.length);
    assertEquals(1, m.poly.monomials[0].vars.length);
    return m.poly.monomials[0].vars[0];
  }
}
