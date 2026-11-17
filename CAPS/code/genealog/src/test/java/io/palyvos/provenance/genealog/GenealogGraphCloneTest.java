package io.palyvos.provenance.genealog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.palyvos.provenance.ananke.aggregate.SortedPointersAggregateStrategy;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import org.junit.Test;

public class GenealogGraphCloneTest {

  @Test
  public void cloneMatchesTraversalAnswersForEachTupleType() {
    SortedPointersAggregateStrategy strategy = new SortedPointersAggregateStrategy();

    GenealogTuple source = typed(GenealogTupleType.SOURCE, 1L);
    assertSameAnswer(source, strategy);

    GenealogTuple meta = typed(GenealogTupleType.META_SOURCE, 2L);
    assertSameAnswer(meta, strategy);

    GenealogTuple mapped = typed(GenealogTupleType.MAP, 3L);
    mapped.setU1(source);
    assertSameAnswer(mapped, strategy);

    GenealogTuple joined = typed(GenealogTupleType.JOIN, 4L);
    joined.setU1(mapped);
    joined.setU2(meta);
    assertSameAnswer(joined, strategy);

    GenealogTuple s1 = typed(GenealogTupleType.SOURCE, 10L);
    GenealogTuple s2 = typed(GenealogTupleType.SOURCE, 11L);
    GenealogTuple s3 = typed(GenealogTupleType.SOURCE, 12L);
    s1.setNext(s2);
    s2.setNext(s3);
    GenealogTuple agg = typed(GenealogTupleType.AGGREGATE, 13L);
    agg.setU2(s1);
    agg.setU1(s3);
    assertSameAnswer(agg, strategy);

    GenealogTuple remote = typed(GenealogTupleType.REMOTE, 20L);
    ArrayList<TimestampedUIDTuple> proven = new ArrayList<TimestampedUIDTuple>();
    proven.add(typed(GenealogTupleType.SOURCE, 21L));
    proven.add(typed(GenealogTupleType.SOURCE, 22L));
    remote.getGenealogData().setProvenance(proven);
    assertSameAnswer(remote, strategy);
  }

  @Test
  public void sharedSubgraphClonePreservesIdentityAndCountAcrossSerialization() {
    SortedPointersAggregateStrategy strategy = new SortedPointersAggregateStrategy();
    GenealogTuple shared = typed(GenealogTupleType.SOURCE, 1L);
    GenealogTuple left = typed(GenealogTupleType.MAP, 2L);
    left.setU1(shared);
    GenealogTuple right = typed(GenealogTupleType.MAP, 3L);
    right.setU1(shared);
    GenealogTuple join = typed(GenealogTupleType.JOIN, 4L);
    join.setU1(left);
    join.setU2(right);

    assertEquals(4, uniqueCount(join));
    assertEquals(2L, GenealogContributionQuery.count(join, strategy));

    GenealogBenchmarkTuple clone = GenealogGraphClone.cloneReachable(join, strategy);
    assertEquals(4, uniqueCount(clone));
    assertTrue(clone.getU1().getU1() == clone.getU2().getU1());
    assertEquals(2L, GenealogContributionQuery.count(clone, strategy));

    GenealogBenchmarkTuple restored = serializeRoundTrip(clone);
    assertEquals(4, uniqueCount(restored));
    assertTrue(restored.getU1().getU1() == restored.getU2().getU1());
    assertEquals(2L, GenealogContributionQuery.count(restored, strategy));
  }

  @Test
  public void cloneDropsUnrelatedNextBeyondAggregateU1() throws Exception {
    SortedPointersAggregateStrategy strategy = new SortedPointersAggregateStrategy();
    GenealogTuple s1 = typed(GenealogTupleType.SOURCE, 1L);
    GenealogTuple s2 = typed(GenealogTupleType.SOURCE, 2L);
    GenealogTuple u1 = typed(GenealogTupleType.SOURCE, 3L);
    GenealogTuple unrelated = typed(GenealogTupleType.SOURCE, 999_001L);
    s1.setNext(s2);
    s2.setNext(u1);
    u1.setNext(unrelated);

    GenealogTuple agg = typed(GenealogTupleType.AGGREGATE, 4L);
    agg.setU2(s1);
    agg.setU1(u1);

    long live = GenealogContributionQuery.count(agg, strategy);
    assertEquals(3L, live);

    GenealogBenchmarkTuple clone = GenealogGraphClone.cloneReachable(agg, strategy);
    assertEquals(live, GenealogContributionQuery.count(clone, strategy));
    assertNull(clone.getU1().getNext());

    GenealogBenchmarkTuple restored = serializeRoundTrip(clone);
    assertEquals(live, GenealogContributionQuery.count(restored, strategy));
    assertNull(restored.getU1().getNext());
    assertTrue("serialized clone must not retain the unrelated next tuple",
        !containsTimestamp(restored, 999_001L));
  }

  private static void assertSameAnswer(GenealogTuple root,
      SortedPointersAggregateStrategy strategy) {
    long expected = GenealogContributionQuery.count(root, strategy);
    GenealogBenchmarkTuple clone = GenealogGraphClone.cloneReachable(root, strategy);
    assertEquals(expected, GenealogContributionQuery.count(clone, strategy));
    GenealogBenchmarkTuple restored = serializeRoundTrip(clone);
    assertEquals(expected, GenealogContributionQuery.count(restored, strategy));
  }

  private static int uniqueCount(GenealogTuple root) {
    java.util.IdentityHashMap<GenealogTuple, Boolean> seen =
        new java.util.IdentityHashMap<GenealogTuple, Boolean>();
    ArrayList<GenealogTuple> stack = new ArrayList<GenealogTuple>();
    stack.add(root);
    while (!stack.isEmpty()) {
      GenealogTuple t = stack.remove(stack.size() - 1);
      if (seen.put(t, Boolean.TRUE) != null) {
        continue;
      }
      if (t.getU1() != null) {
        stack.add(t.getU1());
      }
      if (t.getU2() != null) {
        stack.add(t.getU2());
      }
      if (t.getNext() != null) {
        stack.add(t.getNext());
      }
    }
    return seen.size();
  }

  private static boolean containsTimestamp(GenealogTuple root, long ts) {
    ArrayList<GenealogTuple> stack = new ArrayList<GenealogTuple>();
    stack.add(root);
    while (!stack.isEmpty()) {
      GenealogTuple t = stack.remove(stack.size() - 1);
      if (t.getTimestamp() == ts) {
        return true;
      }
      if (t.getU1() != null) {
        stack.add(t.getU1());
      }
      if (t.getU2() != null) {
        stack.add(t.getU2());
      }
      if (t.getNext() != null) {
        stack.add(t.getNext());
      }
    }
    return false;
  }

  private static GenealogBenchmarkTuple serializeRoundTrip(GenealogBenchmarkTuple tuple) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(bytes);
      out.writeObject(tuple);
      out.close();
      ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()));
      try {
        return (GenealogBenchmarkTuple) in.readObject();
      } finally {
        in.close();
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  static GenealogTuple typed(GenealogTupleType type, long ts) {
    GenealogContributionQueryTest.Stub t = new GenealogContributionQueryTest.Stub();
    t.initGenealog(type);
    t.setTimestamp(ts);
    return t;
  }
}
