package io.palyvos.provenance.genealog;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.TaggedUnion;
import temporalindex.LiveState;
import temporalindex.LiveStateSink;

/**
 * Tail sink of the live_memory mode. Besides publishing the live-state registry (LiveStateSink), it counts
 * the provenance-graph nodes that are reachable from the outputs: GeneaLog keeps the whole contribution graph alive
 * through its pointers (U1, U2, next), so every node that an output can reach must be retained to answer a query
 * about it. Each output is traversed once with a JVM-wide identity-visited set (a node reached again is skipped),
 * so the set is the retained graph and its size is the number of nodes. The traversal follows the same pointers as
 * the query (GenealogGraphTraverser): MAP -> U1, JOIN -> U1 and U2, AGGREGATE -> the strategy's iterator.
 *
 * Across a network exchange (every keyBy, also at parallelism 1) GeneaLog's serializer flattens the graph of a
 * tuple into the set of its source tuples; the receiver gets a REMOTE tuple that holds that set (a deserialized copy
 * per tuple, not shared). A REMOTE tuple is counted as one node plus one node per set entry.
 *
 * GeneaLog's side of the peak live-state measurement (experiments/live_memory).
 */
public class GenealogLiveSink<T extends GenealogTuple> extends LiveStateSink<T> {

  private static final long serialVersionUID = 1L;

  /** GenealogData of a tuple: uid, U1, U2, next at 8 B. */
  public static final long TUPLE_BYTES = 32;
  /** State of one aggregate window (SortedPointers strategy): the two references earliest and latest. */
  public static final long AGGREGATE_WINDOW_BYTES = 16;

  static final Set<GenealogTuple> VISITED = Collections.newSetFromMap(new IdentityHashMap<GenealogTuple, Boolean>());
  static long remoteEntries;

  /** The tuple behind a window element: itself, or the left or right tuple of a window-join element. */
  static GenealogTuple tuple(Object o) {
    if (o instanceof TaggedUnion)
      o = ((TaggedUnion<?, ?>) o).isOne() ? ((TaggedUnion<?, ?>) o).getOne() : ((TaggedUnion<?, ?>) o).getTwo();
    return (GenealogTuple) o;
  }

  /** The GenealogData of a tuple: uid, U1, U2, next at 8 B. */
  public static final LiveState.Sizer<Object> TUPLE_ONLY = new LiveState.Sizer<Object>() {
    private static final long serialVersionUID = 1L;

    @Override
    public long bytes(Object o) {
      return TUPLE_BYTES;
    }
  };

  /**
   * The provenance set of a REMOTE tuple. Across a network exchange GeneaLog replaces the graph of a tuple by the set of its
   * source tuples; the receiving tuple holds that set (a copy per tuple), each entry a tuple with its own GenealogData.
   */
  public static final LiveState.Sizer<Object> REMOTE_SET = new LiveState.Sizer<Object>() {
    private static final long serialVersionUID = 1L;

    @Override
    public long bytes(Object o) {
      GenealogTuple t = tuple(o);
      return t.getTupleType() == GenealogTupleType.REMOTE ? TUPLE_BYTES * t.getProvenance().size() : 0;
    }
  };

  /** A tuple and, if it is REMOTE, its provenance set. */
  public static final LiveState.Sizer<Object> TUPLE_SIZER = new LiveState.Sizer<Object>() {
    private static final long serialVersionUID = 1L;

    @Override
    public long bytes(Object o) {
      return TUPLE_BYTES + REMOTE_SET.bytes(o);
    }
  };

  static final long[] BY_TYPE = new long[GenealogTupleType.values().length];

  final ExperimentSettings settings;
  transient ProvenanceAggregateStrategy strategy;
  transient ArrayDeque<GenealogTuple> stack;

  public GenealogLiveSink(ExperimentSettings settings) {
    this.settings = settings;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    strategy = settings.aggregateStrategySupplier().get();
    stack = new ArrayDeque<>();
  }

  @Override
  protected void observe(T output) {
    synchronized (VISITED) {
      push(output);
      while (!stack.isEmpty()) {
        GenealogTuple t = stack.pop();
        BY_TYPE[t.getTupleType().ordinal()]++;
        switch (t.getTupleType()) {
          case SOURCE:
            break;
          case MAP:
            push(t.getU1());
            break;
          case JOIN:
            push(t.getU1());
            push(t.getU2());
            break;
          case AGGREGATE:
            for (java.util.Iterator<GenealogTuple> it = strategy.provenanceIterator(t); it.hasNext();)
              push(it.next());
            break;
          case REMOTE:
            remoteEntries += t.getProvenance().size();
            break;
          default:
            throw new IllegalStateException("tuple type " + t.getTupleType());
        }
      }
    }
  }

  void push(GenealogTuple t) {
    if (t == null)
      throw new IllegalArgumentException("null pointer in the provenance graph");
    if (VISITED.add(t))
      stack.push(t);
  }

  @Override
  protected void extras(Map<String, Long> out) {
    synchronized (VISITED) {
      out.put("genealog_reachable_nodes", (long) VISITED.size() + remoteEntries);
      out.put("genealog_reachable_remote_entries", remoteEntries);
      for (GenealogTupleType type : GenealogTupleType.values())
        out.put("genealog_reachable_" + type.name().toLowerCase(), BY_TYPE[type.ordinal()]);
    }
  }
}
