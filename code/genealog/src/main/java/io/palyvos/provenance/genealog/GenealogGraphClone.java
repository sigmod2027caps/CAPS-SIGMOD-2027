package io.palyvos.provenance.genealog;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;

/**
 * Deep-clones the reachable provenance graph of one output into
 * {@link GenealogBenchmarkTuple}s, without following unrelated {@code next}
 * chains past each aggregate's U1.
 */
public final class GenealogGraphClone {

  private GenealogGraphClone() {}

  public static GenealogBenchmarkTuple cloneReachable(
      GenealogTuple root, ProvenanceAggregateStrategy strategy) {
    return cloneNode(root, new IdentityHashMap<GenealogTuple, GenealogBenchmarkTuple>(), strategy);
  }

  private static GenealogBenchmarkTuple cloneNode(
      GenealogTuple source,
      IdentityHashMap<GenealogTuple, GenealogBenchmarkTuple> seen,
      ProvenanceAggregateStrategy strategy) {
    GenealogBenchmarkTuple existing = seen.get(source);
    if (existing != null) {
      return existing;
    }
    GenealogBenchmarkTuple copy = new GenealogBenchmarkTuple(
        source.getTimestamp(), source.getStimulus(), source.getTupleType());
    copy.setUID(source.getUID());
    seen.put(source, copy);
    switch (source.getTupleType()) {
      case SOURCE:
      case META_SOURCE:
        break;
      case MAP:
        copy.setU1(cloneNode(source.getU1(), seen, strategy));
        break;
      case JOIN:
        copy.setU1(cloneNode(source.getU1(), seen, strategy));
        copy.setU2(cloneNode(source.getU2(), seen, strategy));
        break;
      case AGGREGATE:
        cloneAggregateChain(source, copy, seen, strategy);
        break;
      case REMOTE:
        copy.getGenealogData().setProvenance(
            cloneProvenance(source.getProvenance(), seen, strategy));
        break;
      default:
        throw new IllegalStateException("Invalid TupleType: " + source.getTupleType());
    }
    return copy;
  }

  private static void cloneAggregateChain(
      GenealogTuple aggregate,
      GenealogBenchmarkTuple copy,
      IdentityHashMap<GenealogTuple, GenealogBenchmarkTuple> seen,
      ProvenanceAggregateStrategy strategy) {
    Iterator<GenealogTuple> it = strategy.provenanceIterator(aggregate);
    GenealogBenchmarkTuple first = null;
    GenealogBenchmarkTuple prev = null;
    GenealogBenchmarkTuple last = null;
    while (it.hasNext()) {
      GenealogBenchmarkTuple cloned = cloneNode(it.next(), seen, strategy);
      if (first == null) {
        first = cloned;
      }
      if (prev != null) {
        prev.setNext(cloned);
      }
      last = cloned;
      prev = cloned;
    }
    if (last == null) {
      throw new IllegalStateException("empty aggregate provenance");
    }
    last.setNext(null);
    copy.setU2(first);
    copy.setU1(last);
  }

  private static Collection<TimestampedUIDTuple> cloneProvenance(
      Collection<TimestampedUIDTuple> provenance,
      IdentityHashMap<GenealogTuple, GenealogBenchmarkTuple> seen,
      ProvenanceAggregateStrategy strategy) {
    if (provenance == null) {
      return null;
    }
    ArrayList<TimestampedUIDTuple> copy = new ArrayList<TimestampedUIDTuple>(provenance.size());
    for (TimestampedUIDTuple tuple : provenance) {
      if (tuple instanceof GenealogTuple) {
        copy.add(cloneNode((GenealogTuple) tuple, seen, strategy));
      } else {
        GenealogBenchmarkTuple stub = new GenealogBenchmarkTuple(
            tuple.getTimestamp(), 0L, GenealogTupleType.SOURCE);
        stub.setUID(tuple.getUID());
        copy.add(stub);
      }
    }
    return copy;
  }
}
