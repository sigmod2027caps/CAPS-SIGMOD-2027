package io.palyvos.provenance.genealog;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import java.util.ArrayDeque;
import java.util.Iterator;

/**
 * BFS how-much query over one output's GeneaLog provenance graph.
 * Shared by the capture sink and the JMH benchmark.
 */
public final class GenealogContributionQuery {

  private GenealogContributionQuery() {}

  public static long count(GenealogTuple start, ProvenanceAggregateStrategy aggregateStrategy) {
    long count = 0;
    ArrayDeque<GenealogTuple> queue = new ArrayDeque<GenealogTuple>();
    queue.addLast(start);
    while (!queue.isEmpty()) {
      GenealogTuple t = queue.removeFirst();
      switch (t.getTupleType()) {
        case SOURCE:
          count++;
          break;
        case MAP:
          queue.addLast(t.getU1());
          break;
        case JOIN:
          queue.addLast(t.getU1());
          queue.addLast(t.getU2());
          break;
        case AGGREGATE:
          Iterator<GenealogTuple> it = aggregateStrategy.provenanceIterator(t);
          while (it.hasNext()) {
            queue.addLast(it.next());
          }
          break;
        case REMOTE:
          count += t.getProvenance().size();
          break;
        case META_SOURCE:
          count++;
          break;
        default:
          throw new IllegalStateException("Invalid TupleType: " + t.getTupleType());
      }
    }
    return count;
  }
}
