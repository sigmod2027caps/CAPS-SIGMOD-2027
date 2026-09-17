package io.palyvos.provenance.usecases.nexmark.provenance;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogAccumulator;
import java.util.function.Supplier;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Counts the joined auctions of a person inside the window (nexmark_1 / Q8).
 * Extends {@link GenealogAccumulator} so that GeneaLog provenance metadata
 * (U1/U2/N chain) is maintained automatically.
 */
public class NexmarkCountAggregateFunction
    implements
    AggregateFunction<NexmarkPersonCountTupleGL, NexmarkCountAggregateFunction.CountAccumulator, NexmarkPersonCountTupleGL> {

  private final Supplier<ProvenanceAggregateStrategy> strategySupplier;

  public NexmarkCountAggregateFunction(
      Supplier<ProvenanceAggregateStrategy> strategySupplier) {
    this.strategySupplier = strategySupplier;
  }

  @Override
  public CountAccumulator createAccumulator() {
    return new CountAccumulator(strategySupplier);
  }

  @Override
  public CountAccumulator add(NexmarkPersonCountTupleGL tuple, CountAccumulator acc) {
    acc.add(tuple);
    return acc;
  }

  @Override
  public NexmarkPersonCountTupleGL getResult(CountAccumulator acc) {
    return acc.getAggregatedResult();
  }

  @Override
  public CountAccumulator merge(CountAccumulator a, CountAccumulator b) {
    throw new UnsupportedOperationException("Merge not supported for provenance windows");
  }

  public static class CountAccumulator
      extends GenealogAccumulator<NexmarkPersonCountTupleGL, NexmarkPersonCountTupleGL, CountAccumulator> {

    private long personId = -1;
    private long count;
    private long maxTimestamp;
    private long maxStimulus;

    public CountAccumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
      super(strategySupplier);
    }

    @Override
    protected void doAdd(NexmarkPersonCountTupleGL t) {
      personId = t.getPersonId();
      count++;
      maxTimestamp = Math.max(maxTimestamp, t.getTimestamp());
      maxStimulus = Math.max(maxStimulus, t.getStimulus());
    }

    @Override
    protected NexmarkPersonCountTupleGL doGetAggregatedResult() {
      return new NexmarkPersonCountTupleGL(personId, maxTimestamp, maxStimulus, count, 0);
    }

    @Override
    protected void doMerge(CountAccumulator other) {
      count += other.count;
      maxTimestamp = Math.max(maxTimestamp, other.maxTimestamp);
      maxStimulus = Math.max(maxStimulus, other.maxStimulus);
      if (personId < 0) {
        personId = other.personId;
      }
    }
  }
}
