package io.palyvos.provenance.usecases.taxi.provenance;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogAccumulator;
import java.util.function.Supplier;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Counts the rides of a taxi inside the window (taxi_1 dataflow).
 * Extends {@link GenealogAccumulator} so that GeneaLog provenance
 * metadata (U1/U2/N chain) is maintained automatically.
 */
public class TaxiCountAggregateFunction
    implements
    AggregateFunction<TaxiCountTupleGL, TaxiCountAggregateFunction.CountAccumulator, TaxiCountTupleGL> {

  private final Supplier<ProvenanceAggregateStrategy> strategySupplier;

  public TaxiCountAggregateFunction(
      Supplier<ProvenanceAggregateStrategy> strategySupplier) {
    this.strategySupplier = strategySupplier;
  }

  @Override
  public CountAccumulator createAccumulator() {
    return new CountAccumulator(strategySupplier);
  }

  @Override
  public CountAccumulator add(TaxiCountTupleGL tuple, CountAccumulator acc) {
    acc.add(tuple);
    return acc;
  }

  @Override
  public TaxiCountTupleGL getResult(CountAccumulator acc) {
    return acc.getAggregatedResult();
  }

  @Override
  public CountAccumulator merge(CountAccumulator a, CountAccumulator b) {
    throw new UnsupportedOperationException("Merge not supported for provenance windows");
  }

  public static class CountAccumulator
      extends GenealogAccumulator<TaxiCountTupleGL, TaxiCountTupleGL, CountAccumulator> {

    private long taxiId = -1;
    private long count;
    private long maxTimestamp;
    private long maxStimulus;

    public CountAccumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
      super(strategySupplier);
    }

    @Override
    protected void doAdd(TaxiCountTupleGL t) {
      taxiId = t.getTaxiId();
      count++;
      maxTimestamp = Math.max(maxTimestamp, t.getTimestamp());
      maxStimulus = Math.max(maxStimulus, t.getStimulus());
    }

    @Override
    protected TaxiCountTupleGL doGetAggregatedResult() {
      return new TaxiCountTupleGL(taxiId, maxTimestamp, maxStimulus, count, 0);
    }

    @Override
    protected void doMerge(CountAccumulator other) {
      count += other.count;
      maxTimestamp = Math.max(maxTimestamp, other.maxTimestamp);
      maxStimulus = Math.max(maxStimulus, other.maxStimulus);
      if (taxiId < 0) {
        taxiId = other.taxiId;
      }
    }
  }
}
