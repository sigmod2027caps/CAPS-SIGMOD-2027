package io.palyvos.provenance.usecases.synthetic_scale.provenance;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogAccumulator;
import java.util.function.Supplier;
import org.apache.flink.api.common.functions.AggregateFunction;

/** Windowed count and sum; the GeneaLog accumulator keeps the pointer chain of the window. */
public class SyntheticScaleAggregateFunction implements
    AggregateFunction<SyntheticScaleTupleGL, SyntheticScaleAggregateFunction.Acc, SyntheticScaleTupleGL> {

  private final Supplier<ProvenanceAggregateStrategy> strategySupplier;

  public SyntheticScaleAggregateFunction(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
    this.strategySupplier = strategySupplier;
  }

  @Override
  public Acc createAccumulator() {
    return new Acc(strategySupplier);
  }

  @Override
  public Acc add(SyntheticScaleTupleGL value, Acc acc) {
    acc.add(value);
    return acc;
  }

  @Override
  public SyntheticScaleTupleGL getResult(Acc acc) {
    return acc.getAggregatedResult();
  }

  @Override
  public Acc merge(Acc a, Acc b) {
    throw new UnsupportedOperationException("Merge not supported for provenance windows");
  }

  public static class Acc
      extends GenealogAccumulator<SyntheticScaleTupleGL, SyntheticScaleTupleGL, Acc> {

    private int keyId;
    private long count;
    private long sum;
    private long maxStimulus;

    public Acc(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
      super(strategySupplier);
    }

    @Override
    protected void doAdd(SyntheticScaleTupleGL t) {
      keyId = t.keyId;
      count++;
      sum += t.value;
      maxStimulus = Math.max(maxStimulus, t.getStimulus());
    }

    @Override
    protected SyntheticScaleTupleGL doGetAggregatedResult() {
      // Placeholder timestamp 0: StampDerivedTime sets the window end after the operator.
      SyntheticScaleTupleGL out = new SyntheticScaleTupleGL(0, 0, 0, keyId, 0);
      out.count = count;
      out.sum = sum;
      out.setStimulus(maxStimulus);
      return out;
    }
  }
}
