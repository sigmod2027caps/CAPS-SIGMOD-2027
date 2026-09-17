package io.palyvos.provenance.usecases.taxi.provenance;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogAccumulator;
import java.util.function.Supplier;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Computes the average value (tip or fare) per zone over the window
 * (taxi_2 dataflow). Extends {@link GenealogAccumulator} so that GeneaLog
 * provenance metadata (U1/U2/N chain) is maintained automatically.
 */
public class ZoneAvgAggregateFunction
    implements
    AggregateFunction<TaxiZoneTupleGL, ZoneAvgAggregateFunction.AvgAccumulator, TaxiZoneTupleGL> {

  private final Supplier<ProvenanceAggregateStrategy> strategySupplier;

  public ZoneAvgAggregateFunction(
      Supplier<ProvenanceAggregateStrategy> strategySupplier) {
    this.strategySupplier = strategySupplier;
  }

  @Override
  public AvgAccumulator createAccumulator() {
    return new AvgAccumulator(strategySupplier);
  }

  @Override
  public AvgAccumulator add(TaxiZoneTupleGL tuple, AvgAccumulator acc) {
    acc.add(tuple);
    return acc;
  }

  @Override
  public TaxiZoneTupleGL getResult(AvgAccumulator acc) {
    return acc.getAggregatedResult();
  }

  @Override
  public AvgAccumulator merge(AvgAccumulator a, AvgAccumulator b) {
    throw new UnsupportedOperationException("Merge not supported for provenance windows");
  }

  public static class AvgAccumulator
      extends GenealogAccumulator<TaxiZoneTupleGL, TaxiZoneTupleGL, AvgAccumulator> {

    private int zone = -1;
    private double sum;
    private long count;
    private long maxTimestamp;
    private long maxStimulus;

    public AvgAccumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
      super(strategySupplier);
    }

    @Override
    protected void doAdd(TaxiZoneTupleGL t) {
      zone = t.getZone();
      sum += t.getValue1();
      count++;
      maxTimestamp = Math.max(maxTimestamp, t.getTimestamp());
      maxStimulus = Math.max(maxStimulus, t.getStimulus());
    }

    @Override
    protected TaxiZoneTupleGL doGetAggregatedResult() {
      double avg = count > 0 ? sum / count : 0;
      return new TaxiZoneTupleGL(zone, maxTimestamp, maxStimulus, avg, 0);
    }

    @Override
    protected void doMerge(AvgAccumulator other) {
      sum += other.sum;
      count += other.count;
      maxTimestamp = Math.max(maxTimestamp, other.maxTimestamp);
      maxStimulus = Math.max(maxStimulus, other.maxStimulus);
      if (zone < 0) {
        zone = other.zone;
      }
    }
  }
}
