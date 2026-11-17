package io.palyvos.provenance.usecases.taxi.provenance;

import io.palyvos.provenance.genealog.GenealogTupleType;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Projects a ride to a per-taxi count of 1 (taxi_1 dataflow).
 * Each output carries GeneaLog MAP metadata pointing back to the source ride.
 */
public class RideToTaxiCountMap
    implements MapFunction<TaxiRideTupleGL, TaxiCountTupleGL> {

  @Override
  public TaxiCountTupleGL map(TaxiRideTupleGL ride) {
    TaxiCountTupleGL result = new TaxiCountTupleGL(
        ride.getTaxiId(), ride.getTimestamp(), ride.getStimulus(), 1, 0);
    result.initGenealog(GenealogTupleType.MAP);
    result.setU1(ride);
    return result;
  }
}
