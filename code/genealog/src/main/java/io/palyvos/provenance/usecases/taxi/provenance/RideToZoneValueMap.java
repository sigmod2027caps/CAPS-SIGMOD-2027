package io.palyvos.provenance.usecases.taxi.provenance;

import io.palyvos.provenance.genealog.GenealogTupleType;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Projects a ride to (zone, value): tip for the card branch, fare for the
 * cash branch (taxi_2 dataflow). Each output carries GeneaLog MAP metadata
 * pointing back to the source ride.
 */
public class RideToZoneValueMap
    implements MapFunction<TaxiRideTupleGL, TaxiZoneTupleGL> {

  private final boolean card;

  public RideToZoneValueMap(boolean card) {
    this.card = card;
  }

  @Override
  public TaxiZoneTupleGL map(TaxiRideTupleGL ride) {
    TaxiZoneTupleGL result = new TaxiZoneTupleGL(
        ride.getZone(), ride.getTimestamp(), ride.getStimulus(),
        card ? ride.getTip() : ride.getFare(), 0);
    result.initGenealog(GenealogTupleType.MAP);
    result.setU1(ride);
    return result;
  }
}
