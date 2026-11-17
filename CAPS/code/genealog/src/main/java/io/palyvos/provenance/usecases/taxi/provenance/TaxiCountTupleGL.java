package io.palyvos.provenance.usecases.taxi.provenance;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.util.BaseTuple;

/**
 * Reusable tuple for all stages of the taxi_1 dataflow after the source:
 * <ul>
 *   <li>Map output: (taxiId, value1=1, value2=0)</li>
 *   <li>Aggregate output: (taxiId, value1=rideCount, value2=0)</li>
 *   <li>Join output: (taxiId, value1=soloCount, value2=crowdedCount)</li>
 * </ul>
 */
public class TaxiCountTupleGL extends BaseTuple implements GenealogTuple {

  private GenealogData gdata = new GenealogData();

  private long taxiId;
  private double value1;
  private double value2;

  public TaxiCountTupleGL(long taxiId, long timestamp, long stimulus,
      double value1, double value2) {
    super(timestamp, String.valueOf(taxiId), stimulus);
    this.taxiId = taxiId;
    this.value1 = value1;
    this.value2 = value2;
  }

  public long getTaxiId() {
    return taxiId;
  }

  public double getValue1() {
    return value1;
  }

  public double getValue2() {
    return value2;
  }

  @Override
  public GenealogData getGenealogData() {
    return gdata;
  }

  @Override
  public void initGenealog(GenealogTupleType tupleType) {
    gdata = new GenealogData();
    gdata.init(tupleType);
  }

  @Override
  public long getUID() {
    return gdata.getUID();
  }

  @Override
  public void setUID(long uid) {
    gdata.setUID(uid);
  }

  // Identity, not field equality: SortedPointersAggregateStrategy takes equal tuples for one tuple
  // inserted twice. Two joined or mapped tuples with the same fields (same derived timestamp, same
  // stimulus millisecond) are distinct contributors and must both stay in the window's chain.
  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }

  @Override
  public String toString() {
    return taxiId + "," + getTimestamp() + "," + value1 + "," + value2;
  }
}
