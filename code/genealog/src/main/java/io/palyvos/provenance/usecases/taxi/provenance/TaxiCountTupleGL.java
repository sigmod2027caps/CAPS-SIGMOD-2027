package io.palyvos.provenance.usecases.taxi.provenance;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.util.BaseTuple;
import java.util.Objects;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TaxiCountTupleGL that = (TaxiCountTupleGL) o;
    return taxiId == that.taxiId
        && Double.compare(that.value1, value1) == 0
        && Double.compare(that.value2, value2) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), taxiId, value1, value2);
  }

  @Override
  public String toString() {
    return taxiId + "," + getTimestamp() + "," + value1 + "," + value2;
  }
}
