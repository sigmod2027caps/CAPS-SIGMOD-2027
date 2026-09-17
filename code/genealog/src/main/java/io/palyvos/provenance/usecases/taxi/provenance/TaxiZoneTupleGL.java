package io.palyvos.provenance.usecases.taxi.provenance;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.util.BaseTuple;
import java.util.Objects;

/**
 * Reusable tuple for all stages of the taxi_2 dataflow after the source:
 * <ul>
 *   <li>Map output: (zone, value1=tip or fare, value2=0)</li>
 *   <li>Aggregate output: (zone, value1=avg, value2=0)</li>
 *   <li>Join output: (zone, value1=avgCardTip, value2=avgCashFare)</li>
 * </ul>
 */
public class TaxiZoneTupleGL extends BaseTuple implements GenealogTuple {

  private GenealogData gdata = new GenealogData();

  private int zone;
  private double value1;
  private double value2;

  public TaxiZoneTupleGL(int zone, long timestamp, long stimulus,
      double value1, double value2) {
    super(timestamp, String.valueOf(zone), stimulus);
    this.zone = zone;
    this.value1 = value1;
    this.value2 = value2;
  }

  public int getZone() {
    return zone;
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
    TaxiZoneTupleGL that = (TaxiZoneTupleGL) o;
    return zone == that.zone
        && Double.compare(that.value1, value1) == 0
        && Double.compare(that.value2, value2) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), zone, value1, value2);
  }

  @Override
  public String toString() {
    return zone + "," + getTimestamp() + "," + value1 + "," + value2;
  }
}
