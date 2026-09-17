package io.palyvos.provenance.usecases.nexmark.provenance;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.util.BaseTuple;
import java.util.Objects;

/**
 * Tuple of the nexmark_1 (Q8) dataflow after the join:
 * <ul>
 *   <li>Join output: (personId, value1=1, value2=0) per joined auction</li>
 *   <li>Aggregate output: (personId, value1=nAuctions, value2=0)</li>
 * </ul>
 */
public class NexmarkPersonCountTupleGL extends BaseTuple implements GenealogTuple {

  private GenealogData gdata = new GenealogData();

  private long personId;
  private double value1;
  private double value2;

  public NexmarkPersonCountTupleGL(long personId, long timestamp, long stimulus,
      double value1, double value2) {
    super(timestamp, String.valueOf(personId), stimulus);
    this.personId = personId;
    this.value1 = value1;
    this.value2 = value2;
  }

  public long getPersonId() {
    return personId;
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
    NexmarkPersonCountTupleGL that = (NexmarkPersonCountTupleGL) o;
    return personId == that.personId
        && Double.compare(that.value1, value1) == 0
        && Double.compare(that.value2, value2) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), personId, value1, value2);
  }

  @Override
  public String toString() {
    return personId + "," + getTimestamp() + "," + value1 + "," + value2;
  }
}
