package io.palyvos.provenance.usecases.nexmark.provenance;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.util.BaseTuple;

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
    return personId + "," + getTimestamp() + "," + value1 + "," + value2;
  }
}
