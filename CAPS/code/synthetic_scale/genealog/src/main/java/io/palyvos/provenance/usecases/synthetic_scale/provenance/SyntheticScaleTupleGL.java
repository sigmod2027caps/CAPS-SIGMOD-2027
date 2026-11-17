package io.palyvos.provenance.usecases.synthetic_scale.provenance;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.util.BaseTuple;

/**
 * Tuple of the channel-cardinality dataflow: a source record (id, sourceId, keyId, value) and
 * the output of the window (keyId, count, sum). The output prints as key,timestamp,count,sum.
 */
public class SyntheticScaleTupleGL extends BaseTuple implements GenealogTuple {

  private GenealogData gdata = new GenealogData();
  public long id;
  public int sourceId;
  public int keyId;
  public long value;
  public long count;
  public long sum;

  public SyntheticScaleTupleGL() {}

  public SyntheticScaleTupleGL(long id, long timestamp, int sourceId, int keyId, long value) {
    super(timestamp, String.valueOf(keyId), timestamp);
    this.id = id;
    this.sourceId = sourceId;
    this.keyId = keyId;
    this.value = value;
  }

  public SyntheticScaleTupleGL copyPayload() {
    SyntheticScaleTupleGL o = new SyntheticScaleTupleGL(id, getTimestamp(), sourceId, keyId, value);
    o.count = count;
    o.sum = sum;
    return o;
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
  // inserted twice, and two outputs of one window can have the same fields (same derived timestamp).
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
    return keyId + "," + getTimestamp() + "," + count + "," + sum;
  }
}
