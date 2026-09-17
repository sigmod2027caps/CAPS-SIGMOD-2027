package io.palyvos.provenance.genealog;

import java.io.Serializable;

/**
 * Minimal serializable GeneaLog tuple used in query fixtures and JMH.
 */
public final class GenealogBenchmarkTuple implements GenealogTuple, Serializable {

  private static final long serialVersionUID = 1L;

  private long timestamp;
  private long stimulus;
  private GenealogData gdata;

  public GenealogBenchmarkTuple() {
    this.gdata = new GenealogData();
  }

  public GenealogBenchmarkTuple(long timestamp, long stimulus, GenealogTupleType type) {
    this.timestamp = timestamp;
    this.stimulus = stimulus;
    this.gdata = new GenealogData();
    this.gdata.init(type);
  }

  @Override
  public void initGenealog(GenealogTupleType tupleType) {
    gdata = new GenealogData();
    gdata.init(tupleType);
  }

  @Override
  public GenealogData getGenealogData() {
    return gdata;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public long getStimulus() {
    return stimulus;
  }

  @Override
  public void setStimulus(long stimulus) {
    this.stimulus = stimulus;
  }
}
