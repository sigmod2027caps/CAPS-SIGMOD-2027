package io.palyvos.provenance.usecases.nexmark.provenance;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.util.BaseTuple;
import java.util.Objects;

/**
 * Output tuple of the nexmark_2 (Q3) dataflow:
 * (personId, name, city, state, auctionId).
 */
public class NexmarkSuggestionTupleGL extends BaseTuple implements GenealogTuple {

  private GenealogData gdata = new GenealogData();

  private long personId;
  private String name;
  private String city;
  private String state;
  private long auctionId;

  public NexmarkSuggestionTupleGL(long personId, long timestamp, long stimulus,
      String name, String city, String state, long auctionId) {
    super(timestamp, String.valueOf(personId), stimulus);
    this.personId = personId;
    this.name = name;
    this.city = city;
    this.state = state;
    this.auctionId = auctionId;
  }

  public long getPersonId() {
    return personId;
  }

  public long getAuctionId() {
    return auctionId;
  }

  public String getName() {
    return name;
  }

  public String getCity() {
    return city;
  }

  public String getState() {
    return state;
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
    NexmarkSuggestionTupleGL that = (NexmarkSuggestionTupleGL) o;
    return personId == that.personId && auctionId == that.auctionId
        && Objects.equals(name, that.name) && Objects.equals(city, that.city)
        && Objects.equals(state, that.state);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), personId, name, city, state, auctionId);
  }

  @Override
  public String toString() {
    return personId + "," + getTimestamp() + "," + name + "," + city + "," + state
        + "," + auctionId;
  }
}
