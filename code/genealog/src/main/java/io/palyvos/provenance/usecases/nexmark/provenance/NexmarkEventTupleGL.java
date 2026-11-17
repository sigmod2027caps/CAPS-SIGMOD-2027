package io.palyvos.provenance.usecases.nexmark.provenance;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.util.BaseTuple;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Source tuple of the Nexmark dataflows: a person registration from
 * persons.txt (personID,ts_ms,name,city,state) or an opened auction from
 * auctions.txt (auctionID,ts_ms,sellerID,category,itemName).
 */
public class NexmarkEventTupleGL extends BaseTuple implements GenealogTuple {

  private static final Pattern COMMA = Pattern.compile(",");

  private GenealogData gdata = new GenealogData();

  private boolean isPerson;
  private long personId;   // person: own id; auction: sellerID
  private long auctionId;  // auction only
  private String name = "";
  private String city = "";
  private String state = "";
  private int category;

  public NexmarkEventTupleGL(long timestamp, String key) {
    super(timestamp, key, System.currentTimeMillis());
  }

  public static NexmarkEventTupleGL fromPersonReading(String line) {
    String[] tokens = COMMA.split(line.trim(), 5);
    NexmarkEventTupleGL tuple =
        new NexmarkEventTupleGL(Long.parseLong(tokens[1]), tokens[0]);
    tuple.isPerson = true;
    tuple.personId = Long.parseLong(tokens[0]);
    tuple.name = tokens[2];
    tuple.city = tokens[3];
    tuple.state = tokens[4];
    tuple.initGenealog(GenealogTupleType.SOURCE);
    return tuple;
  }

  public static NexmarkEventTupleGL fromAuctionReading(String line) {
    String[] tokens = COMMA.split(line.trim(), 5);
    NexmarkEventTupleGL tuple =
        new NexmarkEventTupleGL(Long.parseLong(tokens[1]), tokens[0]);
    tuple.isPerson = false;
    tuple.auctionId = Long.parseLong(tokens[0]);
    tuple.personId = Long.parseLong(tokens[2]);
    tuple.category = Integer.parseInt(tokens[3]);
    tuple.initGenealog(GenealogTupleType.SOURCE);
    return tuple;
  }

  public boolean isPerson() { return isPerson; }
  public long getPersonId() { return personId; }
  public long getAuctionId() { return auctionId; }
  public String getName() { return name; }
  public String getCity() { return city; }
  public String getState() { return state; }
  public int getCategory() { return category; }

  public boolean inTargetState() {
    return "OR".equals(state) || "ID".equals(state) || "CA".equals(state);
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
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    NexmarkEventTupleGL that = (NexmarkEventTupleGL) o;
    return isPerson == that.isPerson && personId == that.personId
        && auctionId == that.auctionId && category == that.category
        && Objects.equals(name, that.name) && Objects.equals(city, that.city)
        && Objects.equals(state, that.state);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), isPerson, personId, auctionId,
        name, city, state, category);
  }

  @Override
  public String toString() {
    return isPerson
        ? "P," + personId + "," + getTimestamp() + "," + name + "," + city + "," + state
        : "A," + auctionId + "," + getTimestamp() + "," + personId + "," + category;
  }
}
