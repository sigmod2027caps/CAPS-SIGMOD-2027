package io.palyvos.provenance.usecases.taxi.provenance;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.util.BaseTuple;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Source tuple of the taxi dataflows: one ride of taxis.txt
 * (taxi_id,ts_ms,passengers,zone,payment,fare,tip).
 */
public class TaxiRideTupleGL extends BaseTuple implements GenealogTuple {

  private static final Pattern COMMA = Pattern.compile(",");

  private GenealogData gdata = new GenealogData();

  private long taxiId;
  private int passengers;
  private int zone;
  private String payment;
  private double fare;
  private double tip;

  public TaxiRideTupleGL(long taxiId, long timestamp, int passengers, int zone,
      String payment, double fare, double tip) {
    super(timestamp, String.valueOf(taxiId), System.currentTimeMillis());
    this.taxiId = taxiId;
    this.passengers = passengers;
    this.zone = zone;
    this.payment = payment;
    this.fare = fare;
    this.tip = tip;
  }

  public static TaxiRideTupleGL fromReading(String line) {
    String[] tokens = COMMA.split(line.trim(), 7);
    TaxiRideTupleGL tuple = new TaxiRideTupleGL(
        Long.parseLong(tokens[0]),
        Long.parseLong(tokens[1]),
        Integer.parseInt(tokens[2]),
        Integer.parseInt(tokens[3]),
        tokens[4],
        Double.parseDouble(tokens[5]),
        Double.parseDouble(tokens[6]));
    tuple.initGenealog(GenealogTupleType.SOURCE);
    return tuple;
  }

  public long getTaxiId() { return taxiId; }
  public int getPassengers() { return passengers; }
  public int getZone() { return zone; }
  public String getPayment() { return payment; }
  public double getFare() { return fare; }
  public double getTip() { return tip; }
  public boolean isCard() { return "CRD".equals(payment); }
  public boolean isCash() { return "CSH".equals(payment); }

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
    TaxiRideTupleGL that = (TaxiRideTupleGL) o;
    return taxiId == that.taxiId && passengers == that.passengers && zone == that.zone
        && Double.compare(that.fare, fare) == 0 && Double.compare(that.tip, tip) == 0
        && Objects.equals(payment, that.payment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), taxiId, passengers, zone, payment, fare, tip);
  }

  @Override
  public String toString() {
    return taxiId + "," + getTimestamp() + "," + passengers + "," + zone + ","
        + payment + "," + fare + "," + tip;
  }
}
