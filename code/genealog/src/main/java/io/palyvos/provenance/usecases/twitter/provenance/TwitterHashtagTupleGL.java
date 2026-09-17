package io.palyvos.provenance.usecases.twitter.provenance;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.util.BaseTuple;
import java.util.Objects;

/**
 * Reusable tuple for all stages after the source:
 * <ul>
 *   <li>Map output: (hashtag, value1=score, value2=0)</li>
 *   <li>Aggregate output: (hashtag, value1=avgScore, value2=0)</li>
 *   <li>Join output: (hashtag, value1=avgScoreL, value2=avgScoreR)</li>
 * </ul>
 */
public class TwitterHashtagTupleGL extends BaseTuple implements GenealogTuple {

  private GenealogData gdata = new GenealogData();

  private String hashtag;
  private double value1;
  private double value2;

  public TwitterHashtagTupleGL(String hashtag, long timestamp, long stimulus,
      double value1, double value2) {
    super(timestamp, hashtag, stimulus);
    this.hashtag = hashtag;
    this.value1 = value1;
    this.value2 = value2;
  }

  public String getHashtag() {
    return hashtag;
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
    TwitterHashtagTupleGL that = (TwitterHashtagTupleGL) o;
    return Double.compare(that.value1, value1) == 0
        && Double.compare(that.value2, value2) == 0
        && Objects.equals(hashtag, that.hashtag);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), hashtag, value1, value2);
  }

  @Override
  public String toString() {
    return hashtag + "," + getTimestamp() + "," + value1 + "," + value2;
  }
}
