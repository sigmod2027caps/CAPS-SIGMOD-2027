package io.palyvos.provenance.usecases.twitter.provenance;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.util.BaseTuple;
import java.util.Objects;
import java.util.regex.Pattern;

public class TwitterTweetTupleGL extends BaseTuple implements GenealogTuple {

  private static final Pattern COMMA = Pattern.compile(",");

  private GenealogData gdata = new GenealogData();

  private long tweetId;
  private String hashtags;
  private int likes;
  private int retweets;
  private boolean isVerified;
  private boolean isBlueVerified;
  private String location;

  public TwitterTweetTupleGL(long tweetId, long timestamp, String hashtags, int likes,
      int retweets, boolean isVerified, boolean isBlueVerified, String location) {
    super(timestamp, String.valueOf(tweetId), System.currentTimeMillis());
    this.tweetId = tweetId;
    this.hashtags = hashtags;
    this.likes = likes;
    this.retweets = retweets;
    this.isVerified = isVerified;
    this.isBlueVerified = isBlueVerified;
    this.location = location;
  }

  public static TwitterTweetTupleGL fromReading(String line) {
    String[] tokens = COMMA.split(line.trim(), 8);
    long tweetId = Long.parseLong(tokens[0]);
    long timestamp = Long.parseLong(tokens[1]);
    String hashtags = tokens[2];
    int likes = Integer.parseInt(tokens[3]);
    int retweets = Integer.parseInt(tokens[4]);
    boolean isVerified = Boolean.parseBoolean(tokens[5]);
    boolean isBlueVerified = Boolean.parseBoolean(tokens[6]);
    String location = tokens.length > 7 ? tokens[7] : "";
    TwitterTweetTupleGL tuple = new TwitterTweetTupleGL(tweetId, timestamp, hashtags, likes,
        retweets, isVerified, isBlueVerified, location);
    tuple.initGenealog(GenealogTupleType.SOURCE);
    return tuple;
  }

  public long getTweetId() { return tweetId; }
  public String getHashtags() { return hashtags; }
  public int getLikes() { return likes; }
  public int getRetweets() { return retweets; }
  public int getScore() { return likes + retweets; }
  public boolean isVerified() { return isVerified || isBlueVerified; }
  public boolean hasLocation() { return location != null && !location.isEmpty(); }
  public String getLocation() { return location; }

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
    TwitterTweetTupleGL that = (TwitterTweetTupleGL) o;
    return tweetId == that.tweetId && likes == that.likes && retweets == that.retweets
        && isVerified == that.isVerified && isBlueVerified == that.isBlueVerified
        && Objects.equals(hashtags, that.hashtags)
        && Objects.equals(location, that.location);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), tweetId, hashtags, likes, retweets,
        isVerified, isBlueVerified, location);
  }

  @Override
  public String toString() {
    return tweetId + "," + getTimestamp() + "," + hashtags + "," + likes + "," + retweets
        + "," + isVerified + "," + isBlueVerified + "," + location;
  }
}
