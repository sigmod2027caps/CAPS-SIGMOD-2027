package io.palyvos.provenance.usecases.twitter.provenance;

import io.palyvos.provenance.genealog.GenealogTupleType;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Splits each tweet into one {@link TwitterHashtagTupleGL} per hashtag.
 * Each output carries GeneaLog MAP metadata pointing back to the source tweet.
 */
public class HashtagScoreFlatMap
    implements FlatMapFunction<TwitterTweetTupleGL, TwitterHashtagTupleGL> {

  @Override
  public void flatMap(TwitterTweetTupleGL tweet, Collector<TwitterHashtagTupleGL> out) {
    String hashtags = tweet.getHashtags();
    if (hashtags == null || hashtags.isEmpty()) {
      return;
    }
    for (String tag : hashtags.split("\\|")) {
      if (tag.isEmpty()) {
        continue;
      }
      TwitterHashtagTupleGL result = new TwitterHashtagTupleGL(
          tag, tweet.getTimestamp(), tweet.getStimulus(), tweet.getScore(), 0);
      result.initGenealog(GenealogTupleType.MAP);
      result.setU1(tweet);
      out.collect(result);
    }
  }
}
