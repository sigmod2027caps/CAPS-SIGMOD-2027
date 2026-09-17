package io.palyvos.provenance.usecases.twitter.provenance;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogAccumulator;
import java.util.function.Supplier;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Computes the average score per hashtag over a tumbling window.
 * Extends {@link GenealogAccumulator} so that GeneaLog provenance
 * metadata (U1/U2/N chain) is maintained automatically.
 */
public class HashtagAvgAggregateFunction
    implements
    AggregateFunction<TwitterHashtagTupleGL, HashtagAvgAggregateFunction.AvgAccumulator, TwitterHashtagTupleGL> {

  private final Supplier<ProvenanceAggregateStrategy> strategySupplier;

  public HashtagAvgAggregateFunction(
      Supplier<ProvenanceAggregateStrategy> strategySupplier) {
    this.strategySupplier = strategySupplier;
  }

  @Override
  public AvgAccumulator createAccumulator() {
    return new AvgAccumulator(strategySupplier);
  }

  @Override
  public AvgAccumulator add(TwitterHashtagTupleGL tuple, AvgAccumulator acc) {
    acc.add(tuple);
    return acc;
  }

  @Override
  public TwitterHashtagTupleGL getResult(AvgAccumulator acc) {
    return acc.getAggregatedResult();
  }

  @Override
  public AvgAccumulator merge(AvgAccumulator a, AvgAccumulator b) {
    throw new UnsupportedOperationException("Merge not supported for provenance windows");
  }

  public static class AvgAccumulator
      extends GenealogAccumulator<TwitterHashtagTupleGL, TwitterHashtagTupleGL, AvgAccumulator> {

    private String hashtag;
    private double sum;
    private int count;
    private long maxTimestamp;
    private long maxStimulus;

    public AvgAccumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
      super(strategySupplier);
    }

    @Override
    protected void doAdd(TwitterHashtagTupleGL t) {
      if (hashtag == null) {
        hashtag = t.getHashtag();
      }
      sum += t.getValue1();
      count++;
      maxTimestamp = Math.max(maxTimestamp, t.getTimestamp());
      maxStimulus = Math.max(maxStimulus, t.getStimulus());
    }

    @Override
    protected TwitterHashtagTupleGL doGetAggregatedResult() {
      double avg = count > 0 ? sum / count : 0;
      return new TwitterHashtagTupleGL(hashtag, maxTimestamp, maxStimulus, avg, 0);
    }

    @Override
    protected void doMerge(AvgAccumulator other) {
      sum += other.sum;
      count += other.count;
      maxTimestamp = Math.max(maxTimestamp, other.maxTimestamp);
      maxStimulus = Math.max(maxStimulus, other.maxStimulus);
      if (hashtag == null) {
        hashtag = other.hashtag;
      }
    }
  }
}
