package caps;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Per-tuple how-much meta-data: one counter per channel (source or path, encoded as int).
 * Instrumentation rules are in the paper; this POJO is what sinks read and merge.
 *
 * <p>Stores: one long per channel (counts[c] = contributions of channel c to
 * the tuple: its source tuples, each counted once per derivation it takes part in), 8 B per channel. Invariants: the length is the number of channels M of the dataflow and is
 * the same for every tuple; counters are non-negative. Created at the sources (one), copied by map-like
 * operators, merged by aggregates and joins (channel-wise sum). Every operation is O(M), nothing is
 * proportional to the number of contributing tuples.
 */
public class HowMuch implements Serializable {

  private static final long serialVersionUID = 1L;

  public long[] counts;

  public HowMuch() {}

  // Channel count is fixed for the whole dataflow - a mismatch here is a silent bug.
  public HowMuch(int channels) {
    this.counts = new long[channels];
  }

  private HowMuch(long[] counts) {
    this.counts = counts;
  }

  /** Source instrumentation: a fresh tuple counting 1 on the given channel. */
  public static HowMuch one(int channel, int channels) {
    HowMuch hm = new HowMuch(channels);
    hm.counts[channel] = 1;
    return hm;
  }

  /** Test and tooling constructor: a tuple with the given counters (the array is copied). */
  public static HowMuch of(long... counts) {
    return new HowMuch(counts.clone());
  }

  /** Map instrumentation: output tuple carries a copy of the input's meta-data. */
  public HowMuch copy() {
    return new HowMuch(counts.clone());
  }

  /** Aggregate instrumentation: channel-wise sum of another tuple's meta-data. */
  public void mergeWith(HowMuch other) {
    for (int i = 0; i < counts.length; i++) {
      counts[i] += other.counts[i];
    }
  }

  /** Join instrumentation: channel-wise sum of the two joined tuples' meta-data. */
  public static HowMuch merge(HowMuch left, HowMuch right) {
    HowMuch result = left.copy();
    result.mergeWith(right);
    return result;
  }

  /** Counter of one channel: its contributions to the tuple. O(1). */
  public long count(int channel) {
    return counts[channel];
  }

  /** The number of channels M. */
  public int channels() {
    return counts.length;
  }

  /** Scalar fold: sum of the existing counters, no allocation. */
  public long total() {
    long sum = 0;
    for (int i = 0; i < counts.length; i++) {
      sum += counts[i];
    }
    return sum;
  }

  /** Vector HMP materialize: a copy of the M-long channel array. */
  public long[] copyCounts() {
    return counts.clone();
  }

  /** Comma-separated counters, e.g. "40,12" - ready for CSV output lines. */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < counts.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(counts[i]);
    }
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof HowMuch)) return false;
    return Arrays.equals(counts, ((HowMuch) o).counts);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(counts);
  }
}
