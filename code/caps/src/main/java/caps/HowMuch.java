package caps;

import java.io.Serializable;
import java.util.Arrays;

public class HowMuch implements Serializable {

  private static final long serialVersionUID = 1L;

  public long[] counts;

  public HowMuch() {}

  public HowMuch(int channels) {
    this.counts = new long[channels];
  }

  private HowMuch(long[] counts) {
    this.counts = counts;
  }

  public static HowMuch one(int channel, int channels) {
    HowMuch hm = new HowMuch(channels);
    hm.counts[channel] = 1;
    return hm;
  }

  public static HowMuch of(long... counts) {
    return new HowMuch(counts.clone());
  }

  public HowMuch copy() {
    return new HowMuch(counts.clone());
  }

  public void mergeWith(HowMuch other) {
    for (int i = 0; i < counts.length; i++) {
      counts[i] += other.counts[i];
    }
  }

  public static HowMuch merge(HowMuch left, HowMuch right) {
    HowMuch result = left.copy();
    result.mergeWith(right);
    return result;
  }

  public long count(int channel) {
    return counts[channel];
  }

  public int channels() {
    return counts.length;
  }

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
