package tieredindex;

/**
 * Result of a temporal HMP query: per-channel counts over [t1, t2].
 * isFinal: the whole range is below the watermark, so the answer can no longer change.
 * defined: at least one output exists in the range. If not, counts is all zero and the answer is bottom.
 * The share rho_c = counts[c] / sum(counts) is undefined (bottom, null) whenever the denominator is 0: no output
 * in the range, or outputs whose vectors are all zero (their counts are still defined, only the share is not).
 * Final, provisional and undefined answers of a temporal how-much query.
 */
public final class Answer {
  public final long[] counts;
  public final boolean isFinal;
  public final boolean defined;

  /** counts: per-channel how-much; isFinal: the range is below the watermark; defined: some output lies in the range. */
  public Answer(long[] counts, boolean isFinal, boolean defined) {
    this.counts = counts;
    this.isFinal = isFinal;
    this.defined = defined;
  }

  /** Per-channel share counts[c] / sum(counts), or null (undefined, bottom) if the total is 0. */
  public double[] shares() {
    long total = 0;
    for (long c : counts)
      total += c;
    if (total == 0)
      return null;
    double[] rho = new double[counts.length];
    for (int c = 0; c < counts.length; c++)
      rho[c] = (double) counts[c] / (double) total;
    return rho;
  }

  /**
   * Versioning of two intervals: absolute delta HMP = b - a and relative delta rho = rho(b) - rho(a),
   * per channel. delta and deltaRho are null (undefined, bottom) when either interval is empty
   * of outputs. deltaRho is also null when either interval has a total of 0 (the denominator of a
   * share), while delta stays defined. Final iff both answers are final.
   */
  public static final class Version {
    public final long[] delta;
    public final double[] deltaRho;
    public final boolean isFinal;
    public final boolean defined;

    public Version(Answer a, Answer b) {
      this.isFinal = a.isFinal && b.isFinal;
      this.defined = a.defined && b.defined;
      if (!defined) {
        this.delta = null;
        this.deltaRho = null;
        return;
      }
      int m = a.counts.length;
      this.delta = new long[m];
      for (int c = 0; c < m; c++)
        delta[c] = b.counts[c] - a.counts[c];
      double[] ra = a.shares();
      double[] rb = b.shares();
      if (ra == null || rb == null) {
        this.deltaRho = null;
        return;
      }
      this.deltaRho = new double[m];
      for (int c = 0; c < m; c++)
        deltaRho[c] = rb[c] - ra[c];
    }
  }
}
