package ink;

import java.io.Serializable;

// Package name is ink; the paper calls this baseline green_polynomial.
/**
 * The annotation that rides on every tuple of the polynomial-semiring
 * baseline. Analogue of {@code caps.HowMuch}: where CAPS stores one
 * {@code long} counter per channel, this stores a {@link Polynomial}
 * over tuple-level variables.
 *
 * <p><b>Variable-id encoding.</b> A source tuple on channel {@code c}
 * (the CAPS channel of that tuple: a virtual source, or a
 * {@code (source, path)} channel for {@code taxi_1}) with per-source-operator
 * sequence number {@code seq} is the singleton polynomial {@code x_v}
 * where
 * <pre>
 *   v = seq * channels + c
 * </pre>
 * so {@code c = v % channels} recovers the source. {@code seq} is a
 * monotonically increasing counter local to the operator instance that
 * first tags the tuple, starting at 0. Two tuples of the same channel
 * therefore remain distinct variables (tuple-level provenance); folding
 * {@code v % channels} is how this baseline answers the same how-much
 * question CAPS reads directly off its counters.
 *
 * <p>{@code v} is stored as an {@code int}. After roughly
 * {@code 2^31 / channels} source tuples the id wraps; that is far past
 * the point where the annotation itself exhausts the heap.
 */
public final class InkMeta implements Serializable {

  private static final long serialVersionUID = 1L;

  public Polynomial poly;

  public InkMeta() {
    this.poly = Polynomial.one();
  }

  public InkMeta(Polynomial poly) {
    this.poly = poly;
  }

  /** Semiring 1. Empty aggregators start here so that {@code 1 * x * y = xy}. */
  public static InkMeta one() {
    return new InkMeta(Polynomial.one());
  }

  /**
   * Singleton annotation of a source tuple: {@code x_{seq * channels + channel}}.
   */
  public static InkMeta source(int channel, int channels, long seq) {
    int varId = (int) (seq * (long) channels + channel);
    return new InkMeta(Polynomial.var(varId));
  }

  /**
   * Polynomial product. This is the JOIN and the AGGREGATE combination
   * rule: both are conjunctive — all contributing tuples are jointly
   * needed — so the annotation is the product of the contributors.
   *
   * <p>Green's projection (the bag-union / alternative-worlds reading of
   * an aggregation) would instead be {@link Polynomial#plus}. We
   * deliberately use {@code times} so the baseline answers the same
   * how-much question CAPS answers: each source tuple contributes one
   * exponent, and {@link #howMuch} sums those exponents per channel.
   * {@code plus} is implemented on {@link Polynomial} and exercised by
   * {@link SelfTest} even though these six dataflows never trigger it.
   */
  public InkMeta times(InkMeta other) {
    return new InkMeta(this.poly.times(other.poly));
  }

  /**
   * The per-source how-much vector. For every monomial, for every
   * variable, add {@code exponent * coefficient} into bucket
   * {@code varId % channels}. This is the query the baseline must run
   * to answer what CAPS reads directly off its counters.
   */
  public long[] howMuch(int channels) {
    long[] counts = new long[channels];
    foldHowMuch(channels, counts);
    return counts;
  }

  /**
   * Scalar fold of {@link #howMuch} without allocating a vector. Used when
   * the timed query is {@code total_contributions} so Ink does equal work
   * to CAPS {@code HowMuch#total} and GeneaLog {@code countContributions}.
   */
  public long howMuchTotal(int channels) {
    return foldHowMuch(channels, null);
  }

  private long foldHowMuch(int channels, long[] counts) {
    long total = 0;
    Monomial[] ms = poly.monomials;
    for (int i = 0; i < ms.length; i++) {
      Monomial m = ms[i];
      final int coeff = m.coefficient;
      for (int j = 0; j < m.vars.length; j++) {
        int ch = m.vars[j] % channels;
        if (ch < 0) {
          ch += channels;
        }
        long add = (long) m.exps[j] * coeff;
        if (counts != null) {
          counts[ch] += add;
        }
        total += add;
      }
    }
    return total;
  }

  public int numMonomials() {
    return poly.numMonomials();
  }

  public int totalVars() {
    return poly.totalVars();
  }

  /**
   * Analytical size of this annotation, charged the same way the paper
   * charges CAPS and GeneaLog: 8 bytes per (variable, exponent) pair,
   * i.e. {@code sum over monomials of (numVars * 8)}. A pair is two
   * ints ({@code varId}, {@code exponent}).
   *
   * <p>The monomial coefficient is not charged. {@link Monomial#coefficient}
   * is an {@code int} (4 B) and is always 1 in all six dataflows
   * ({@link Polynomial#plus} is never called and no monomial is ever
   * produced twice in a product), so a real implementer would not
   * serialize it at all. If {@code plus} were ever exercised the
   * coefficient would have to be charged at 4 B.
   */
  // Coefficient is not charged — in these dataflows it is always 1 and never serialized.
  public long metadataBytes() {
    long bytes = 0L;
    Monomial[] ms = poly.monomials;
    for (int i = 0; i < ms.length; i++) {
      bytes += (long) ms[i].numVars() * 8L;
    }
    return bytes;
  }

  /**
   * Annotations are immutable, so a "copy" is a new wrapper around the
   * same polynomial. Kept so aggregator {@code getResult} sites stay
   * parallel to the CAPS {@code HowMuch.copy()} calls.
   */
  public InkMeta copy() {
    return new InkMeta(poly);
  }

  @Override
  public String toString() {
    return poly.toString();
  }
}
