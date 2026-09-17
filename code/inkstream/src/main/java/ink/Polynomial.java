package ink;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

/**
 * A polynomial: a sum of {@link Monomial}s. This is the provenance semiring
 * of Green, Karvounarakis and Tannen, as used by Langhi et al. (PVLDB 2025)
 * for inconsistency annotations — except that their {@code times(Polynomial)}
 * does not distribute (it multiplies each factor onto an accumulator one
 * monomial at a time, which is not the pairwise product) and their
 * {@code plus} uses {@code Stream.toList()}, which is Java 16+. Both are
 * implemented correctly here, on Java 8, over a monomial array rather than
 * a {@code List}.
 *
 * <p>{@code plus} concatenates and combines like terms by summing
 * coefficients. {@code times} is the full distributive product: every
 * monomial of {@code this} times every monomial of {@code that}, then
 * like terms are combined.
 */
public final class Polynomial implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final Monomial[] NONE = new Monomial[0];

  public Monomial[] monomials;

  public Polynomial() {
    this.monomials = new Monomial[] {Monomial.one()};
  }

  Polynomial(Monomial[] monomials) {
    this.monomials = monomials;
  }

  /** Semiring 1: the constant polynomial 1. */
  public static Polynomial one() {
    return new Polynomial(new Monomial[] {Monomial.one()});
  }

  /** Singleton {@code x_varId}. */
  public static Polynomial var(int varId) {
    return new Polynomial(new Monomial[] {Monomial.var(varId)});
  }

  /**
   * Sum of two polynomials. Like monomials (same variables and exponents)
   * have their coefficients added; a resulting coefficient of zero drops
   * the term.
   */
  public Polynomial plus(Polynomial that) {
    Monomial[] raw = new Monomial[this.monomials.length + that.monomials.length];
    System.arraycopy(this.monomials, 0, raw, 0, this.monomials.length);
    System.arraycopy(that.monomials, 0, raw, this.monomials.length, that.monomials.length);
    return new Polynomial(combineLike(raw));
  }

  /**
   * Distributive product. Langhi et al.'s {@code Polynomial.times(Polynomial)}
   * folds each monomial onto a running polynomial with {@code times(Monomial)},
   * which is not the Cartesian product of the two sums; we do the pairwise
   * product and then combine like terms.
   */
  public Polynomial times(Polynomial that) {
    final int n = this.monomials.length;
    final int m = that.monomials.length;
    if (n == 0 || m == 0) {
      return new Polynomial(NONE);
    }
    // Join multiplies annotations; the product can blow up to n*m monomials before like terms merge.
    Monomial[] raw = new Monomial[n * m];
    int k = 0;
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < m; j++) {
        raw[k++] = this.monomials[i].times(that.monomials[j]);
      }
    }
    return new Polynomial(combineLike(raw));
  }

  public int numMonomials() {
    return monomials.length;
  }

  /** Sum of {@link Monomial#numVars()} over every monomial (not distinct vars). */
  public int totalVars() {
    int n = 0;
    for (int i = 0; i < monomials.length; i++) {
      n += monomials[i].numVars();
    }
    return n;
  }

  /** Maximum monomial degree; 0 for the empty (zero) polynomial. */
  public int degree() {
    int d = 0;
    for (int i = 0; i < monomials.length; i++) {
      int md = monomials[i].degree();
      if (md > d) {
        d = md;
      }
    }
    return d;
  }

  /**
   * Langhi-style sum of monomials, e.g. {@code 2<7>^2+<9>^1}.
   */
  @Override
  public String toString() {
    if (monomials.length == 0) {
      return "0";
    }
    StringBuilder sb = new StringBuilder();
    sb.append(monomials[0]);
    for (int i = 1; i < monomials.length; i++) {
      sb.append('+').append(monomials[i]);
    }
    return sb.toString();
  }

  /**
   * Sort by {@code (vars, exps)} and merge adjacent like terms. Zero
   * coefficients are dropped.
   */
  // We keep the monomials sorted, becasue the equality test is then linear.
  static Monomial[] combineLike(Monomial[] raw) {
    if (raw.length == 0) {
      return NONE;
    }
    if (raw.length == 1) {
      return raw[0].coefficient == 0 ? NONE : raw;
    }
    Arrays.sort(raw, new Comparator<Monomial>() {
      @Override
      public int compare(Monomial a, Monomial b) {
        return Monomial.compareLike(a, b);
      }
    });
    Monomial[] tmp = new Monomial[raw.length];
    int w = 0;
    Monomial cur = raw[0];
    int coeff = cur.coefficient;
    for (int i = 1; i < raw.length; i++) {
      if (cur.equals(raw[i])) {
        coeff += raw[i].coefficient;
      } else {
        if (coeff != 0) {
          tmp[w++] = coeff == cur.coefficient
              ? cur
              : new Monomial(cur.vars, cur.exps, coeff);
        }
        cur = raw[i];
        coeff = cur.coefficient;
      }
    }
    if (coeff != 0) {
      tmp[w++] = coeff == cur.coefficient
          ? cur
          : new Monomial(cur.vars, cur.exps, coeff);
    }
    if (w == 0) {
      return NONE;
    }
    if (w == tmp.length) {
      return tmp;
    }
    return Arrays.copyOf(tmp, w);
  }
}
