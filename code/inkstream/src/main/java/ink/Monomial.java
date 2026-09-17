package ink;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A single monomial over integer variable ids: {@code c * x_i^e_i * x_j^e_j * ...}.
 *
 * <p>Representation is two parallel primitive arrays kept sorted by variable
 * id, plus an integer coefficient. Langhi et al. store the same thing in a
 * {@code HashMap<variable, exponent>}; we use sorted primitive arrays
 * deliberately so that the baseline is implemented generously / efficiently
 * and a slow data structure cannot be blamed for the comparison against CAPS.
 *
 * <p>The algebra is the standard monomial product of a provenance semiring:
 * coefficients multiply and exponents of a shared variable add. The merge
 * of the two sorted arrays is O(n+m).
 */
public final class Monomial implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final int[] EMPTY = new int[0];

  /** Variable ids, strictly increasing. Parallel to {@link #exps}. */
  public final int[] vars;

  /** Exponents, one per entry of {@link #vars}. */
  public final int[] exps;

  public final int coefficient;

  public Monomial() {
    this.vars = EMPTY;
    this.exps = EMPTY;
    this.coefficient = 1;
  }

  Monomial(int[] vars, int[] exps, int coefficient) {
    this.vars = vars;
    this.exps = exps;
    this.coefficient = coefficient;
  }

  /** Semiring 1: the empty product, coefficient 1, no variables. */
  public static Monomial one() {
    return new Monomial(EMPTY, EMPTY, 1);
  }

  /** Coefficient 1, a single variable with exponent 1. */
  public static Monomial var(int varId) {
    return new Monomial(new int[] {varId}, new int[] {1}, 1);
  }

  /**
   * Product of two monomials. Shared variables add exponents; disjoint
   * variables are copied; coefficients multiply. Linear in the total
   * number of variables.
   */
  // Two sorted arrays merge in O(n+m); a HashMap would make the baseline look worse than it is.
  public Monomial times(Monomial other) {
    final int n = this.vars.length;
    final int m = other.vars.length;
    final int coeff = this.coefficient * other.coefficient;
    if (n == 0) {
      return new Monomial(other.vars, other.exps, coeff);
    }
    if (m == 0) {
      return new Monomial(this.vars, this.exps, coeff);
    }
    final int[] tmpVars = new int[n + m];
    final int[] tmpExps = new int[n + m];
    int i = 0;
    int j = 0;
    int k = 0;
    while (i < n && j < m) {
      final int a = this.vars[i];
      final int b = other.vars[j];
      if (a < b) {
        tmpVars[k] = a;
        tmpExps[k] = this.exps[i];
        i++;
      } else if (a > b) {
        tmpVars[k] = b;
        tmpExps[k] = other.exps[j];
        j++;
      } else {
        tmpVars[k] = a;
        tmpExps[k] = this.exps[i] + other.exps[j];
        i++;
        j++;
      }
      k++;
    }
    while (i < n) {
      tmpVars[k] = this.vars[i];
      tmpExps[k] = this.exps[i];
      i++;
      k++;
    }
    while (j < m) {
      tmpVars[k] = other.vars[j];
      tmpExps[k] = other.exps[j];
      j++;
      k++;
    }
    if (k == tmpVars.length) {
      return new Monomial(tmpVars, tmpExps, coeff);
    }
    return new Monomial(Arrays.copyOf(tmpVars, k), Arrays.copyOf(tmpExps, k), coeff);
  }

  /** Sum of the exponents. */
  public int degree() {
    int d = 0;
    for (int i = 0; i < exps.length; i++) {
      d += exps[i];
    }
    return d;
  }

  public int numVars() {
    return vars.length;
  }

  /**
   * Equality is over {@code (vars, exps)} only. The coefficient is ignored
   * so that {@link Polynomial} can identify like terms and sum their
   * coefficients.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Monomial)) {
      return false;
    }
    Monomial other = (Monomial) o;
    return Arrays.equals(vars, other.vars) && Arrays.equals(exps, other.exps);
  }

  @Override
  public int hashCode() {
    return 31 * Arrays.hashCode(vars) + Arrays.hashCode(exps);
  }

  /**
   * Langhi-style rendering, except that a coefficient of 1 is omitted when
   * the monomial has variables (e.g. {@code 2<7>^2} vs {@code <9>^1}).
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (coefficient != 1 || vars.length == 0) {
      sb.append(coefficient);
    }
    for (int i = 0; i < vars.length; i++) {
      sb.append('<').append(vars[i]).append(">^").append(exps[i]);
    }
    return sb.toString();
  }

  /** Lexicographic order on {@code (vars, exps)}, used when combining like terms. */
  static int compareLike(Monomial a, Monomial b) {
    int c = compareInts(a.vars, b.vars);
    if (c != 0) {
      return c;
    }
    return compareInts(a.exps, b.exps);
  }

  private static int compareInts(int[] a, int[] b) {
    final int n = Math.min(a.length, b.length);
    for (int i = 0; i < n; i++) {
      if (a[i] != b[i]) {
        return a[i] < b[i] ? -1 : 1;
      }
    }
    return a.length - b.length;
  }
}
