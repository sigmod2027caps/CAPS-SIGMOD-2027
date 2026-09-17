package ink;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Mutable append-only monomial accumulator used by the six dataflow
 * aggregates. Langhi et al.'s {@code Monomial.times(R, int)} mutates in
 * place in O(1); the previous {@code acc.hm = acc.hm.times(value.hm)}
 * allocated two {@code int[n+1]} arrays and copied {@code n} elements
 * per {@code add}, i.e. {@code Theta(n^2)} work per window of occupancy
 * {@code n}. This builder restores the O(1) common case.
 *
 * <p><b>Invariant:</b> {@code vars[0..n)} is strictly increasing. The
 * same holds for a second builder passed to {@link #mul(MonomialBuilder)}.
 *
 * <p><b>Fast path</b> ({@link #mul(InkMeta)}): every annotation in these
 * dataflows is a single monomial with coefficient 1, so the incoming
 * {@code (var, exp)} pairs are folded in with {@link #mulVar}. A source
 * operator mints strictly increasing varIds, so the typical call is a
 * trailing bump or an append, both O(1). Several variables on one
 * monomial (e.g. a joined pair) are handled by looping {@code mulVar}.
 *
 * <p><b>Fallback:</b> if the incoming annotation has several monomials
 * or a coefficient other than 1 — neither occurs in the six dataflows,
 * but {@link Polynomial#plus} exists — the builder materialises itself
 * with {@link #toMeta()} and uses {@link Polynomial#times}. The product
 * is stored as an overflow polynomial so the result stays correct even
 * when it is no longer a single monomial.
 */
public class MonomialBuilder implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final int INIT_CAP = 4;

  /** Variable ids, strictly increasing on {@code [0, n)}. */
  private int[] vars;

  /** Exponents, parallel to {@link #vars}. */
  private int[] exps;

  private int n;

  /**
   * Set when {@link #mul(InkMeta)} had to leave the single-monomial
   * world. Null on the fast path used by the six dataflows.
   */
  // The polynomial grows with the tuples that contributed, so we can not model it as one monomial forever.
  private Polynomial overflow;

  public MonomialBuilder() {
    this.vars = new int[INIT_CAP];
    this.exps = new int[INIT_CAP];
    this.n = 0;
    this.overflow = null;
  }

  /**
   * Multiply in the annotation of one incoming tuple.
   *
   * <p>Fast path: a single monomial of coefficient 1 — for each
   * {@code (var, exp)} call {@link #mulVar}. Fallback: materialise and
   * {@link Polynomial#times}.
   */
  public void mul(InkMeta meta) {
    if (overflow != null) {
      overflow = overflow.times(meta.poly);
      return;
    }
    if (isFastPath(meta)) {
      Monomial m = meta.poly.monomials[0];
      // Typical dataflow: one var per source tuple; trailing bump is the common occurence.
      for (int i = 0; i < m.vars.length; i++) {
        mulVar(m.vars[i], m.exps[i]);
      }
      return;
    }
    absorb(toMeta().times(meta));
  }

  /**
   * Multiply by {@code x_varId^exp}. If {@code n > 0} and the last
   * stored variable is {@code varId}, bump its exponent (O(1), the
   * common case). If the builder is empty or {@code varId} is larger
   * than every stored id, append (O(1)). Otherwise binary-search and
   * insert (O(n), rare: out-of-order varIds).
   */
  public void mulVar(int varId, int exp) {
    if (overflow != null) {
      overflow = overflow.times(varPoly(varId, exp));
      return;
    }
    if (n > 0 && vars[n - 1] == varId) {
      exps[n - 1] += exp;
      assert strictlyIncreasing();
      return;
    }
    if (n == 0 || varId > vars[n - 1]) {
      ensure(n + 1);
      vars[n] = varId;
      exps[n] = exp;
      n++;
      assert strictlyIncreasing();
      return;
    }
    int idx = Arrays.binarySearch(vars, 0, n, varId);
    if (idx >= 0) {
      exps[idx] += exp;
      assert strictlyIncreasing();
      return;
    }
    int ins = -idx - 1;
    ensure(n + 1);
    System.arraycopy(vars, ins, vars, ins + 1, n - ins);
    System.arraycopy(exps, ins, exps, ins + 1, n - ins);
    vars[ins] = varId;
    exps[ins] = exp;
    n++;
    assert strictlyIncreasing();
  }

  /**
   * Multiply by another builder. Used by the aggregate {@code merge}
   * / combine path. Same fast path / fallback split as {@link #mul(InkMeta)}.
   */
  public void mul(MonomialBuilder other) {
    if (other == null) {
      return;
    }
    if (overflow != null || other.overflow != null) {
      absorb(toMeta().times(other.toMeta()));
      return;
    }
    for (int i = 0; i < other.n; i++) {
      mulVar(other.vars[i], other.exps[i]);
    }
  }

  /**
   * Freeze into an immutable {@link InkMeta} by copying exactly
   * {@code n} entries. Does not hand out the builder's live arrays:
   * {@code getResult} may be called on a window whose accumulator
   * keeps receiving elements.
   */
  public InkMeta toMeta() {
    assert strictlyIncreasing();
    if (overflow != null) {
      return new InkMeta(overflow);
    }
    if (n == 0) {
      return InkMeta.one();
    }
    return new InkMeta(new Polynomial(new Monomial[] {
        new Monomial(Arrays.copyOf(vars, n), Arrays.copyOf(exps, n), 1)
    }));
  }

  public int numVars() {
    if (overflow != null) {
      return overflow.totalVars();
    }
    return n;
  }

  private static boolean isFastPath(InkMeta meta) {
    return meta != null
        && meta.poly != null
        && meta.poly.monomials.length == 1
        && meta.poly.monomials[0].coefficient == 1;
  }

  private void absorb(InkMeta product) {
    Polynomial p = product.poly;
    if (p.monomials.length == 1 && p.monomials[0].coefficient == 1) {
      Monomial m = p.monomials[0];
      n = m.vars.length;
      int cap = vars.length;
      while (cap < n) {
        cap *= 2;
      }
      vars = Arrays.copyOf(m.vars, cap);
      exps = Arrays.copyOf(m.exps, cap);
      overflow = null;
      assert strictlyIncreasing();
      return;
    }
    overflow = p;
    n = 0;
  }

  private static Polynomial varPoly(int varId, int exp) {
    return new Polynomial(new Monomial[] {
        new Monomial(new int[] {varId}, new int[] {exp}, 1)
    });
  }

  private void ensure(int need) {
    if (need <= vars.length) {
      return;
    }
    int cap = vars.length;
    while (cap < need) {
      cap *= 2;
    }
    vars = Arrays.copyOf(vars, cap);
    exps = Arrays.copyOf(exps, cap);
  }

  /** {@code vars[0..n)} strictly increasing. Vacuous when {@code n == 0}. */
  private boolean strictlyIncreasing() {
    for (int i = 1; i < n; i++) {
      if (vars[i] <= vars[i - 1]) {
        return false;
      }
    }
    return true;
  }
}
