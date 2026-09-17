package ink;

/**
 * Plain {@code main} self-test of the polynomial algebra. No JUnit:
 * {@code java -cp competitors/inkstream/target/classes ink.SelfTest}.
 * Prints {@code SELFTEST OK} and exits 0 on success; exits non-zero
 * on the first failure.
 */
public final class SelfTest {

  private SelfTest() {}

  // No JUnit in the artifact — java -cp ... ink.SelfTest is enough for the algebra checks.
  public static void main(String[] args) {
    try {
      monomialTimesAddsExponents();
      polynomialTimesDistributes();
      polynomialPlusCombinesLikeTerms();
      taxi1HowMuch();
      metadataBytesKnown();
      builderMulVarBumpsTrailing();
      builderInsertsOutOfOrder();
      builderToMetaDoesNotAlias();
      builderTaxi1HowMuch();
      System.out.println("SELFTEST OK");
    } catch (AssertionError e) {
      System.err.println("SELFTEST FAIL: " + e.getMessage());
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  /** {@code x1 * x1 = x1^2}. */
  private static void monomialTimesAddsExponents() {
    Monomial product = Monomial.var(1).times(Monomial.var(1));
    check(product.numVars() == 1, "x1*x1 should have one variable, got " + product.numVars());
    check(product.vars[0] == 1, "x1*x1 variable id");
    check(product.exps[0] == 2, "x1*x1 should have exponent 2, got " + product.exps[0]);
    check(product.coefficient == 1, "x1*x1 coefficient");
    check(product.degree() == 2, "x1*x1 degree");
  }

  /** {@code (x1 + x2) * x1 = x1^2 + x1*x2}, two monomials. */
  private static void polynomialTimesDistributes() {
    Polynomial sum = Polynomial.var(1).plus(Polynomial.var(2));
    Polynomial product = sum.times(Polynomial.var(1));
    check(product.numMonomials() == 2,
        "(x1+x2)*x1 should distribute to 2 monomials, got " + product.numMonomials()
            + " [" + product + "]");
    // x1^2 and x1*x2, in like-term sort order.
    Monomial a = product.monomials[0];
    Monomial b = product.monomials[1];
    check(a.numVars() == 1 && a.vars[0] == 1 && a.exps[0] == 2,
        "first term should be x1^2, got " + a);
    check(b.numVars() == 2 && b.vars[0] == 1 && b.exps[0] == 1
            && b.vars[1] == 2 && b.exps[1] == 1,
        "second term should be x1*x2, got " + b);
  }

  /** {@code x1 + x1 = 2*x1}; {@code x1 + x2} stays two terms. */
  private static void polynomialPlusCombinesLikeTerms() {
    Polynomial like = Polynomial.var(1).plus(Polynomial.var(1));
    check(like.numMonomials() == 1, "x1+x1 should combine, got " + like.numMonomials());
    check(like.monomials[0].coefficient == 2, "x1+x1 coefficient should be 2");
    check(like.monomials[0].numVars() == 1 && like.monomials[0].vars[0] == 1,
        "x1+x1 should remain x1");

    Polynomial unlike = Polynomial.var(1).plus(Polynomial.var(2));
    check(unlike.numMonomials() == 2, "x1+x2 should stay 2 monomials");
  }

  /**
   * Taxi_1 intro-figure example: four source tuples on channels
   * {0, 0, 1, 1} multiplied together (join + aggregate, both
   * conjunctive) yield how-much {@code {2, 2}} for {@code channels = 2}.
   */
  private static void taxi1HowMuch() {
    InkMeta m = InkMeta.source(0, 2, 0)
        .times(InkMeta.source(0, 2, 1))
        .times(InkMeta.source(1, 2, 2))
        .times(InkMeta.source(1, 2, 3));
    long[] hm = m.howMuch(2);
    check(hm.length == 2, "howMuch length");
    check(hm[0] == 2 && hm[1] == 2,
        "taxi_1 howMuch should be {2,2}, got {" + hm[0] + "," + hm[1] + "}");
    check(m.howMuchTotal(2) == 4, "howMuchTotal must fold without a vector");
    check(m.numMonomials() == 1, "product of four distinct vars is one monomial");
    check(m.totalVars() == 4, "four variables in the product");
  }

  /**
   * One monomial of two variables: {@code 2 * 8 = 16} bytes
   * (8 B per (variable, exponent) pair; coefficient is not charged).
   */
  private static void metadataBytesKnown() {
    InkMeta twoVars = InkMeta.source(0, 2, 0).times(InkMeta.source(1, 2, 1));
    check(twoVars.metadataBytes() == 16L,
        "x*y metadataBytes should be 16, got " + twoVars.metadataBytes());
    InkMeta oneVar = InkMeta.source(0, 2, 0);
    check(oneVar.metadataBytes() == 8L,
        "x metadataBytes should be 8, got " + oneVar.metadataBytes());
    InkMeta unit = InkMeta.one();
    check(unit.metadataBytes() == 0L,
        "1 metadataBytes should be 0 (no variables, coefficient not charged), got "
            + unit.metadataBytes());
  }

  /** Repeated trailing {@code mulVar} bumps the exponent, still one variable. */
  private static void builderMulVarBumpsTrailing() {
    MonomialBuilder b = new MonomialBuilder();
    b.mulVar(5, 1);
    b.mulVar(5, 2);
    check(b.numVars() == 1, "trailing bump should stay one variable, got " + b.numVars());
    InkMeta m = b.toMeta();
    check(m.numMonomials() == 1, "trailing bump is one monomial");
    check(m.poly.monomials[0].vars[0] == 5 && m.poly.monomials[0].exps[0] == 3,
        "x5 * x5^2 should be x5^3, got " + m);
  }

  /** Out-of-order {@code mulVar} keeps {@code vars} strictly increasing. */
  private static void builderInsertsOutOfOrder() {
    MonomialBuilder b = new MonomialBuilder();
    b.mulVar(3, 1);
    b.mulVar(1, 2);
    b.mulVar(2, 1);
    InkMeta m = b.toMeta();
    Monomial mono = m.poly.monomials[0];
    check(mono.numVars() == 3, "out-of-order insert should have 3 vars, got " + mono.numVars());
    check(mono.vars[0] == 1 && mono.vars[1] == 2 && mono.vars[2] == 3,
        "vars should be strictly increasing 1,2,3, got " + m);
    check(mono.exps[0] == 2 && mono.exps[1] == 1 && mono.exps[2] == 1,
        "exponents should follow the vars, got " + m);
  }

  /**
   * {@code toMeta()} copies; mutating the builder afterwards must not
   * change the frozen annotation.
   */
  private static void builderToMetaDoesNotAlias() {
    MonomialBuilder b = new MonomialBuilder();
    b.mulVar(1, 1);
    InkMeta frozen = b.toMeta();
    b.mulVar(2, 1);
    b.mulVar(1, 4);
    check(frozen.totalVars() == 1 && frozen.poly.monomials[0].vars[0] == 1
            && frozen.poly.monomials[0].exps[0] == 1,
        "frozen meta must stay x1^1 after further builder writes, got " + frozen);
    check(b.toMeta().totalVars() == 2,
        "builder itself should now have two variables");
  }

  /**
   * Taxi_1 intro-figure via the builder: {0,0,1,1} yields how-much
   * {@code {2, 2}} for {@code channels = 2}, identical to the
   * {@link InkMeta#times} chain in {@link #taxi1HowMuch}.
   */
  private static void builderTaxi1HowMuch() {
    MonomialBuilder b = new MonomialBuilder();
    b.mul(InkMeta.source(0, 2, 0));
    b.mul(InkMeta.source(0, 2, 1));
    b.mul(InkMeta.source(1, 2, 2));
    b.mul(InkMeta.source(1, 2, 3));
    InkMeta m = b.toMeta();
    long[] hm = m.howMuch(2);
    check(hm.length == 2, "builder howMuch length");
    check(hm[0] == 2 && hm[1] == 2,
        "builder taxi_1 howMuch should be {2,2}, got {" + hm[0] + "," + hm[1] + "}");
    check(m.numMonomials() == 1, "builder product of four distinct vars is one monomial");
    check(m.totalVars() == 4, "builder four variables in the product");
  }

  private static void check(boolean cond, String msg) {
    if (!cond) {
      throw new AssertionError(msg);
    }
  }
}
