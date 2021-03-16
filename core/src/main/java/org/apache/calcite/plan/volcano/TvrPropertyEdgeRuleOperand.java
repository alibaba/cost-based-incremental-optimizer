package org.apache.calcite.plan.volcano;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptRuleOperandChildPolicy;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * The operand that matches a Tvr Property edge between two Tvrs.
 */
public class TvrPropertyEdgeRuleOperand extends RelOptRuleOperand {

  Class<? extends TvrProperty> propertyClazz;
  Predicate<TvrProperty> predicate;

  TvrRelOptRuleOperand fromTvrOp;
  TvrRelOptRuleOperand toTvrOp;

  public <R extends TvrProperty> TvrPropertyEdgeRuleOperand(
      Class<R> propertyClazz, Predicate<? super R>  predicate,
      TvrRelOptRuleOperand fromTvrOp, TvrRelOptRuleOperand toTvrOp) {
    // Ensures that no RelNode actually matches us
    super(DummyRelNode.class, null, k -> false,
        RelOptRuleOperandChildPolicy.ANY, ImmutableList.of());
    this.propertyClazz = propertyClazz;
    this.predicate = (Predicate<TvrProperty>) predicate;
    this.fromTvrOp = fromTvrOp;
    this.toTvrOp = Objects.requireNonNull(toTvrOp);
  }

  public Class<? extends TvrProperty> getMatchedPropertyClass() {
    return propertyClazz;
  }

  public boolean matches(TvrProperty property) {
    if (!propertyClazz.isInstance(property)) {
      return false;
    }
    return predicate.test(property);
  }

  public TvrRelOptRuleOperand getFromTvrOp() {
    return this.fromTvrOp;
  }

  public void setFromOp(TvrRelOptRuleOperand fromTvrOp) {
    this.fromTvrOp = fromTvrOp;
  }

  public TvrRelOptRuleOperand getToTvrOp() {
    return this.toTvrOp;
  }

  public void setToTvrOp(TvrRelOptRuleOperand toTvrOp) {
    this.toTvrOp = toTvrOp;
  }

  public Predicate<TvrProperty> getPredicate() {
    return predicate;
  }

  public boolean isFromTvrOp(TvrRelOptRuleOperand tvrOp) {
    return this.fromTvrOp == tvrOp;
  }

  public TvrRelOptRuleOperand theOtherEndTvrOp(TvrRelOptRuleOperand tvrOp) {
    if (this.fromTvrOp == tvrOp) {
      return this.toTvrOp;
    } else if (this.toTvrOp == tvrOp) {
      return this.fromTvrOp;
    } else {
      throw new RuntimeException(
          tvrOp + " is not either end of this edge " + this);
    }
  }
}
