package org.apache.calcite.plan.volcano;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptRuleOperandChildPolicy;
import org.apache.calcite.plan.tvr.TvrSemantics;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * The operand that matches a TvrSemantics edge between a TvrMetaSet and a RelNode.
 */
public class TvrEdgeRelOptRuleOperand extends RelOptRuleOperand {

  Class<? extends TvrSemantics> tvrClazz;
  Predicate<TvrSemantics> predicate;
  ArrayList<TvrEdgeTimeMatchInfo> tvrEdgeTimeInfoList;

  TvrRelOptRuleOperand tvrOp;
  RelOptRuleOperand relOp;

  /**
   * If true, the matched (non-RelSubset) RelNode should be generic to
   * TvrMetaSet's type. See VolcanoPlanner.tvrGenericRels.
   */
  boolean enforceTvrType;

  public <R extends TvrSemantics> TvrEdgeRelOptRuleOperand(Class<R> tvrClazz,
      Predicate<? super R> predicate,
      List<TvrEdgeTimeMatchInfo> tvrEdgeTimeInfoList,
      TvrRelOptRuleOperand tvrOp, RelOptRuleOperand relOp,
      boolean enforceTvrType) {
    // Ensures that no RelNode actually matches us
    super(DummyRelNode.class, null, k -> false,
        RelOptRuleOperandChildPolicy.ANY, ImmutableList.of());
    this.tvrClazz = tvrClazz;
    this.predicate = (Predicate<TvrSemantics>) predicate;
    if (tvrEdgeTimeInfoList == null) {
      this.tvrEdgeTimeInfoList = new ArrayList<>();
    } else {
      this.tvrEdgeTimeInfoList = new ArrayList<>(tvrEdgeTimeInfoList);
    }
    this.tvrOp = tvrOp;
    this.relOp = relOp;
    this.enforceTvrType = enforceTvrType;
  }

  public Class<? extends TvrSemantics> getMatchedTvrClass() {
    return tvrClazz;
  }

  public boolean matches(TvrSemantics tvrKey) {
    if (!tvrClazz.isInstance(tvrKey)) {
      return false;
    }
    return predicate.test(tvrKey);
  }

  public void setTvrOp(TvrRelOptRuleOperand tvrOp) {
    this.tvrOp = tvrOp;
  }

  public TvrRelOptRuleOperand getTvrOp() {
    return this.tvrOp;
  }

  public RelOptRuleOperand getRelOp() {
    return this.relOp;
  }

  public Predicate<TvrSemantics> getPredicate() {
    return predicate;
  }

  public boolean enforceTvrType() {
    return this.enforceTvrType;
  }

  public List<TvrEdgeTimeMatchInfo> getTimeInfoList() {
    return this.tvrEdgeTimeInfoList;
  }

  public void addTvrEdgeTimeMatchInfo(TvrEdgeTimeMatchInfo info) {
    this.tvrEdgeTimeInfoList.add(info);
  }

}
