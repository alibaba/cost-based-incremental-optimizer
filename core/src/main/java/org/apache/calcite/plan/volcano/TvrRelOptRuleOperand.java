package org.apache.calcite.plan.volcano;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptRuleOperandChildPolicy;
import org.apache.calcite.plan.tvr.TvrSemantics;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * The operand that matches a TvrMetaSet.
 */
public class TvrRelOptRuleOperand extends RelOptRuleOperand {

  public Predicate<TvrMetaSet> tvrPredicate;

  // Two identical TvrEdgeRelOptRuleOperand should be treated as different ones
  public List<TvrEdgeRelOptRuleOperand> tvrChildren;

  // This contains property edges coming in and out
  public List<TvrPropertyEdgeRuleOperand> tvrPropertyEdges;

  public boolean sameTvrType;

  public TvrRelOptRuleOperand(Predicate<TvrMetaSet> tvrPredicate,
      boolean sameTvrType) {
    // Ensures that no RelNode actually matches us
    super(DummyRelNode.class, null, k -> false,
        RelOptRuleOperandChildPolicy.ANY, ImmutableList.of());
    this.tvrPredicate = tvrPredicate;
    tvrChildren = new ArrayList<>();
    tvrPropertyEdges = new ArrayList<>();
    this.sameTvrType = sameTvrType;
  }

  public TvrRelOptRuleOperand() {
    this(x -> true, true);
  }

  public void addTvrConnection(Class<? extends TvrSemantics> clazz,
      Predicate<TvrSemantics> predicate, List<TvrEdgeTimeMatchInfo> tvrEdgeTimeInfoList,
      RelOptRuleOperand child, boolean enforceTvrType) {
    TvrEdgeRelOptRuleOperand edgeOp =
        new TvrEdgeRelOptRuleOperand(clazz, predicate, tvrEdgeTimeInfoList, this,
            child, enforceTvrType);
    tvrChildren.add(edgeOp);
    child.tvrParents.add(edgeOp);
  }

  public void addTvrPropertyEdge(Class<? extends TvrProperty> clazz,
      Predicate<TvrProperty> predicate, TvrRelOptRuleOperand toTvrOp) {
    TvrPropertyEdgeRuleOperand propertyEdge =
        new TvrPropertyEdgeRuleOperand(clazz, predicate, this, toTvrOp);
    tvrPropertyEdges.add(propertyEdge);
    if (toTvrOp != this) {
      toTvrOp.tvrPropertyEdges.add(propertyEdge);
    }
  }

  public boolean matches(TvrMetaSet tvr) {
    return tvrPredicate.test(tvr);
  }

}
