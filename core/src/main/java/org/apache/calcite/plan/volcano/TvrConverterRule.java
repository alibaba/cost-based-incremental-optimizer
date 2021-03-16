package org.apache.calcite.plan.volcano;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.tvr.TvrSemantics;
import java.util.List;

public abstract class TvrConverterRule extends RelOptRule implements
    TvrMetaSet.TvrConvertMatchPattern {

  private final List<Class<? extends TvrSemantics>> tvrClasses;

  protected TvrConverterRule(
      ImmutableList<Class<? extends TvrSemantics>> tvrClasses) {
    super(operand(RelOptRuleOperand.DummyRelNode.class, none()));
    this.tvrClasses = tvrClasses;
  }

  @Override
  final public boolean matches(RelOptRuleCall call) {
    return false;
  }

  @Override
  final public void onMatch(RelOptRuleCall call) {
  }

  @Override
  final public List<Class<? extends TvrSemantics>> getRelatedTvrClasses() {
    return tvrClasses;
  }

  protected RelSubset getInputSubset(RelOptCluster cluster, TvrMetaSet tvr,
      TvrSemantics tvrTrait) {
    RelSet set = tvr.getRelSet(tvrTrait);
    return set.getOrCreateSubset(cluster, cluster.traitSet());
  }
}
