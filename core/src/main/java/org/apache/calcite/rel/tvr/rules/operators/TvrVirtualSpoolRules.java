package org.apache.calcite.rel.tvr.rules.operators;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.rels.LogicalTvrVirtualSpool;
import org.apache.calcite.rel.tvr.rels.TvrVirtualSpool;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.trait.TvrValueDelta;

import static java.util.Collections.singletonList;

public abstract class TvrVirtualSpoolRules {

  public static final TvrSetDeltaTvrVirtualSpoolRule TVR_SET_DELTA_TvrVirtualSpool_RULE =
      new TvrSetDeltaTvrVirtualSpoolRule();
  public static final TvrValueSemanticsTvrVirtualSpoolRule
      TVR_VALUE_SEMANTICS_TvrVirtualSpool_RULE = new TvrValueSemanticsTvrVirtualSpoolRule();
}

abstract class TvrTvrVirtualSpoolRuleBase extends RelOptTvrRule {
  TvrTvrVirtualSpoolRuleBase(Class<? extends TvrSemantics> tvrClass) {
    super(operand(TvrVirtualSpool.class, tvrEdgeSSMax(tvr()),
        operand(RelSubset.class,
            tvrEdgeSSMax(tvr(tvrEdge(tvrClass, logicalSubset()))), any())));
  }
}

/**
 * Tvr set delta TvrVirtualSpool rule
 */
class TvrSetDeltaTvrVirtualSpoolRule extends TvrTvrVirtualSpoolRuleBase {
  TvrSetDeltaTvrVirtualSpoolRule() {
    super(TvrSetDelta.class);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch TvrVirtualSpoolAny = getRoot(call);
    TvrVirtualSpool logicalTvrVirtualSpool = TvrVirtualSpoolAny.get();
    RelOfTvr input = TvrVirtualSpoolAny.input(0).tvrSibling();

    RelNode newLogicalTvrVirtualSpool = logicalTvrVirtualSpool
        .copy(logicalTvrVirtualSpool.getTraitSet(), singletonList(input.rel.get()));

    transformToRootTvr(call, newLogicalTvrVirtualSpool, input.tvrSemantics);
  }
}

/**
 * Tvr value semantics TvrVirtualSpool rule
 */
class TvrValueSemanticsTvrVirtualSpoolRule extends TvrTvrVirtualSpoolRuleBase {
  TvrValueSemanticsTvrVirtualSpoolRule() {
    super(TvrValueDelta.class);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch TvrVirtualSpoolAny = getRoot(call);
    LogicalTvrVirtualSpool logicalTvrVirtualSpool = TvrVirtualSpoolAny.get();
    RelOfTvr input = TvrVirtualSpoolAny.input(0).tvrSibling();

    RelNode newLogicalTvrVirtualSpool = logicalTvrVirtualSpool
        .copy(logicalTvrVirtualSpool.getTraitSet(), singletonList(input.rel.get()));

    transformToRootTvr(call, newLogicalTvrVirtualSpool, input.tvrSemantics);
  }
  
}
