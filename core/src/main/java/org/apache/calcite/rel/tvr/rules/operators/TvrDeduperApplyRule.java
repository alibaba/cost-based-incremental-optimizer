package org.apache.calcite.rel.tvr.rules.operators;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.tvr.rels.LogicalTvrDeduper;

public class TvrDeduperApplyRule extends RelOptRule {

  public static final TvrDeduperApplyRule INSTANCE = new TvrDeduperApplyRule();

  private TvrDeduperApplyRule() {
    super(operand(LogicalTvrDeduper.class, any()),
        "TvrDeduperApplyRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalTvrDeduper deduper = call.rel(0);
    call.transformTo(deduper.apply());
  }
}
