package org.apache.calcite.rel.tvr.rules.streaming;

import org.apache.calcite.plan.InterTvrRule;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.tvr.trait.property.TvrStreamingPropertyQN;
import org.apache.calcite.rel.tvr.trait.property.TvrStreamingPropertyQP;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrUtils;

public class TvrStreamingValuesPropRule extends RelOptTvrRule
    implements InterTvrRule {

  public static TvrStreamingValuesPropRule INSTANCE =
      new TvrStreamingValuesPropRule();

  private TvrStreamingValuesPropRule() {
    super(operand(Values.class, tvrEdgeSSMax(tvr()), none()));
  }

  /**
   * Matches:
   * <p>
   *     Values --SET_SNAPSHOT_MAX-- Tvr
   * <p>
   *
   * Converts to:
   * <p>
   *                                   TvrStreamingPropertyQP
   *                                      -------
   *                                    /        \
   *     Values --SET_SNAPSHOT_MAX-- Tvr <-------
   *                                   \
   *                                    -------------> Tvr1 --SET_SNAPSHOT_MAX-- EmptyValues
   *                                    TvrStreamingPropertyQN
   * <p>
   */
  @Override
  public boolean matches(RelOptRuleCall call) {
    TvrMetaSet tvr = getRoot(call).tvr().get();
    RelOptCluster cluster = getRoot(call).get().getCluster();
    TvrContext ctx = TvrContext.getInstance(cluster);
    return tvr.getTvrType().equals(ctx.getDefaultTvrType());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelNode rel = getRoot(call).get();
    TvrMetaSet tvr = getRoot(call).tvr().get();

    RelNode emptyValues =
        TvrUtils.makeLogicalEmptyValues(rel.getCluster(), rel.getRowType());

    call.transformBuilder()
        .addPropertyLink(tvr, TvrStreamingPropertyQP.INSTANCE, rel,
            tvr.getTvrType())
        .addPropertyLink(tvr, TvrStreamingPropertyQN.INSTANCE, emptyValues,
            tvr.getTvrType()).transform();
  }
}
