package org.apache.calcite.rel.tvr.rules.streaming;

import org.apache.calcite.plan.InterTvrRule;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.tvr.trait.property.TvrStreamingPropertyQN;
import org.apache.calcite.rel.tvr.trait.property.TvrStreamingPropertyQP;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrTableUtils;
import org.apache.calcite.rel.tvr.utils.TvrUtils;

public class TvrStreamingTableScanPropRule extends RelOptTvrRule
    implements InterTvrRule {

  public static TvrStreamingTableScanPropRule INSTANCE =
      new TvrStreamingTableScanPropRule();

  private TvrStreamingTableScanPropRule() {
    super(operand(LogicalTableScan.class,
        rel -> !TvrTableUtils.isDerivedTable(rel)
            && TvrTableUtils.findTvrRelatedTableMapStr(rel) == null && TvrUtils
            .isBaseTableAppendOnly(rel.getTable()), tvrEdgeSSMax(tvr()),
        none()));
  }

  /**
   * Matches:
   * <p>
   *   LogicalTableScan --SET_SNAPSHOT_MAX-- Tvr
   * <p>
   *
   * Converts to:
   * <p>
   *                                   TvrStreamingPropertyQP
   *                                             -------
   *                                           /        \
   *   LogicalTableScan --SET_SNAPSHOT_MAX-- Tvr <-------
   *                                          \
   *                                           -------------> Tvr1 --SET_SNAPSHOT_MAX-- EmptyValues
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

    RelNode values =
        TvrUtils.makeLogicalEmptyValues(rel.getCluster(), rel.getRowType());

    call.transformBuilder()
        .addPropertyLink(tvr, TvrStreamingPropertyQP.INSTANCE, rel,
            tvr.getTvrType())
        .addPropertyLink(tvr, TvrStreamingPropertyQN.INSTANCE, values,
            tvr.getTvrType()).transform();
  }
}
