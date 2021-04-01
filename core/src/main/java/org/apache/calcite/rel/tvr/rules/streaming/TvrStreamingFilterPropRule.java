package org.apache.calcite.rel.tvr.rules.streaming;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.InterTvrRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.tvr.trait.property.TvrStreamingPropertyQN;
import org.apache.calcite.rel.tvr.trait.property.TvrStreamingPropertyQP;

public class TvrStreamingFilterPropRule extends RelOptTvrRule
    implements InterTvrRule {

  public static TvrStreamingFilterPropRule INSTANCE =
      new TvrStreamingFilterPropRule();

  private TvrStreamingFilterPropRule() {
    super(operand(LogicalFilter.class, tvrEdgeSSMax(tvr()),
        operand(RelSubset.class, tvrEdgeSSMax(
            tvr(tvrProperty(TvrStreamingPropertyQP.class,
                tvr(tvrEdgeSSMax(logicalSubset()))),
                tvrProperty(TvrStreamingPropertyQN.class,
                    tvr(tvrEdgeSSMax(logicalSubset()))))), any())));
  }

  /**
   * Matches:
   * <p>
   *
   *   LogicalFilter  --SET_SNAPSHOT_MAX--  Tvr1
   *         |                                     QP
   *       input      --SET_SNAPSHOT_MAX--  Tvr2 -----> Tvr3  --SET_SNAPSHOT_MAX--  qpInput
   *                                          \   QN
   *                                           ---------------> Tvr4  --SET_SNAPSHOT_MAX--  qnInput
   *
   * <p>
   * Converts to:
   * <p>
   *                                               QN
   *   LogicalFilter  --SET_SNAPSHOT_MAX--  Tvr1 ------------> Tvr5  --SET_SNAPSHOT_MAX--  newFilter1
   *         |                                 \   QP                                          |
   *         |                                  -----> Tvr6  --SET_SNAPSHOT_MAX-- newFilter2   |
   *         |                                                                        |        |
   *         |                                     QP                                 |        |
   *       input      --SET_SNAPSHOT_MAX--  Tvr2 -----> Tvr3  --SET_SNAPSHOT_MAX--  qpInput    |
   *                                          \   QN                                           |
   *                                           ---------------> Tvr4  --SET_SNAPSHOT_MAX--  qnInput
   * <p>
   */
  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch match = getRoot(call);
    LogicalFilter filter = match.get();
    TvrMetaSet rootTvr = match.tvr().get();

    RelNode qnInput =
        match.input(0).tvr().propertyToTvr(TvrStreamingPropertyQN.class)
            .rel(TvrSemantics.SET_SNAPSHOT_MAX).get();
    RelNode qpInput =
        match.input(0).tvr().propertyToTvr(TvrStreamingPropertyQP.class)
            .rel(TvrSemantics.SET_SNAPSHOT_MAX).get();

    RelNode newFilter1 =
        filter.copy(filter.getTraitSet(), ImmutableList.of(qnInput));
    RelNode newFilter2 =
        filter.copy(filter.getTraitSet(), ImmutableList.of(qpInput));

    // QP + QN = SetSnapshotMax
    RelNode merge =
        LogicalUnion.create(ImmutableList.of(newFilter1, newFilter2), true);

    call.transformBuilder().addEquiv(merge, filter)
        .addTvrType(merge, rootTvr.getTvrType())
        .addPropertyLink(rootTvr, TvrStreamingPropertyQP.INSTANCE, newFilter2,
            rootTvr.getTvrType())
        .addPropertyLink(rootTvr, TvrStreamingPropertyQN.INSTANCE, newFilter1,
            rootTvr.getTvrType()).transform();
  }
}
