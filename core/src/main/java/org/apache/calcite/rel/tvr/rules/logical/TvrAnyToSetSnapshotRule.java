package org.apache.calcite.rel.tvr.rules.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrSetSnapshot;
import org.apache.calcite.plan.volcano.*;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This rule is a part of Progressive General Rules. This rule converts trait
 * "TvrSemantics.SET_SNAPSHOT_MAX" of any node to "TvrSetSnapshot" when its input nodes'
 * traits are all "TvrSetSnapshot" and have the same version.
 */
public class TvrAnyToSetSnapshotRule extends RelOptTvrRule {

  public static TvrAnyToSetSnapshotRule INSTANCE =
      new TvrAnyToSetSnapshotRule();

  /**
   * When numInputs == 2, matches:
   * <p>
   *     ParentOp     --SET_SNAPSHOT_MAX--    ParentTvr
   *     /    \
   * ChildOp1  \   --SET_SNAPSHOT_MAX--   ChildTvr1     --SetSnapShot--  LeafOp1
   *       ChildOp2   --SET_SNAPSHOT_MAX--      ChildTvr2    --SetSnapShot--    LeafOp2
   *
   * <p>
   * Converts to:
   * <p>
   *     ParentOp     --SET_SNAPSHOT_MAX--    ParentTvr  --SetSnapShot--   NewParentOp
   *     /    \                                                 /    \
   * ChildOp1  \   --SET_SNAPSHOT_MAX--   ChildTvr1     --SetSnapShot--  LeafOp1   \
   *       ChildOp2   --SET_SNAPSHOT_MAX--      ChildTvr2    --SetSnapShot--    LeafOp2
   *
   * <p>
   */
  public TvrAnyToSetSnapshotRule() {
    super(operand(RelNode.class, tvrEdgeSSMax(tvr()), unordered(
        operand(RelSubset.class,
            tvrEdgeSSMax(tvr(tvrEdge(TvrSetSnapshot.class, logicalSubset()))),
            any()))));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    RelNode rel = root.get();
    VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();
    RelOptCluster cluster = rel.getCluster();

    TvrMetaSet childTvr = root.input(0).tvr().get();
    TvrSemantics setSnapshot = root.input(0).tvrSibling().tvrSemantics;

//    // No need to create new snapshots of sinks, as the ones on Snapshot(MAX)
//    // is enough
//    if (rel instanceof TableSink || rel instanceof AdhocSink) {
//      return false;
//    }

    // The input matched is ready, but we fire only when all inputs are ready
    return rel.getInputs().stream().allMatch(input -> {
      RelSet set = planner.getSet(input);
      TvrMetaSet myChildTvr = set.getTvrForTvrSet(childTvr.getTvrType());
      if (myChildTvr == null) {
        return false;
      }
      return myChildTvr.getSubset(setSnapshot, cluster.traitSet()) != null;
    });
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    RelNode parentRel = root.get();
    VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();
    RelOptCluster cluster = parentRel.getCluster();

    TvrMetaSetType tvrType = root.input(0).tvr().get().getTvrType();
    TvrSemantics setSnapshot = root.input(0).tvrSibling().tvrSemantics;

    List<RelNode> newInputs = parentRel.getInputs().stream().map(input -> {
      RelSet set = planner.getSet(input);
      TvrMetaSet myChildTvr = set.getTvrForTvrSet(tvrType);
      assert myChildTvr != null;
      RelNode newInput = myChildTvr.getSubset(setSnapshot, cluster.traitSet());
      assert newInput != null;
      return newInput;
    }).collect(Collectors.toList());

    RelNode newRel = parentRel.copy(parentRel.getTraitSet(), newInputs);

    transformToRootTvr(call, newRel, setSnapshot);
  }

}
