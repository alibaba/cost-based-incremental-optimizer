package org.apache.calcite.rel.tvr.rules.logical;


import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrSetSnapshot;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.utils.TvrUtils;

import java.util.List;

public class SetDeltaTvrBridgingRule extends RelOptTvrRule {

  public static SetDeltaTvrBridgingRule INSTANCE = new SetDeltaTvrBridgingRule();

  /**
   * Bridge the Positive/Negative SetDelta between two TvrMetaSets
   *
   * Matches:
   * RelSubset
   *     -- TvrSetSnapshot -- Tvr#1 -- super TvrSetDelta(+/-) -- RelSubset
   *     -- TvrSetSnapshot -- Tvr#2
   * conditions:
   *  1. two TvrSetSnapshots are the same
   *  2. in Tvr#1, TvrSetSnapshot and TvrSetDelta have the same time range
   *  3. two tvrs are different
   *
   * Generates:
   * RelSubset
   *     -- TvrSetSnapshot -- Tvr#1 -- super TvrSetDelta(+/-) -- RelSubset
   *     -- TvrSetSnapshot -- Tvr#2 -- super TvrSetDelta(+/-) -- RelSubset
   *
   */
  private SetDeltaTvrBridgingRule() {
    super(operand(RelSubset.class, any(),
        tvrEdge(TvrSetSnapshot.class,
            t -> !t.equals(TvrSemantics.SET_SNAPSHOT_MAX), IDENTICAL_TIME,
            tvr(tvrEdge(TvrSetDelta.class,
                t -> !t.isPositiveOnly() && TvrUtils.equivSnapshot.test(t),
                IDENTICAL_TIME, logicalSubset()))),
        tvrEdge(TvrSetSnapshot.class,
            t -> !t.equals(TvrSemantics.SET_SNAPSHOT_MAX), IDENTICAL_TIME,
            tvr(false))));  // allow different tvr types
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    List<TvrMatch> tvrs = root.tvrs();
    TvrMatch tvrWithDelta = tvrs.stream().filter(tvr ->
        tvr.rels(TvrSemantics.class).size() > 1).findFirst().get();
    TvrMetaSet fromTvr = tvrWithDelta.get();
    TvrMatch tvrWithoutDelta = tvrs.stream().filter(tvr ->
        tvr.rels(TvrSemantics.class).size() == 1).findFirst().get();
    TvrMetaSet toTvr = tvrWithoutDelta.get();

    RelOfTvr fromSnapshot = tvrWithDelta.rel(TvrSetSnapshot.class);
    RelOfTvr fromDelta = tvrWithDelta.rel(TvrSetDelta.class);
    RelOfTvr toSnapshot = tvrWithoutDelta.rel(TvrSetSnapshot.class);

    assert fromSnapshot.tvrSemantics.timeRangeEquals(fromDelta.tvrSemantics);
    assert fromSnapshot.tvrSemantics.timeRangeEquals(toSnapshot.tvrSemantics);

    if (! fromSnapshot.tvrSemantics.equals(toSnapshot.tvrSemantics)) {
      return false;
    }
    if (fromTvr.getTvrType().equals(toTvr.getTvrType())) {
      return false;
    }

    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    List<TvrMatch> tvrs = root.tvrs();
    TvrMatch tvrWithDelta = tvrs.stream().filter(tvr ->
        tvr.rels(TvrSemantics.class).size() > 1).findFirst().get();
    TvrMatch tvrWithoutDelta = tvrs.stream().filter(tvr ->
        tvr.rels(TvrSemantics.class).size() == 1).findFirst().get();
    TvrMetaSet toTvr = tvrWithoutDelta.get();

    RelOfTvr fromDelta = tvrWithDelta.rel(TvrSetDelta.class);

    RelOptRuleCall.TransformBuilder builder = call.transformBuilder();
    builder.addTvrLink(fromDelta.rel.get(), fromDelta.tvrSemantics, toTvr);
    builder.transform();
  }
}
