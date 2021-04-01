package org.apache.calcite.rel.tvr.rules.logical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrSetSnapshot;
import org.apache.calcite.plan.volcano.TvrConverterRule;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrMetaSet.TvrConvertMatch;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;

import java.util.List;

public class TvrSetSnapshotToPositiveSuperSetDeltaRule
    extends TvrConverterRule {

  public static TvrSetSnapshotToPositiveSuperSetDeltaRule INSTANCE =
      new TvrSetSnapshotToPositiveSuperSetDeltaRule();

  private TvrSetSnapshotToPositiveSuperSetDeltaRule() {
    super(ImmutableList.of(TvrSetSnapshot.class));
  }

  @Override
  public List<TvrConvertMatch> match(TvrMetaSet tvr, TvrSemantics newTrait,
      RelOptCluster cluster) {
    TvrSetDelta setDelta =
        new TvrSetDelta(newTrait.fromVersion, newTrait.toVersion, true);
    RelNode input = getInputSubset(cluster, tvr, newTrait);
    return ImmutableList
        .of(new TvrConvertMatch(setDelta, ImmutableList.of(input)));
  }
}
