package org.apache.calcite.rel.tvr.rules.logical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrSetSnapshot;
import org.apache.calcite.plan.volcano.TvrConverterRule;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrMetaSet.TvrConvertMatch;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.trait.TvrValueDelta;
import org.apache.calcite.rel.tvr.utils.TvrUtils;

import java.util.Collections;
import java.util.List;

/**
 * This rule is a part of Progressive General Rules. This rule converts trait
 * "TvrValueSnapshot" of any node to "TvrSetSnapshot" by applying the final
 * aggregate, filtering the rows whose "COUNT(1)" is less than or equal to zero,
 * and then projecting accordingly.
 */
public class TvrSuperValueDeltaToSetSnapshotRule extends TvrConverterRule {

  public static TvrSuperValueDeltaToSetSnapshotRule INSTANCE =
      new TvrSuperValueDeltaToSetSnapshotRule();

  private TvrSuperValueDeltaToSetSnapshotRule() {
    super(ImmutableList.of(TvrValueDelta.class));
  }

  @Override
  public List<TvrConvertMatch> match(TvrMetaSet tvr, TvrSemantics newTrait,
      RelOptCluster cluster) {
    if (!TvrUtils.isSnapshotTime.test(newTrait)) {
      return Collections.emptyList();
    }

    TvrValueDelta superValueDelta = (TvrValueDelta) newTrait;
    RelNode input = getInputSubset(cluster, tvr, newTrait);

    List<RelNode> newRels =
        superValueDelta.getTransformer().apply(ImmutableList.of(input));
    return ImmutableList
        .of(new TvrConvertMatch(new TvrSetSnapshot(newTrait.toVersion),
            newRels));
  }
}