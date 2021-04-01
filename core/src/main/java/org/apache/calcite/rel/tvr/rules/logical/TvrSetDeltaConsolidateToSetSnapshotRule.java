package org.apache.calcite.rel.tvr.rules.logical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrSetSnapshot;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.TvrConverterRule;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrMetaSet.TvrConvertMatch;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrUtils;

import java.util.Collections;
import java.util.List;

public class TvrSetDeltaConsolidateToSetSnapshotRule extends TvrConverterRule {

  public static TvrSetDeltaConsolidateToSetSnapshotRule INSTANCE =
      new TvrSetDeltaConsolidateToSetSnapshotRule();

  private TvrSetDeltaConsolidateToSetSnapshotRule() {
    super(ImmutableList.of(TvrSetDelta.class));
  }

  @Override
  public List<TvrConvertMatch> match(TvrMetaSet tvr,
      TvrSemantics newTrait, RelOptCluster cluster) {
    TvrVersion[] snapshots = tvr.getTvrType().getSnapshots();
    if (!(TvrUtils.isSnapshotTime.test(newTrait) && newTrait.toVersion
        .equals(snapshots[snapshots.length - 1]))) {
      return Collections.emptyList();
    }

    TvrContext ctx = TvrContext.getInstance(cluster);
    if (!TvrUtils.progressiveConsolidateInAllRelset(ctx)) {
      boolean tvrUnderSink =
          tvr.getRelSet(TvrSemantics.SET_SNAPSHOT_MAX).getSubsets().stream()
              .flatMap(s -> s.getParentRels().stream())
              .filter(s -> s instanceof AbstractConverter).count() == 0;

      if (!tvrUnderSink) {
        return Collections.emptyList();
      }
    }

    TvrSetDelta inputTrait = (TvrSetDelta) newTrait;
    RelNode input = getInputSubset(cluster, tvr, newTrait);
    RelNode rel = inputTrait.isPositiveOnly() ?
        input :
        TvrSetDelta
            .consolidate(input, input.getCluster().getRexBuilder(), inputTrait,
                true);

    return ImmutableList
        .of(new TvrConvertMatch(new TvrSetSnapshot(newTrait.toVersion),
            ImmutableList.of(rel)));
  }
}
