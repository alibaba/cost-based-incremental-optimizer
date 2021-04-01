package org.apache.calcite.rel.tvr.rules.logical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.TvrConverterRule;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrMetaSet.TvrConvertMatch;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.tvr.trait.TvrValueDelta;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.calcite.rel.tvr.utils.TvrUtils.isSnapshotTime;

/**
 * This rule is a part of Progressive General Rules. This rule converts trait
 * "TvrValueDelta" (t' - t) to "TvrValueDelta" (MIN - t) by merging a
 * previous super value delta state (MIN - t').
 */
public class TvrValueDeltaMergeRule extends TvrConverterRule {

  public static TvrValueDeltaMergeRule INSTANCE =
      new TvrValueDeltaMergeRule();

  private TvrValueDeltaMergeRule() {
    super(ImmutableList.of(TvrValueDelta.class));
  }

  private TvrConvertMatch create(TvrValueDelta prev, TvrValueDelta delta,
      RelOptCluster cluster, TvrMetaSet tvr) {
    RelNode leftInput = getInputSubset(cluster, tvr, prev);
    RelNode rightInput = getInputSubset(cluster, tvr, delta);
    RelNode rel =
        LogicalUnion.create(ImmutableList.of(leftInput, rightInput), true);

    // always do the consolidation here to transform the trait to a positive-only value delta
    // FIXME: aggregate transformer will not do the consolidation
    TvrValueDelta to = new TvrValueDelta(prev.fromVersion, delta.toVersion,
        prev.getTransformer(), true);
    return new TvrConvertMatch(to, to.consolidate(ImmutableList.of(rel)));
  }

  @Override
  public List<TvrConvertMatch> match(TvrMetaSet tvr, TvrSemantics newTrait,
      RelOptCluster cluster) {
    TvrValueDelta delta = (TvrValueDelta) newTrait;

    if (isSnapshotTime.test(delta)) {
      return tvr.allTvrSemantics().stream().filter(
          d -> d instanceof TvrValueDelta && delta.valueDefEquals(d)
              && d.fromVersion.equals(delta.toVersion))
          .map(d -> create(delta, (TvrValueDelta) d, cluster, tvr))
          .collect(Collectors.toList());
    }

    return tvr.allTvrSemantics().stream().filter(
        p -> p instanceof TvrValueDelta && isSnapshotTime.test(p) && delta
            .valueDefEquals(p) && p.toVersion.equals(delta.fromVersion))
        .map(p -> create((TvrValueDelta) p, delta, cluster, tvr))
        .collect(Collectors.toList());
  }
}
