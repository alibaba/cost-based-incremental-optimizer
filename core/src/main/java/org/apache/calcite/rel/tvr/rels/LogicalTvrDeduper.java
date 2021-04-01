package org.apache.calcite.rel.tvr.rels;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

/**
 * This node is usually used to convert a value delta to positive-only value delta
 * when merging value deltas.
 * For example:
 *    value delta+ (MIN - t1) + value delta (t1 - t2) => value delta (MIN - t2)
 *
 *    value delta (MIN - t2) -> Tvr Deduper -> value delta+ (MIN - t2)
 */
public abstract class LogicalTvrDeduper extends SingleRel {

  LogicalTvrDeduper(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, traits, input);
  }

  /**
   *  do the de-duplication
   */
  public abstract RelNode apply();

  /**
   * compare two TvrDeduper
   */
  public abstract boolean valueEquals(Object o);
}
