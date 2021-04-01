package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;

import java.util.Collection;
import java.util.Set;

public interface MultiQueryCostOptimizer {

  void relCostIncreased(Set<RelNode> rels);

  boolean tryToMat(Set<RelSubset> subset, MatType matType, boolean strict);

  void rollback();

  void commit();

  Set<RelSubset> getMaterialized();

  RelNode getBest(RelSubset subset);

  RelOptCost potentialBenefit(RelSubset subset, int numReuse);

  /**
   * Gets the overall cost using the given materialized nodes.
   */
  RelOptCost getOverallCost();

  /**
   * Builds the cheapest plan using the given materialized nodes.
   * @param bestNodes
   */
  RelNode buildCheapestPlan(Collection<RelNode> bestNodes);
}
