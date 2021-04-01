package org.apache.calcite.rel.tvr.rules.dbtoaster;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;

import java.util.List;

import static org.apache.calcite.rel.tvr.rules.dbtoaster.LoptMultiJoin.*;
import static org.apache.calcite.rel.tvr.rules.dbtoaster.GooJoinOrderingRule.*;

/**
 * Cost function for join ordering.
 */
interface CostFunction {

  /**
   * Cost estimate.
   * @param vertexes
   * @param relNodes
   * @param multiJoin
   * @param edge
   * @return cost
   */
  double cost(List<Vertex> vertexes,
      List<Pair<RelNode, Mappings.TargetMapping>> relNodes,
      LoptMultiJoin multiJoin, Edge edge);
  
  /**
   * The bitSet means which input of the MultiJoin has data updated.
   * @param updateInputs
   */
  default void setUpdateInputs(ImmutableBitSet updateInputs) {
  }
}
