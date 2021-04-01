package org.apache.calcite.rel.tvr.rules.dbtoaster;


import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;

import java.util.List;
import static org.apache.calcite.rel.tvr.rules.dbtoaster.LoptMultiJoin.*;
import static org.apache.calcite.rel.tvr.rules.dbtoaster.GooJoinOrderingRule.*;
import static org.apache.calcite.rel.tvr.utils.TvrRelOptUtils.*;

/**
 * Created by tedxu on 08/08/2017.
 */
public class CostFunctions {

  public static final CostFunction NATURAL = (vertices, relNodes, multiJoin, edge) -> 0;

  public static final CostFunction SELECTIVITY = (vertices, relNodes, multiJoin, edge) -> {
    final int factor0 = edge.factors.nth(0);
    final int factor1 = edge.factors.nth(1);
    return selectivity(vertices, factor0, factor1, relNodes, multiJoin,
        edge.resolvedCondition, edge.condition, false);
  };

  public static final CostFunction CARDINALITY = (vertices, relNodes, multiJoin, edge) -> {
    final int factor0 = edge.factors.nth(0);
    final int factor1 = edge.factors.nth(1);
    Vertex left = vertices.get(factor0);
    Vertex right = vertices.get(factor1);
    int d = Math.max(depth(factor0, vertices), depth(factor1, vertices));
    double punishment = 1 + .3 * d;
    return left.cost
        * right.cost
        * punishment
        * selectivity(vertices, factor0, factor1, relNodes, multiJoin,
            edge.resolvedCondition, edge.condition, false);
  };

  public static int depth(int factor, List<Vertex> vertices) {
    Vertex v = vertices.get(factor);
    if (v instanceof LeafVertex) {
      return 0;
    }
    assert v instanceof JoinVertex;
    final int factor0 = v.factors.nth(0);
    final int factor1 = v.factors.nth(1);
    return Math.max(depth(factor0, vertices), depth(factor1, vertices)) + 1;
  }

  /**
   * If there exits NDV, the selectivity will be 1/max(NDV); otherwise, the
   * selectivity will be guessed by the filters.
   * 
   * @param vertices
   * @param factor0
   * @param factor1
   * @param relNodes
   * @param multiJoin
   * @return
   */
  public static double selectivity(List<Vertex> vertices, int factor0,
      int factor1, List<Pair<RelNode, TargetMapping>> relNodes,
      LoptMultiJoin multiJoin, JoinConditionSplitRes resolvedCondition, RexNode condition, boolean throwsIfNdvNotExist) {
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final RexBuilder rexBuilder = multiJoin.getMultiJoinRel().getCluster()
        .getRexBuilder();
    Vertex leftVertex = vertices.get(factor0);
    Vertex rightVertex = vertices.get(factor1);
    Pair<RelNode, TargetMapping> leftPair = relNodes.get(factor0);
    Pair<RelNode, TargetMapping> rightPair = relNodes.get(factor1);
    double maxNDV = getMaxNDV(leftVertex, rightVertex, leftPair, rightPair,
        resolvedCondition, multiJoin, mq);

    List<RexNode> filters = Lists.newArrayList();
    filters.addAll(resolvedCondition.getLeftFilter());
    filters.addAll(resolvedCondition.getRightFilter());
    filters.addAll(resolvedCondition.getNonEqualExprs());

    if (maxNDV < 0) {
      if (throwsIfNdvNotExist) {
        throw new UnsupportedOperationException("NDV not deterministic");
      }
      return estimateSelectivity(condition, null, null);
    } else {
      double selectivity = estimateSelectivity(
              RexUtil.composeConjunction(rexBuilder, filters, false), null,
              null);
      return selectivity / maxNDV;
    }
  }

  public static Double estimateSelectivity(RexNode predicate, RelNode input,
                                           RelMetadataQuery mq) {
    return 0.15;
  }
  
  public static double getMaxNDV(Vertex leftVertex, Vertex rightVertex,
      Pair<RelNode, TargetMapping> leftPair,
      Pair<RelNode, TargetMapping> rightPair,
      JoinConditionSplitRes resolvedCondition, LoptMultiJoin multiJoin,
      RelMetadataQuery mq) {
    // NOTE, the resolvedCondition is sensitive for factor-order, we should get
    // the right joinKeys based on the current factor-order.
    List<RexNode> leftKeys = resolvedCondition.getLeftJoinKeys();
    
    // This means that the equal condition is true.
    if (leftKeys.isEmpty()) {
      assert resolvedCondition.getRightJoinKeys().isEmpty();
      return 1.0;
    }
    ImmutableBitSet leftKeyFactors = multiJoin.getJoinFilterFactorBitmap(
        leftKeys.get(0), false);
    ImmutableBitSet originalLeftMask, originalRightMask;
    if (leftVertex.factors.contains(leftKeyFactors)) {
      originalLeftMask = InputFinder.bits(resolvedCondition.getLeftJoinKeys(),
          null);
      originalRightMask = InputFinder.bits(
          resolvedCondition.getRightJoinKeys(), null);
    } else {
      originalLeftMask = InputFinder.bits(resolvedCondition.getRightJoinKeys(),
          null);
      originalRightMask = InputFinder.bits(resolvedCondition.getLeftJoinKeys(),
          null);
    }
    ImmutableBitSet leftMask = shuttleMask(originalLeftMask, leftPair.right);
    ImmutableBitSet rightMask = shuttleMask(originalRightMask, rightPair.right);

    Double leftNDV = mq.getDistinctRowCount(leftPair.left, leftMask, null);
    Double rightNDV = mq.getDistinctRowCount(rightPair.left, rightMask, null);
    if (leftNDV != null && leftNDV < Double.MAX_VALUE && rightNDV != null
        && rightNDV < Double.MAX_VALUE) {
      return Double.max(leftNDV, rightNDV);
    }
    return -1.0;
  }
  
  /**
   * Get the new bitSet for the originalMask by apply a TargetMapping
   * 
   * @param originalMask
   * @param mapping
   * @return
   */
  private static ImmutableBitSet shuttleMask(ImmutableBitSet originalMask,
      TargetMapping mapping) {
    List<Integer> newIndexList = Lists
        .newArrayListWithExpectedSize(originalMask.size());
    for (Integer index : originalMask.toList()) {
      newIndexList.add(mapping.getTarget(index));
    }
    return ImmutableBitSet.of(newIndexList);
  }
  
  public static final CostFunction UPDATELAST = new UpdateLastCostFunction();
  
  public static class UpdateLastCostFunction implements CostFunction {
    
    private static final double UPDATE_INPUT_COST = 1000.0;
    
    // default value is 0, maybe for test    
    private ImmutableBitSet updateInputs = ImmutableBitSet.of(0);

    @Override
    public double cost(List<Vertex> vertexes,
        List<Pair<RelNode, TargetMapping>> relNodes, LoptMultiJoin multiJoin,
        Edge edge) {
      if (edge.condition.isAlwaysTrue()) {
        // means cross product
        return Double.MAX_VALUE;
      }
      
      if (edge.factors.intersects(updateInputs)) {
        return UPDATE_INPUT_COST;
      }
      return 0;
    }
    
    @Override
    public void setUpdateInputs(ImmutableBitSet updateInputs) {
      this.updateInputs = updateInputs;
    }
    
  }

  public static class UpdateFirstCostFunction implements CostFunction {

    private static final double UPDATE_INPUT_COST = -1000.0;

    // default value is 0, maybe for test
    private ImmutableBitSet updateInputs = ImmutableBitSet.of(0);

    @Override
    public double cost(List<Vertex> vertexes,
        List<Pair<RelNode, TargetMapping>> relNodes, LoptMultiJoin multiJoin,
        Edge edge) {
      if (edge.condition.isAlwaysTrue()) {
        // means cross product
        return Double.MAX_VALUE;
      }

      if (edge.factors.intersects(updateInputs)) {
        return UPDATE_INPUT_COST;
      }
      return 0;
    }

    @Override
    public void setUpdateInputs(ImmutableBitSet updateInputs) {
      this.updateInputs = updateInputs;
    }

  }
}
