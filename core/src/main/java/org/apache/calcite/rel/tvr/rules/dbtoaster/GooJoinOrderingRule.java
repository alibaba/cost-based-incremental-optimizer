package org.apache.calcite.rel.tvr.rules.dbtoaster;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.tvr.utils.Config;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrRelOptUtils;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

import static org.apache.calcite.rel.tvr.rules.dbtoaster.LoptMultiJoin.*;
import static org.apache.calcite.rel.tvr.utils.TvrRelOptUtils.*;

/**
 * Planner rule that finds an approximately optimal ordering for join operators
 * using a heuristic algorithm.
 *
 * <p>It is triggered by the pattern {@link MultiJoin}.
 *
 * <p>It is similar to
 * {@link org.apache.calcite.rel.rules.LoptOptimizeJoinRule}.
 * {@code LoptOptimizeJoinRule} is only capable of producing left-deep joins;
 * this rule is capable of producing bushy joins.
 *
 * <p>TODO:
 * <ol>
 *   <li>Join conditions that touch 1 factor.
 *   <li>Join conditions that touch 3 factors.
 *   <li>More than 1 join conditions that touch the same pair of factors,
 *       e.g. {@code t0.c1 = t1.c1 and t1.c2 = t0.c3}
 * </ol>
 *
 * <p>NOTE: Changes from org.apache.calcite
 * <ol>
 *   <li>Edges to contain a join type attribute, and Vertices to contain a
 *   LHS join type attribute so that we can support outer joins.
 *   <li>Joins are not swapped to major-miner after selection from join
 *   functions.
 * </ol>
 */
public class GooJoinOrderingRule extends RelOptRule {

  private static final Log LOG = LogFactory.getLog(GooJoinOrderingRule.class);

  public static final GooJoinOrderingRule NAIVE =
      new GooJoinOrderingRule(false,
          CostFunctions.NATURAL, "GooJoinOrderingRule:naive");

  public static final GooJoinOrderingRule UNGROUP =
      new GooJoinOrderingRule(false,
          CostFunctions.CARDINALITY, "GooJoinOrderingRule:ungroup");

  public static final GooJoinOrderingRule GROUP =
      new GooJoinOrderingRule(true,
          CostFunctions.CARDINALITY, "GooJoinOrderingRule:group");

  public static final GooJoinOrderingRule SELECTIVITY =
      new GooJoinOrderingRule(false,
          CostFunctions.SELECTIVITY, "GooJoinOrderingRule:selectivity");
  
  public static final GooJoinOrderingRule UPDATELAST = new GooJoinOrderingRule(
      false, CostFunctions.UPDATELAST, "GooJoinOrderingRule:updateLast");

  boolean groupJoins;
  CostFunction costFunction;

  /** Creates an GooJoinOrderingRule. */
  public GooJoinOrderingRule(
      boolean groupJoins,
      CostFunction costFunction, String description) {
    super(operand(MultiJoin.class, any()), description);
    this.groupJoins = groupJoins;
    this.costFunction = costFunction;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    TvrContext ctx = TvrContext.getInstance(call.rel(0).getCluster());
    if (TvrUtils.progressiveDbToasterStrict(ctx)) {
      return;
    }

    RelNode transformed = transform(call.rel(0), call.builder());
    if (transformed != null) {
      call.transformTo(transformed);
    }
  }

  public RelNode transform(MultiJoin multiJoinRel, RelBuilder relBuilder) {
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final Config config = TvrContext.getInstance(multiJoinRel.getCluster()).getConfig();
    // for build the final relNode tree.
    List<Pair<RelNode, TargetMapping>> relNodes = Lists.newArrayList();

    final LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);

    // inputRef mappings for each factor, source is the inputRef based on the
    // multiJoin while target is the inputRef based on the factor of the
    // multJoin.
    final List<TargetMapping> factorMappings = Lists.newArrayList();

    final List<Vertex> vertexes = Lists.newArrayList();
    int x = 0;
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
      final RelNode rel = multiJoin.getJoinFactor(i);
      double cost = mq.getRowCount(rel);
      LeafVertex leaf = new LeafVertex(i, rel, cost, x);
      vertexes.add(leaf);

      int fieldCount = rel.getRowType().getFieldCount();
      final TargetMapping mapping = Mappings
          .offsetSource(Mappings.createIdentity(fieldCount), x,
              multiJoin.getNumTotalFields());
      factorMappings.add(mapping);

      x += fieldCount;

      // for build
      relNodes.add(Pair.of(leaf.rel, mapping));
    }
    assert x == multiJoin.getNumTotalFields();

    final List<Pair<RexNode, ImmutableBitSet>> notEdgeFilters = Lists
        .newLinkedList();
    final List<Edge> unusedEdges = Lists.newLinkedList();

    // resolve the joinEdge, splitting the joinEdges to [equalExprs,
    // leftFilters, rightFilters, nonEqualExprs]
    resolveJoinEdge(notEdgeFilters, unusedEdges, multiJoin, factorMappings);

    // If resolved edge have no available NDV, we should better skip risky
    //  cost functions.
    for (Edge edge : unusedEdges) {
      final int factor0 = edge.factors.nth(0);
      final int factor1 = edge.factors.nth(1);
      try {
        // when enforceJoinOrdering is true, we will not throw exception when
        // NDV doesn't exit.
        CostFunctions.selectivity(vertexes, factor0, factor1, relNodes,
            multiJoin, edge.resolvedCondition, edge.condition, false);
      } catch (UnsupportedOperationException e) {
        if (costFunction != CostFunctions.NATURAL
            && !(costFunction instanceof CostFunctions.UpdateLastCostFunction)
            && !(costFunction instanceof CostFunctions.UpdateFirstCostFunction)) {
          return null;
        }
      }
    }

    // Comparator to minimize intermediate result.
    // We achieve this by minimize costs (not precisely), say cost function is
    // Cij = sij * Ri * Rj,
    final Comparator<Edge> edgeComparator = new Comparator<LoptMultiJoin.Edge>() {
      public int compare(LoptMultiJoin.Edge e0, LoptMultiJoin.Edge e1) {
        return Double.compare(cost(e0), cost(e1));
      }

      private double cost(LoptMultiJoin.Edge edge) {
        assert edge.factors.cardinality() == 2 : edge.factors;
        return costFunction.cost(vertexes, relNodes, multiJoin, edge);
      }
    };

    final List<LoptMultiJoin.Edge> usedEdges = Lists.newArrayList();

    if (groupJoins) {
      ArrayListMultimap<List<String>, Edge> joinKeys2Edge = ArrayListMultimap
          .create();
      for (Edge edge : unusedEdges) {
        JoinConditionSplitRes resolvedCondition = edge.resolvedCondition;
        List<String> leftOrderedKeys = getOrderedJoinKeysWithoutCast(resolvedCondition.getLeftJoinKeys());
        List<String> rightOrderedKeys = getOrderedJoinKeysWithoutCast(resolvedCondition.getRightJoinKeys());
        if (!leftOrderedKeys.isEmpty()) {
          joinKeys2Edge.put(leftOrderedKeys, edge);
          joinKeys2Edge.put(rightOrderedKeys, edge);
        }
      }

      for (List<String> key : joinKeys2Edge.keySet()) {
        // Edges may change after makeVertex. Every time we try to makeVertex for
        // grouping, we should retain only the elements in the unusedEdges.
        List<Edge> edges = Lists.newArrayList(joinKeys2Edge.get(key));
        edges.retainAll(unusedEdges);
        if (edges.size() >= 2) {
          // For join-group, the join tree is a left-deep tree, choose the best
          // edges greedily.
          Vertex leftVertex = null;
          // NOTE that the edges is changing.
          int edgeSize = edges.size();
          for (int i = 0; i < edgeSize; i++) {
            Edge bestEdge = null;
            if (i == 0) {
              int edgeOrdinal = chooseBestEdge(edges, edgeComparator);
              bestEdge = edges.get(edgeOrdinal);
            } else {
              // get the candidate edges which is connected to the leftVertex
              ImmutableBitSet leftVertexFactor = ImmutableBitSet
                  .of(leftVertex.id);
              List<Edge> connectedEdges = Lists.newArrayList();
              for (Edge edge : edges) {
                // all factors have been updated after makeVertex.
                if (edge.factors.contains(leftVertexFactor)) {
                  connectedEdges.add(edge);
                }
              }
              assert !connectedEdges.isEmpty();
              int edgeOrdinal = chooseBestEdge(connectedEdges, edgeComparator);
              bestEdge = connectedEdges.get(edgeOrdinal);
            }

            edges.remove(bestEdge);
            int[] factors = bestEdge.factors.toArray();
            makeVertex(factors[0], factors[1], bestEdge.joinType, vertexes,
                unusedEdges, usedEdges, relNodes, multiJoin);
            Vertex vertex = vertexes.get(vertexes.size() - 1);
            assert vertex instanceof JoinVertex;
            buildJoinVertex((JoinVertex) vertex, relNodes, notEdgeFilters,
                vertexes, multiJoin, factorMappings);
            // udpate the leftVertex.
            leftVertex = vertex;
          }
        }
      }
    }

    for (;;) {
      final int edgeOrdinal = chooseBestEdge(unusedEdges, edgeComparator);
      if (LOG.isDebugEnabled()) {
        trace(vertexes, unusedEdges, usedEdges, edgeOrdinal);
      }
      final int[] factors;
      JoinRelType joinType;
      if (edgeOrdinal == -1) {
        // No more edges. Are there any un-joined vertexes?
        final Vertex lastVertex = Util.last(vertexes);
        final int z = lastVertex.factors.previousClearBit(lastVertex.id - 1);
        assert z < 0;
        break;
      } else {
        final LoptMultiJoin.Edge bestEdge = unusedEdges.get(edgeOrdinal);

        // For now, assume that the edge is between precisely two factors.
        // 1-factor conditions have probably been pushed down,
        // and 3-or-more-factor conditions are advanced. (TODO:)
        // Therefore, for now, the factors that are merged are exactly the
        // factors on this edge.
        assert bestEdge.factors.cardinality() == 2;
        factors = bestEdge.factors.toArray();
        joinType = bestEdge.joinType;
      }
      makeVertex(factors[0], factors[1], joinType, vertexes, unusedEdges,
          usedEdges, relNodes, multiJoin);
      Vertex vertex = vertexes.get(vertexes.size() - 1);
      assert vertex instanceof JoinVertex;
      buildJoinVertex((JoinVertex)vertex, relNodes, notEdgeFilters, vertexes, multiJoin,
          factorMappings);

      if (LOG.isDebugEnabled()) {
        LOG.debug(Util.last(relNodes));
      }

    }

    final Pair<RelNode, TargetMapping> top = Util.last(relNodes);
    RelNode n = relBuilder
        .push(top.left)
        .project(relBuilder.fields(top.right))
        .build();

    if (n instanceof Project && ProjectRemoveRule.isTrivial((Project) n)) {
      return top.left;
    }

    return n;
  }
  
  private void buildJoinVertex(JoinVertex vertex,
      List<Pair<RelNode, TargetMapping>> relNodes,
      List<Pair<RexNode, ImmutableBitSet>> notEdgeFilters,
      List<Vertex> vertexes, LoptMultiJoin multiJoin,
      List<TargetMapping> factorMappings) {

    JoinVertex joinVertex = (JoinVertex) vertex;
    final Pair<RelNode, TargetMapping> leftPair = relNodes
        .get(joinVertex.leftFactor);
    RelNode left = leftPair.left;
    final TargetMapping leftMapping = leftPair.right;
    final Pair<RelNode, TargetMapping> rightPair = relNodes
        .get(joinVertex.rightFactor);
    RelNode right = rightPair.left;
    final TargetMapping rightMapping = rightPair.right;

    // handle the multiFactorConditions, compose the complete join
    // condition.
    List<RexNode> conditions = Lists.newArrayList(joinVertex.conditions);
    List<JoinConditionSplitRes> resolvedConditions = Lists.newArrayList();
    final Iterator<Pair<RexNode, ImmutableBitSet>> filterIterator = notEdgeFilters
        .iterator();
    while (filterIterator.hasNext()) {
      Pair<RexNode, ImmutableBitSet> pair = filterIterator.next();
      RexNode filter = pair.left;
      ImmutableBitSet filterFactors = pair.right;
      if (!filter.isAlwaysTrue() && joinVertex.factors.contains(filterFactors)) {
        List<Integer> factorList = Lists.newArrayList();
        getOrderedLeafFactors(joinVertex, vertexes, factorList);
        JoinConditionSplitRes resolvedCondition = resolveJoinCondition(
            multiJoin, factorMappings, factorList, left, right,
            joinVertex.type, filter);
        resolvedConditions.add(resolvedCondition);
        conditions.add(filter);
        filterIterator.remove();
      }
    }

    final JoinConditionSplitRes finalResolvedCondition;
    if (resolvedConditions.isEmpty()) {
      finalResolvedCondition = joinVertex.resolvedCondition;
    } else {
      resolvedConditions.add(joinVertex.resolvedCondition);
      finalResolvedCondition = JoinConditionSplitRes.merge(resolvedConditions);
    }

    final RexNode condition = RexUtil.composeConjunction(left.getCluster()
        .getRexBuilder(), conditions, false);

    final Pair<RelNode, TargetMapping> join = buildJoin(left, right,
        condition, finalResolvedCondition, joinVertex.type, leftMapping,
        rightMapping);

    relNodes.add(join);
  }
  
  /**
   * Get the new bitSet for the originalMask by apply a TargetMapping
   * 
   * @param originalMask
   * @param mapping
   * @return
   */
  private ImmutableBitSet shuttleMask(ImmutableBitSet originalMask,
      TargetMapping mapping) {
    List<Integer> newIndexList = Lists.newArrayListWithExpectedSize(originalMask.size());
    for (Integer index : originalMask.toList()) {
      newIndexList.add(mapping.getTarget(index));
    }
    return ImmutableBitSet.of(newIndexList);
  }
  
  private double getMaxNDV(Vertex leftVertex, Vertex rightVertex, Pair<RelNode, TargetMapping> leftPair,
      Pair<RelNode, TargetMapping> rightPair,
      JoinConditionSplitRes resolvedCondition, LoptMultiJoin multiJoin, RelMetadataQuery mq) {
    // NOTE, the resolvedCondition is sensitive for factor-order, we should get
    // the right joinKeys based on the current factor-order.
    List<RexNode> leftKeys = resolvedCondition.getLeftJoinKeys();
    assert !leftKeys.isEmpty();
    ImmutableBitSet leftKeyFactors = multiJoin.getJoinFilterFactorBitmap(
        leftKeys.get(0),
        false);
    ImmutableBitSet originalLeftMask, originalRightMask;
    if (leftVertex.factors.contains(leftKeyFactors)) {
      originalLeftMask = InputFinder.bits(
          resolvedCondition.getLeftJoinKeys(), null);
      originalRightMask = InputFinder.bits(
          resolvedCondition.getRightJoinKeys(), null);
    } else {
      originalLeftMask = InputFinder.bits(
          resolvedCondition.getRightJoinKeys(), null);
      originalRightMask = InputFinder.bits(
          resolvedCondition.getLeftJoinKeys(), null);
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
  
  private Pair<RelNode, TargetMapping> buildJoin(
      RelNode left, RelNode right,
      RexNode joinCondition, JoinConditionSplitRes resolvedJoinCondition,
      JoinRelType joinType,
      TargetMapping leftMapping, TargetMapping rightMapping) {

    // if not grouped, just build the join.
    TargetMapping mapping =
        Mappings.merge(leftMapping,
            Mappings.offsetTarget(rightMapping,
                left.getRowType().getFieldCount()));
    RexVisitor<RexNode> shuttle =
        new RexPermuteInputsShuttle(mapping, left, right);
    JoinConditionSplitRes shuttledSplitRes = resolvedJoinCondition
        .accept(shuttle);
    List<RelNode> newInputs = Lists.newArrayList();

    RexBuilder rexBuilder = left.getCluster().getRexBuilder();

    List<RexNode> joinFilters = new ArrayList<>();
    joinFilters.addAll(shuttledSplitRes.getLeftFilter());
    joinFilters.addAll(shuttledSplitRes.getRightFilter());
    joinFilters.addAll(shuttledSplitRes.getNonEqualExprs());
    shuttledSplitRes.getEqualExprs().forEach(equalExpr -> {
      joinFilters.add(equalExpr.composeEqualExpr(rexBuilder));
    });

    RexNode joinFilter = RexUtil.composeConjunction(left.getCluster().getRexBuilder(), joinFilters, false);

    newInputs.add(left);
    newInputs.add(right);

    RelDataType rowType = TvrRelOptUtils.deriveJoinRowType(left.getRowType(),
        right.getRowType(), joinType, left.getCluster().getTypeFactory(),
        ImmutableList.of());

    LogicalJoin logicalJoin = LogicalJoin.create(left, right, joinFilter, new HashSet<>(), joinType, true, ImmutableList.of());
    return Pair.of(logicalJoin, mapping);
  }
  
  /**
   * NOTE that, the resolve result is factors-order sensitive.When resolving the
   * join condition, the smaller factor is as the left RelNode while the bigger
   * factor is as the right RelNode.
   * 
   * @param notEdgeFilters
   * @param unusedEdges
   * @param multiJoin
   * @param factorMappings
   */
  void resolveJoinEdge(
      List<Pair<RexNode, ImmutableBitSet>> notEdgeFilters,
      List<Edge> unusedEdges, LoptMultiJoin multiJoin,
      List<TargetMapping> factorMappings) {
    List<Triple<RexNode, ImmutableBitSet, JoinRelType>> nonBinaryConditions = Lists.newArrayList();
    MultiJoin multiJoinRel = multiJoin.getMultiJoinRel();
    if (multiJoinRel.isFullOuterJoin()) {
      // It is a bit hacky, if multiJoinRel is an Full Outer Join, its join
      // types are [INNER, INNER], but there is another call to identify this.
      JoinRelType type = JoinRelType.FULL;
      // when it comes to full-outer join.
      // 1).There will be only two join inputs. 2). multiJoin.getJoinFilters()
      // will return all of the join conditions.
      assert multiJoinRel.getInputs().size() == 2;
      RexNode condition = RexUtil.composeConjunction(multiJoinRel.getCluster()
          .getRexBuilder(), multiJoin.getJoinFilters(), false);
      List<Integer> factors = Lists.newArrayList(0, 1);
      JoinConditionSplitRes resolvedCondition = resolveJoinCondition(multiJoin,
          factorMappings, factors, multiJoinRel.getInput(0),
          multiJoinRel.getInput(1), type, condition);
      if (resolvedCondition.getEqualExprs().isEmpty()) {
        nonBinaryConditions.add(Triple.of(condition, ImmutableBitSet.of(0, 1),
            type));
      } else {
        Edge edge = multiJoin.createEdge(condition, resolvedCondition, type);
        unusedEdges.add(edge);
      }
    } else {
      // Handle non-outer join filters.
      // Reconstruct the join edge based on the corresponding join factors. NOTE
      // that the joinFilters in multiJoin have
      // been flattened, including the equal expressions("A.id = B.id" and
      // "A.item = B.item" are two joinFilters) and others("A.key > B.key" is
      // also
      // a joinFilter in multiJoin).
      ArrayListMultimap<ImmutableBitSet, RexNode> binaryConditions = ArrayListMultimap
          .create();
      ArrayListMultimap<ImmutableBitSet, RexNode> singletonConditions = ArrayListMultimap
          .create();
      for (RexNode node : multiJoin.getJoinFilters()) {
        ImmutableBitSet filterFactors = multiJoin.getJoinFilterFactorBitmap(
            node, false);
        if (filterFactors.cardinality() == 1) {
          singletonConditions.put(filterFactors, node);
        } else if (filterFactors.cardinality() == 2) {
          binaryConditions.put(filterFactors, node);
        } else {
          nonBinaryConditions.add(Triple.of(node, filterFactors,
              JoinRelType.INNER));
        }
      }

      // for inner join filters:
      // 1. merge the edge2Conditions and singleFactor2Conditions
      // 2. resolve joinCondition
      // 3. create Edge.
      for (ImmutableBitSet twoFactor : binaryConditions.keySet()) {
        JoinRelType type = JoinRelType.INNER;
        List<RexNode> completeConditions = Lists.newArrayList(binaryConditions
            .get(twoFactor));
        // The list is sorted.
        List<Integer> factors = twoFactor.toList();
        ImmutableBitSet smallFactor = ImmutableBitSet.of(factors.get(0));
        ImmutableBitSet bigFactor = ImmutableBitSet.of(factors.get(1));
        if (singletonConditions.containsKey(smallFactor)) {
          completeConditions.addAll(singletonConditions.get(smallFactor));
          singletonConditions.removeAll(smallFactor);
        }
        if (singletonConditions.containsKey(bigFactor)) {
          completeConditions.addAll(singletonConditions.get(bigFactor));
          singletonConditions.removeAll(bigFactor);
        }

        RexNode condition = RexUtil.composeConjunction(multiJoinRel
            .getCluster().getRexBuilder(), completeConditions, false);
        JoinConditionSplitRes resolvedCondition = resolveJoinCondition(
            multiJoin, factorMappings, factors,
            multiJoinRel.getInput(factors.get(0)),
            multiJoinRel.getInput(factors.get(1)), type, condition);

        // only conditions with equalExprs is treated as a edge.
        if (resolvedCondition.getEqualExprs().isEmpty()) {
          nonBinaryConditions.add(Triple.of(condition, twoFactor, type));
        } else {
          Edge edge = multiJoin.createEdge(condition, resolvedCondition, type);
          unusedEdges.add(edge);
        }
      }

      if (!singletonConditions.isEmpty()) {
        JoinRelType type = JoinRelType.INNER;
        for (ImmutableBitSet factor : singletonConditions.keySet()) {
          List<RexNode> singletonFilters = singletonConditions.get(factor);
          RexNode condition = RexUtil.composeConjunction(multiJoinRel
              .getCluster().getRexBuilder(), singletonFilters, false);
          nonBinaryConditions.add(Triple.of(condition, factor, type));
        }
      }

      // handle outer join filters.
      // since the outerJoinConditions have been combined based on the factors,
      // so
      // we needn't do the combination like the inner join filters.
      for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
        JoinRelType type = multiJoin.multiJoin.getJoinTypes().get(i);
        RexNode condition = multiJoin.getOuterJoinCond(i);
        if (condition != null) {
          ImmutableBitSet filterFactors = multiJoin.getJoinFilterFactorBitmap(
              condition, false);
          if (filterFactors.cardinality() == 2) {
            // the filter maybe has equalExprs for factor(i)
            if (filterFactors.contains(ImmutableBitSet.of(i))) {
              // The array is sorted.
              List<Integer> factors = filterFactors.toList();
              JoinConditionSplitRes resolvedCondition = resolveJoinCondition(
                  multiJoin, factorMappings, factors,
                  multiJoinRel.getInput(factors.get(0)),
                  multiJoinRel.getInput(factors.get(1)), type, condition);

              // only conditions with equalExprs is treated as a edge.
              if (resolvedCondition.getEqualExprs().isEmpty()) {
                nonBinaryConditions.add(Triple.of(condition, filterFactors,
                    type));
              } else {
                Edge edge = multiJoin.createEdge(condition, resolvedCondition,
                    type);
                unusedEdges.add(edge);
              }
            } else {
              // e.g. A join B left outer join C on A.id = B.id
              // the filter is not a edge since it has no equal condition between factor(i) and others.
              ImmutableBitSet outerConditionFactors = ImmutableBitSet.builder()
                  .addAll(filterFactors).set(i).build();
              nonBinaryConditions.add(Triple.of(condition,
                  outerConditionFactors, type));
            }
          } else if (filterFactors.cardinality() == 1
              || filterFactors.cardinality() == 0) {
            int major = type == JoinRelType.LEFT ? i - 1 : i;
            nonBinaryConditions.add(Triple.of(condition,
                ImmutableBitSet.range(major, major + 2), type));
          } else {
            nonBinaryConditions.add(Triple.of(condition, filterFactors, type));
          }
        }
      }
    }

    // handle the connection of the join graph
    List<ImmutableBitSet> connectedFactorsList = Lists.newArrayList();
    for (Edge edge: unusedEdges) {
      ImmutableBitSet factors = edge.factors;
      adjustConnection(connectedFactorsList, factors);
    }
    
    for (Triple<RexNode, ImmutableBitSet, JoinRelType> e : nonBinaryConditions) {
      ImmutableBitSet factors = e.getMiddle();
      if (factors.cardinality() >= 2) {
        // chose two factors to create a edge.
        ImmutableBitSet choseFactors = ImmutableBitSet.of(factors.nth(0),
            factors.nth(factors.cardinality() - 1));
        if (!factors.isEmpty()
            && adjustConnection(connectedFactorsList, choseFactors)) {
          // when the nonBinaryConditions affect the connection, we need to add
          // an
          // edge.
          Edge edge = multiJoin.createEdge(choseFactors, e.getRight());
          unusedEdges.add(edge);
        }
      }
    }
    
    // final adjustment to handle Cartesian join, we must ensure the join order
    // so that naive rule can leverage
    for (int i = 0; i < multiJoin.getNumJoinFactors() -1; i++) {
      ImmutableBitSet factors = ImmutableBitSet.of(i, i+1);
      // connections: {0}, {1}, {2,3}
      if (adjustConnection(connectedFactorsList, factors)) {
        // add an edge that with no join condition
        Edge edge = multiJoin.createEdge(factors, JoinRelType.INNER);
        unusedEdges.add(edge);
      }
    }

    nonBinaryConditions.forEach(e -> notEdgeFilters.add(Pair.of(e.getLeft(),
        e.getMiddle())));
    // Sort by maximum factors and minimum factors so naive rule can leverage
    unusedEdges.sort(new Comparator<LoptMultiJoin.Edge>() {
      public int compare(LoptMultiJoin.Edge e0, LoptMultiJoin.Edge e1) {
        int[] e0Factors = e0.factors.toArray();
        int[] e1Factors = e1.factors.toArray();
        if (e0Factors[1] < e1Factors[1]) {
          return -1;
        } else if (e0Factors[1] > e1Factors[1]) {
          return 1;
        } else {
          return Integer.compare(e0Factors[0], e1Factors[0]);
        }
      }
    });
  }
  
  /**
   * return true if change the conenctedFactorsList.
   * 
   * @param connectedFactorsList
   * @param factors
   */
  static boolean adjustConnection(List<ImmutableBitSet> connectedFactorsList,
      ImmutableBitSet factors) {
    List<Integer> merges = Lists.newArrayList();
    for (int i = 0; i < connectedFactorsList.size(); i++) {
      ImmutableBitSet connectedFactors = connectedFactorsList.get(i);
      if (connectedFactors.contains(factors)) {
        return false;
      }
      if (factors.intersects(connectedFactors)) {
        merges.add(i);
      }
    }
    if (merges.isEmpty()) {
      // new connected graph.
      connectedFactorsList.add(factors);
    } else {
      // merge the factors to construct the new connected graph
      ImmutableBitSet mergedConnectedFactors = factors
          .union(connectedFactorsList.get(merges.get(0)));
      for (int i = 1; i < merges.size(); i++) {
        mergedConnectedFactors = mergedConnectedFactors
            .union(connectedFactorsList.get(merges.get(i)));
      }
      connectedFactorsList.set(merges.get(0), mergedConnectedFactors);
      for (int i = 1; i < merges.size(); i++) {
        connectedFactorsList.remove(merges.get(i));
      }
    }
    return true;
  }

  /**
   * NOTE, the inputRef in the resolveResult is based on the fields of the
   * multiJoin which is convenient to do join group and to build the
   * OdpsMultiJoin.
   * 
   * @param multiJoin
   * @param mappings
   * @param factors, in the order of the join order.
   * @param joinCondition
   * @return
   */
  private JoinConditionSplitRes resolveJoinCondition(LoptMultiJoin multiJoin,
      List<TargetMapping> mappings, List<Integer> factors, RelNode left,
      RelNode right, JoinRelType type, RexNode joinCondition) {
    assert mappings.size() > 1;
    // 1. shift the inputRef based on the join factors.
    TargetMapping mapping = mappings.get(factors.get(0));
    int offset = multiJoin.getNumFieldsInJoinFactor(factors.get(0));
    for (int i = 1; i < factors.size(); i++) {
      int factor = factors.get(i);
      mapping = Mappings.merge(mapping,
          Mappings.offsetTarget(mappings.get(factor), offset));
      offset += multiJoin.getNumFieldsInJoinFactor(factor);
    }
    final RexVisitor<RexNode> shuttle = new RexPermuteInputsShuttle(mapping,
        left, right);
    joinCondition = joinCondition.accept(shuttle);

    // 2. analyze the join condition
    JoinConditionSplitRes resolveResult = TvrRelOptUtils
        .analyzeJoinCondition(left, right, joinCondition, null, type, true,
            true, true);

    // 3. shift the inputRef back based on the multiJoin for further operations.
    Mapping inverseMapping = mapping.inverse();
    final RexVisitor<RexNode> inverseShuttle = new RexPermuteInputsShuttle(
        inverseMapping, left, right);
    List<EqualExprs> newEqualExprs = Lists.newArrayList();
    List<RexNode> newLeftFilters = Lists.newArrayList();
    List<RexNode> newRightFilters = Lists.newArrayList();
    List<RexNode> newNonEqualExprs = Lists.newArrayList();
    for (EqualExprs equalExprs : resolveResult.getEqualExprs()) {
      RexNode leftRexNode = equalExprs.getLeftReference();
      RexNode rightRexNode = equalExprs.getRightReference();
      RexNode newLeftRexNode = leftRexNode.accept(inverseShuttle);
      RexNode newRightRexNode = rightRexNode.accept(inverseShuttle);
      newEqualExprs.add(EqualExprs.of(newLeftRexNode, newRightRexNode, equalExprs.isComparingNulls()));
    }
    for (RexNode rexNode : resolveResult.getLeftFilter()) {
      newLeftFilters.add(rexNode.accept(inverseShuttle));
    }
    for (RexNode rexNode : resolveResult.getRightFilter()) {
      // NOTE that the inputRef of rightFilters is based on the right relNode.
      newRightFilters.add(RexUtil.shift(rexNode,
          left.getRowType().getFieldCount()).accept(inverseShuttle));
    }
    for (RexNode rexNode : resolveResult.getNonEqualExprs()) {
      newNonEqualExprs.add(rexNode.accept(inverseShuttle));
    }

    return new JoinConditionSplitRes(newEqualExprs, newLeftFilters,
        newRightFilters, newNonEqualExprs);
  }

  private void makeVertex(int majorFactor, int minorFactor,
      JoinRelType joinType, List<Vertex> vertexes, List<Edge> unusedEdges,
      List<Edge> usedEdges,
      List<Pair<RelNode, TargetMapping>> relNodes,
      LoptMultiJoin multiJoin) {
    final Vertex majorVertex = vertexes.get(majorFactor);
    final Vertex minorVertex = vertexes.get(minorFactor);

    // Find the join conditions. All conditions whose factors are now all in
    // the join can now be used.
    final int v = vertexes.size();
    final ImmutableBitSet newFactors =
        majorVertex.factors
            .rebuild()
            .addAll(minorVertex.factors)
            .set(v)
            .build();

    final List<JoinConditionSplitRes> resolvedConditions = Lists.newArrayList();
    final List<RexNode> conditions = Lists.newArrayList();
    final Iterator<Edge> edgeIterator = unusedEdges.iterator();
    while (edgeIterator.hasNext()) {
      LoptMultiJoin.Edge edge = edgeIterator.next();
      if (newFactors.contains(edge.factors)) {
        // The edge's factors must appear in both the majorFactor and the
        // minorFactor, otherwise the edge must have been merged in the
        // majorVertex or in the minorVertex.
        // So we can infer that the edge's JoinConditionSplitRes is still
        // effective
        // unless we may should adjust the equalExprs, leftFilters and the
        // rightFilters based
        // on the majorFacttor and the minorFactor.
        if (!edge.resolvedCondition.getLeftJoinKeys().isEmpty()) {
          JoinConditionSplitRes newResolvedCondition = adjustResolvedCondition(
              edge, majorVertex.factors, minorVertex.factors, multiJoin);
          resolvedConditions.add(newResolvedCondition);
        }
        
        conditions.add(edge.condition);
        edgeIterator.remove();
        usedEdges.add(edge);
      }
    }
    final JoinConditionSplitRes finalResolvedCondition = JoinConditionSplitRes
        .merge(resolvedConditions);

    double cost = majorVertex.cost
        * minorVertex.cost
        * CostFunctions.selectivity(vertexes, majorFactor, minorFactor,
            relNodes, multiJoin, finalResolvedCondition, RexUtil
                .composeConjunction(multiJoin.multiJoin.getCluster()
                    .getRexBuilder(), conditions, false), false);
    final Vertex newVertex = new JoinVertex(v, majorFactor, minorFactor,
        newFactors, cost, ImmutableList.copyOf(conditions),
        finalResolvedCondition, joinType);
    vertexes.add(newVertex);

    // Re-compute selectivity of edges above the one just chosen.
    // Suppose that we just chose the edge between "product" (10k rows) and
    // "product_class" (10 rows).
    // Both of those vertices are now replaced by a new vertex "P-PC".
    // This vertex has fewer rows (1k rows) -- a fact that is critical to
    // decisions made later. (Hence "greedy" algorithm not "simple".)
    // The adjacent edges are modified.
    final ImmutableBitSet merged =
        ImmutableBitSet.of(minorFactor, majorFactor);
    for (int i = 0; i < unusedEdges.size(); i++) {
      final LoptMultiJoin.Edge edge = unusedEdges.get(i);
      ImmutableBitSet intersect = edge.factors.intersect(merged);
      if (!intersect.isEmpty()) {
        ImmutableBitSet oldEdgeFactors = edge.factors;
        ImmutableBitSet newEdgeFactors =
            edge.factors
                .rebuild()
                .removeAll(newFactors)
                .set(v)
                .build();
        assert newEdgeFactors.cardinality() == 2;
        edge.setFactors(newEdgeFactors);

        assert intersect.cardinality() == 1;
        // If edge lower factors are merged, the new edge join type should
        // reverse.
        if (oldEdgeFactors.nth(0) == intersect.nth(0)) {
          edge.reverseJoinType();
        }
      }
    }
  }
  
  /**
   * Adjust the JoinConditionSplitRes based on the leftFactors and rightFactors,
   * since the JoinConditionSplitRes is factors-order sensitive
   * 
   * @param edge
   * @param leftFactors
   * @param rightFactors
   * @return
   */
  private JoinConditionSplitRes adjustResolvedCondition(
      LoptMultiJoin.Edge edge, ImmutableBitSet leftFactors,
      ImmutableBitSet rightFactors, LoptMultiJoin multiJoin) {
    List<RexNode> leftKeys = edge.resolvedCondition.getLeftJoinKeys();
    assert !leftKeys.isEmpty();
    ImmutableBitSet leftKeyFactors = multiJoin.getJoinFilterFactorBitmap(
        leftKeys.get(0),
        false);
    if (leftFactors.contains(leftKeyFactors)) {
      return edge.resolvedCondition;
    }
    return edge.resolvedCondition.swapLeftRight();
  }
  
  /**
   * @param joinVertex
   * @return
   */
  private void getOrderedLeafFactors(JoinVertex joinVertex,
      List<Vertex> vertexes, List<Integer> factors) {
    Vertex left = vertexes.get(joinVertex.leftFactor);
    if (left instanceof LeafVertex) {
      factors.add(left.id);
    } else {
      // JoinVertex
      getOrderedLeafFactors((JoinVertex) left, vertexes, factors);
    }

    Vertex right = vertexes.get(joinVertex.rightFactor);
    if (right instanceof LeafVertex) {
      factors.add(right.id);
    } else {
      // JoinVertex
      getOrderedLeafFactors((JoinVertex) right, vertexes, factors);
    }
  }
  
  private void trace(List<Vertex> vertexes,
      List<LoptMultiJoin.Edge> unusedEdges, List<LoptMultiJoin.Edge> usedEdges,
      int edgeOrdinal) {
    if (edgeOrdinal >= 0) {
    LOG.debug("bestEdge: " + unusedEdges.get(edgeOrdinal));
    }
    LOG.debug("vertexes:");
    for (Vertex vertex : vertexes) {
      LOG.debug(vertex);
    }
    LOG.debug("unused edges:");
    for (LoptMultiJoin.Edge edge : unusedEdges) {
      LOG.debug(edge);
    }
    LOG.debug("edges:");
    for (LoptMultiJoin.Edge edge : usedEdges) {
      LOG.debug(edge);
    }
  }

  int chooseBestEdge(List<LoptMultiJoin.Edge> edges,
      Comparator<LoptMultiJoin.Edge> comparator) {
    return minPos(edges, comparator);
  }

  /**
   * For comparison of join Keys
   *
   * @param rexNodes
   *
   * @return
   */
  public static List<String> getOrderedJoinKeysWithoutCast(
          List<RexNode> rexNodes) {
    List<String> joinKeys = Lists.newArrayList();
    for (RexNode rexNode : rexNodes) {
      String joinKey = getJoinKeyWithoutCast(rexNode).toString();
      joinKeys.add(joinKey);
    }

    Collections.sort(joinKeys);
    return joinKeys;
  }

  private static RexNode getJoinKeyWithoutCast(RexNode rexNode) {
    if (rexNode instanceof RexInputRef) {
      return rexNode;
    } else {
      assert rexNode instanceof RexCall;
      return (RexCall) rexNode;
    }
  }

  /** Returns the index within a list at which compares least according to a
   * comparator.
   *
   * <p>In the case of a tie, returns the earliest such element.</p>
   *
   * <p>If the list is empty, returns -1.</p>
   */
  static <E> int minPos(List<E> list, Comparator<E> fn) {
    if (list.isEmpty()) {
      return -1;
    }
    E eBest = list.get(0);
    int iBest = 0;
    for (int i = 1; i < list.size(); i++) {
      E e = list.get(i);
      if (fn.compare(e, eBest) < 0) {
        eBest = e;
        iBest = i;
      }
    }
    return iBest;
  }

  /** Participant in a join (relation or join). */
  abstract static class Vertex {
    final int id;

    protected final ImmutableBitSet factors;
    final double cost;

    Vertex(int id, ImmutableBitSet factors, double cost) {
      this.id = id;
      this.factors = factors;
      this.cost = cost;
    }
  }

  /** Relation participating in a join. */
  static class LeafVertex extends Vertex {
    protected final RelNode rel;
    final int fieldOffset;

    LeafVertex(int id, RelNode rel, double cost, int fieldOffset) {
      super(id, ImmutableBitSet.of(id), cost);
      this.rel = rel;
      this.fieldOffset = fieldOffset;
    }

    @Override public String toString() {
      return "LeafVertex(id: " + id
          + ", cost: " + Util.human(cost)
          + ", factors: " + factors
          + ", fieldOffset: " + fieldOffset
          + ")";
    }
  }

  /** Participant in a join which is itself a join. */
  static class JoinVertex extends Vertex {
    final int leftFactor;
    final int rightFactor;
    /** Zero or more join conditions. All are in terms of the original input
     * columns (not in terms of the outputs of left and right input factors). */
    final ImmutableList<RexNode> conditions;
    private JoinRelType type;
    final JoinConditionSplitRes resolvedCondition;

    JoinVertex(int id, int leftFactor, int rightFactor,
        ImmutableBitSet factors, double cost,
        ImmutableList<RexNode> conditions,
        JoinConditionSplitRes resolvedCondition,
        JoinRelType selfType) {
      super(id, factors, cost);
      this.leftFactor = leftFactor;
      this.rightFactor = rightFactor;
      this.conditions = Preconditions.checkNotNull(conditions);
      this.resolvedCondition = resolvedCondition;
      this.type = selfType;
    }

    @Override public String toString() {
      return "JoinVertex(id: " + id
          + ", cost: " + Util.human(cost)
          + ", factors: " + factors
          + ", leftFactor: " + leftFactor
          + ", rightFactor: " + rightFactor
          + ", joinType: " + type
          + ")";
    }
  }
}

// End GooJoinOrderingRule.java

