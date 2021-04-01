package org.apache.calcite.rel.tvr.rules.dbtoaster;

import com.google.common.collect.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.tvr.utils.Config;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrRelOptUtils;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;

import java.util.*;

import static java.util.stream.Collectors.toList;
import static org.apache.calcite.plan.RelOptUtil.InputReferencedVisitor;

import static org.apache.calcite.rel.tvr.rules.dbtoaster.LoptMultiJoin.*;
import static org.apache.calcite.rel.tvr.rules.dbtoaster.GooJoinOrderingRule.*;
import static org.apache.calcite.rel.tvr.utils.TvrRelOptUtils.*;

public class DBTGooJoinOrderingRule extends GooJoinOrderingRule {

  public DBTGooJoinOrderingRule(boolean groupJoins, CostFunction costFunction,
                                String description) {
    super(groupJoins, costFunction, description);
  }

  public RelNode transformDBT(MultiJoin multiJoinRel,
      RelBuilder relBuilder, Integer relUpdated) {

    // fall back to regular GooJoinOrderingRule
    // if the relUpdated is null
    // if it only has 2 inputs
    if (relUpdated == null || multiJoinRel.getInputs().size() <= 2) {
      return this.transform(multiJoinRel, relBuilder);
    }
    // full outer join MultiJoin only has 2 inputs, this multiJoin cannot be full outer join
    assert ! multiJoinRel.isFullOuterJoin();

    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final Config
        config = TvrContext.getInstance(multiJoinRel.getCluster()).getConfig();
    // for build the final relNode tree.
    List<Pair<RelNode, Mappings.TargetMapping>> relNodes = Lists.newArrayList();

    final LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);

    // inputRef mappings for each factor, source is the inputRef based on the
    // multiJoin while target is the inputRef based on the factor of the
    // multJoin.
    final List<Mappings.TargetMapping> factorMappings = Lists.newArrayList();

    final List<Vertex> vertexes = Lists.newArrayList();
    int x = 0;
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
      final RelNode rel = multiJoin.getJoinFactor(i);
      double cost = mq.getRowCount(rel);
      LeafVertex leaf = new LeafVertex(i, rel, cost, x);
      vertexes.add(leaf);

      int fieldCount = rel.getRowType().getFieldCount();
      final Mappings.TargetMapping mapping = Mappings
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
        if (costFunction != CostFunctions.NATURAL &&
            ! (costFunction instanceof CostFunctions.UpdateLastCostFunction)) {
          return null;
        }
      }
    }

    final List<Edge> usedEdges = Lists.newArrayList();

    // we now have resolved all the join edges, start dbtoaster rules

    // remove the edges that are related to the relNode we want to remove
    List<Edge> remainingEdges = unusedEdges.stream().filter(edge ->
        !edge.factors.get(relUpdated)).collect(toList());

    // fall back to regular GooJoinOrderingRule
    // if there are no edges left to form additional MultiJoin
    if (remainingEdges.isEmpty()) {
      return this.transform(multiJoinRel, relBuilder);
    }

    // build a join connection graph using the remaining edges
    List<ImmutableBitSet> remainingConnectedFactorsList = Lists.newArrayList();
    for (Edge edge: remainingEdges) {
      adjustConnection(remainingConnectedFactorsList, edge.factors);
    }

    for (ImmutableBitSet connectedFactors : remainingConnectedFactorsList) {
      // build a MultiJoin if the connected factors have more than 1 relNodes
      if (connectedFactors.cardinality() > 1) {
        // build the multiJoinVertex
        makeMultiJoinVertex(
            unusedEdges, usedEdges, connectedFactors, vertexes, multiJoin);
        // build the multiJoin RelNode based on the vertex
        MultiJoinVertex
            multiJoinVertex = (MultiJoinVertex) Util.last(vertexes);
        buildMultiJoin(multiJoinVertex, relNodes, notEdgeFilters, multiJoin);
      }
    }

    // the remaining connected edges should be used by now
    // all unused edges should be connected to the removed relNodes
    assert unusedEdges.stream().allMatch(edge -> edge.factors.get(relUpdated));
    // all unused edges should form exactly one connected component
    List<ImmutableBitSet> finalConnectedFactorsList = Lists.newArrayList();
    for (Edge edge: unusedEdges) {
      adjustConnection(finalConnectedFactorsList, edge.factors);
    }
    assert finalConnectedFactorsList.size() == 1;
    // build the final top-level multiJoinVertex
    makeMultiJoinVertex(unusedEdges, usedEdges,
        finalConnectedFactorsList.get(0), vertexes, multiJoin);
    MultiJoinVertex
        multiJoinVertex = (MultiJoinVertex) Util.last(vertexes);
    buildMultiJoin(multiJoinVertex, relNodes, notEdgeFilters, multiJoin);

    final Pair<RelNode, Mappings.TargetMapping> top = Util.last(relNodes);
    MultiJoin topMultiJoin = (MultiJoin) top.left;

    // before transforming top MultiJoin to OdpsMultiJoin
    // check if any child is a MultiJoin that does't need further reordering by dbtoaster
    // when an input MultiJoin:
    //  a) inputs are all dimension tables,
    //  or b) only have 2 inputs, for example, A JOIN B, dbtoaster can't split it further into any more joins

    TvrContext ctx = TvrContext.getInstance(topMultiJoin.getCluster());
    for (int i = 0; i < topMultiJoin.getInputs().size(); i++) {
      RelNode input = topMultiJoin.getInput(i);
      if (input instanceof MultiJoin) {
        if (input.getInputs().size() == 2 || ctx.getDimensionTables().containsAll(
            TvrUtils.collectUpdateTableNames(input))) {
          // use the original GooJoinOrderingRule to transform the input to OdpsMultiJoin
          RelNode transformed = this.transform((MultiJoin) input, relBuilder);
          topMultiJoin.replaceInput(i, transformed);
        }
      }
    }

    // use the original GooJoinOrderingRule to transform only the top MultiJoin
    RelNode transform = this.transform((MultiJoin) top.left, relBuilder);

    RelNode p = relBuilder
        .push(transform)
        .project(relBuilder.fields(top.right))
        .build();

    if (p instanceof Project && ProjectRemoveRule.isTrivial((Project) p)) {
      return transform;
    } else {
      return p;
    }
  }

  private void buildMultiJoin(MultiJoinVertex multiJoinVertex,
      List<Pair<RelNode, Mappings.TargetMapping>> relNodes,
      List<Pair<RexNode, ImmutableBitSet>> notEdgeFilters,
      LoptMultiJoin multiJoin) {
    RelOptCluster cluster = multiJoin.getMultiJoinRel().getCluster();

    // handle the multiFactorConditions, compose the complete join condition.
    List<RexNode> conditions = Lists.newArrayList(multiJoinVertex.conditions);
    // find additional conditions from notEdgeFilters
    final Iterator<Pair<RexNode, ImmutableBitSet>> filterIterator = notEdgeFilters
        .iterator();
    while (filterIterator.hasNext()) {
      Pair<RexNode, ImmutableBitSet> pair = filterIterator.next();
      RexNode filter = pair.left;
      ImmutableBitSet filterFactors = pair.right;
      if (!filter.isAlwaysTrue() && multiJoinVertex.factors.contains(filterFactors)) {
        conditions.add(filter);
        filterIterator.remove();
      }
    }

    // prepare new target mappings from original MultiJoin to the new join
    List<Pair<RelNode, Mappings.TargetMapping>> inputs = new ArrayList<>();
    for (Integer factor : multiJoinVertex.multiJoinFactors) {
      inputs.add(relNodes.get(factor));
    }

    Pair<RelNode, Mappings.TargetMapping> leftMost = inputs.get(0);
    Mappings.TargetMapping mapping = leftMost.right;
    int offset = leftMost.left.getRowType().getFieldCount();

    for (int i = 1; i < inputs.size(); i++) {
      Pair<RelNode, Mappings.TargetMapping> right = inputs.get(i);
      mapping = Mappings.merge(mapping, Mappings.offsetTarget(right.right, offset));
      offset += right.left.getRowType().getFieldCount();
    }

    List<RelNode> inputRels = inputs.stream().map(p -> p.left).collect(toList());
    RelNode[] inputRelArray = inputRels.toArray(new RelNode[0]);
    RexVisitor<RexNode> shuttle = new RexPermuteInputsShuttle(mapping, inputRelArray);

    // extract joinFilters, a list of inner and full outer join conditions
    List<RexNode> shuttledJoinFilterList = new ArrayList<>();
    // extract outerJoinConditions, map from the input index to the conditions
    Multimap<Integer, RexNode> shuttledOuterJoinConditionMap = LinkedHashMultimap.create();

    // full outer join should already fall back to the original GooJoinOrderingRule
    assert ! multiJoin.getMultiJoinRel().isFullOuterJoin();

    // mapping from a field index to the rel node index
    Map<Integer, Integer> fieldsToRelIndex = new LinkedHashMap<>();
    ImmutableBitSet.Builder outerJoinFieldsBuilder = ImmutableBitSet.builder();
    offset = 0;
    for (int i = 0; i < inputRels.size(); i++) {
      JoinRelType type = multiJoinVertex.types.get(i);
      int fieldCount = inputRels.get(i).getRowType().getFieldCount();
      if (type == JoinRelType.LEFT || type == JoinRelType.RIGHT) {
        outerJoinFieldsBuilder.set(offset, offset + fieldCount);
      }
      for (int field = offset; field < offset + fieldCount; field++) {
        fieldsToRelIndex.put(field, i);
      }
      offset += fieldCount;
    }
    ImmutableBitSet outerJoinFields = outerJoinFieldsBuilder.build();

    for (RexNode condition : conditions) {
      // permute the condition field index using the mapping
      RexNode shuttledCondition = condition.accept(shuttle);
      // find all input references of the shuttled condition
      InputReferencedVisitor inputRefVisitor = new InputReferencedVisitor();
      shuttledCondition.accept(inputRefVisitor);
      ImmutableBitSet inputRefs = ImmutableBitSet.of(inputRefVisitor.inputPosReferenced);
      // check if this condition references an outer join relNode
      ImmutableBitSet intersect = inputRefs.intersect(outerJoinFields);
      if (intersect.isEmpty()) {
        // not an outer join condition
        shuttledJoinFilterList.add(shuttledCondition);
      } else {
        // find the relNode index of these conditions
        Set<Integer> rels = new LinkedHashSet<>();
        for (Integer field : intersect) {
          rels.add(fieldsToRelIndex.get(field));
        }
        assert rels.size() == 1;
        int factor = rels.iterator().next();
        shuttledOuterJoinConditionMap.put(factor, shuttledCondition);
      }
    }

    // compose conditions using AND
    RexNode shuttledJoinFilters =
        RexUtil.composeConjunction(cluster.getRexBuilder(), shuttledJoinFilterList, false);

    // the outer join conditions for each factor, null if there's no outer join condition
    List<RexNode> shuttledJOuterJoinConditions = new ArrayList<>();
    for (int i = 0; i < inputRels.size(); i++) {
      Collection<RexNode> rexNodes = shuttledOuterJoinConditionMap.get(i);
      if (rexNodes.isEmpty()) {
        shuttledJOuterJoinConditions.add(null);
      } else {
        RexNode shuttledJOuterJoinCondition = RexUtil
            .composeConjunction(cluster.getRexBuilder(), rexNodes, false);
        shuttledJOuterJoinConditions.add(shuttledJOuterJoinCondition);
      }
    }

//    RexNode joinCondition =
//        RexUtil.composeConjunction(cluster.getRexBuilder(), conditions, false);
//    RexNode shuttledCondition = joinCondition.accept(shuttle);

    RelDataType rowType = leftMost.left.getRowType();
    for (int i = 1; i < inputs.size(); i++) {
      RelDataType rightRowType = inputs.get(i).left.getRowType();
      JoinRelType joinType = multiJoinVertex.types.get(i);
      rowType = TvrRelOptUtils.deriveJoinRowType(rowType,
          rightRowType, joinType, cluster.getTypeFactory(),
          ImmutableList.of());
    }

    boolean isFullOuterJoin = false;
    List<JoinRelType> joinTypes = new ArrayList<>();
    List<ImmutableBitSet> projFields = new ArrayList<>();
    Map<Integer, ImmutableIntList> joinFieldRefCountsMap = new LinkedHashMap<>();
    for (int i = 0; i < inputs.size(); i++) {
      joinTypes.add(multiJoinVertex.types.get(i));
      // null means project all fields
      projFields.add(null);

      // we do not need to fill in joinFieldRefCountsMap since no one is using it
      joinFieldRefCountsMap.put(i, ImmutableIntList.of());
    }

    MultiJoin newMultiJoin =
        new MultiJoin(cluster, inputRels, shuttledJoinFilters, rowType,
            isFullOuterJoin, shuttledJOuterJoinConditions, joinTypes, projFields,
            ImmutableMap.copyOf(joinFieldRefCountsMap), null);

    relNodes.add(Pair.of(newMultiJoin, mapping));

  }

  private void makeMultiJoinVertex(
      List<Edge> unusedEdges,
      List<Edge> usedEdges,
      ImmutableBitSet connectedFactors,
      List<Vertex> vertexes,
      LoptMultiJoin multiJoin) {

    final int v = vertexes.size();

    ImmutableBitSet.Builder newFactorsBuilder = ImmutableBitSet.builder();
    for (Integer factor : connectedFactors) {
      newFactorsBuilder.addAll(vertexes.get(factor).factors);
    }
    newFactorsBuilder.set(v);
    ImmutableBitSet newFactors = newFactorsBuilder.build();

    final List<RexNode> conditions = Lists.newArrayList();
    final Iterator<Edge> edgeIterator = unusedEdges.iterator();

    // initialize all join types to INNER (not null-generating)
    // the join types will be adjusted when iterating each edge
    Map<Integer, JoinRelType> typesMap = new LinkedHashMap<>();
    for (Integer factor : connectedFactors) {
      typesMap.put(factor, JoinRelType.INNER);
    }

    while (edgeIterator.hasNext()) {
      Edge edge = edgeIterator.next();
      if (newFactors.contains(edge.factors)) {
        conditions.add(edge.condition);
        edgeIterator.remove();
        usedEdges.add(edge);

        // determine the join type of factors
        assert edge.factors.cardinality() == 2;
        int leftFactor = edge.factors.toArray()[0];
        int rightFactor = edge.factors.toArray()[1];

        switch (edge.joinType) {
        case INNER:
          break;
        case LEFT:
          // right factor is null-generating
          typesMap.put(rightFactor, JoinRelType.LEFT);
          break;
        case RIGHT:
          typesMap.put(leftFactor, JoinRelType.RIGHT);
          break;
        case FULL:
          typesMap.put(leftFactor, JoinRelType.FULL);
          typesMap.put(rightFactor, JoinRelType.FULL);
          break;
        }
      }
    }

    // we don't care about cost for now
    double cost = 0;

    List<Integer> multiJoinFactors = connectedFactors.toList();
    List<JoinRelType> types = new ArrayList<>();
    for (Integer factor : multiJoinFactors) {
      types.add(typesMap.get(factor));
    }

    MultiJoinVertex multiJoinVertex =
        new MultiJoinVertex(v, multiJoinFactors, newFactors, cost,
            ImmutableList.copyOf(conditions), types);

    vertexes.add(multiJoinVertex);

    for (final Edge edge : unusedEdges) {
      ImmutableBitSet intersect = edge.factors.intersect(connectedFactors);
      if (!intersect.isEmpty()) {
        ImmutableBitSet oldEdgeFactors = edge.factors;
        ImmutableBitSet newEdgeFactors =
            edge.factors.rebuild().removeAll(newFactors).set(v).build();
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

  static class MultiJoinVertex extends Vertex {
    // multiJoinFactors only contain the direct children of this vertex
    // factors in Vertex contains all the factor covered by this vertex (all recursive children and self)
    List<Integer> multiJoinFactors;
    /** Zero or more join conditions. All are in terms of the original input
     * columns (not in terms of the outputs of left and right input factors). */
    final ImmutableList<RexNode> conditions;
    List<JoinRelType> types;

    MultiJoinVertex(int id, List<Integer> multiJoinFactors,
        ImmutableBitSet factors, double cost,
        ImmutableList<RexNode> conditions, List<JoinRelType> types) {
      super(id, factors, cost);
      this.multiJoinFactors = multiJoinFactors;
      this.conditions = conditions;
      this.types = types;
    }
  }

}
