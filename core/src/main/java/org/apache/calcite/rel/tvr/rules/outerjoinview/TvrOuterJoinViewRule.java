package org.apache.calcite.rel.tvr.rules.outerjoinview;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AdhocSink;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableSink;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.trait.property.TvrOuterJoinViewDoneProperty;
import org.apache.calcite.rel.tvr.trait.property.TvrOuterJoinViewProperty;
import org.apache.calcite.rel.tvr.trait.property.TvrUpdateOneTableProperty;
import org.apache.calcite.rel.tvr.utils.ProjectBuilder;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrJoinUtils;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.VersionInterval;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil.updateOneTableProperties;
import static org.apache.calcite.rel.tvr.utils.TvrUtils.isPositiveOnlySetDelta;
import static org.apache.calcite.rel.tvr.utils.TvrUtils.isSnapshotTime;


/**
 * The rule that assembles the +-SetDelta using the outer join view algorithm.
 * (Efficient Maintenance of Materialized Outer-Join Views, ICDE'07)
 */
public class TvrOuterJoinViewRule extends RelOptTvrRule {

  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  public static TvrOuterJoinViewRule INSTANCE = new TvrOuterJoinViewRule();

  private RelOptCluster cluster;
  private RexBuilder rexBuilder;
  private TvrContext ctx;

  private TvrMetaSet tvr;
  private TvrOuterJoinViewProperty property;
  private List<Integer> changingTablesOrdered;
  private Map<TvrUpdateOneTableProperty, TvrMetaSet> vdProperties;

  // The subsumption graph described in the paper
  private Multimap<LinkedHashSet<Integer>, LinkedHashSet<Integer>> parentTerms;

  // The time interval we are trying to generate a delta for
  private VersionInterval deltaInteval;

  private TvrOuterJoinViewRule() {
    super(operand(RelNode.class, TvrOuterJoinViewRule::fireOnRel, tvrEdgeSSMax(
        tvr(tvrEdge(TvrSetDelta.class,  // Match previous po view result
            isSnapshotTime.and(isPositiveOnlySetDelta), logicalSubset()),
            tvrProperty(TvrOuterJoinViewProperty.class,
                tvr(false)),  // Need to match different tvrTypes
            tvrProperty(TvrUpdateOneTableProperty.class,
                tvr(tvrEdge(TvrSetDelta.class, isSnapshotTime.negate(),
                    logicalSubset()))))), any()));
  }

  private static boolean fireOnRel(RelNode rel) {
    // Fire if the ojv property cannot propagate further
    VolcanoPlanner planner = (VolcanoPlanner) rel.getCluster().getPlanner();
    RelSubset subset = planner.getSubset(rel);
    return subset.getParentRels().stream().anyMatch(
        node -> node instanceof TableSink || node instanceof AdhocSink
            || node instanceof Aggregate || node instanceof Sort);
  }

  private boolean init(RelMatch root) {
    this.property = root.tvr().property(TvrOuterJoinViewProperty.class);
    this.cluster = root.get().getCluster();
    this.rexBuilder = cluster.getRexBuilder();
    this.ctx = TvrContext.getInstance(cluster);

    this.tvr = root.tvr().get();
    this.changingTablesOrdered =
        this.ctx.allNonDimTablesSorted().stream().map(ctx::tableOrdinal)
            .collect(Collectors.toList());

    this.vdProperties = updateOneTableProperties(tvr, TvrUpdateOneTableProperty.PropertyType.OJV);

    buildTermSubsumptionGraph(this.property.getTerms());

    // Locate the delta interval we are computing this time, depending on the
    // matched previous SuperDelta
    TvrSemantics previousSuperDelta =
        root.tvrSibling(TvrSetDelta.class).tvrSemantics;
    VersionInterval[] deltas = tvr.getTvrType().getDeltas();
    deltaInteval = null;
    for (VersionInterval delta : deltas) {
      if (delta.from.equals(previousSuperDelta.toVersion)) {
        deltaInteval = delta;
        break;
      }
    }
    return deltaInteval != null;
  }

  /**
   * Matches:
   *                                      ---
   *               (SetSnapshotMax)     /    | (TvrOuterJoinViewProperty)
   * OdpsMultiJoin ---------------- Tvr  <---
   *                                 |
   *                                 | (TvrUpdateOneTableProperty)
   *              (+SetDelta)        |
   *    SubSet ------------------- vdTvr
   *
   */
  @Override
  public boolean matches(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    if (!init(root)) {
      return false;
    }

    // Need to make sure that all needed VD +SetDeltas are ready
    for (Integer changingTable : changingTablesOrdered) {
      if (findVdDelta(changingTable) == null) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    RelNode relNode = root.get();
    RelNode prevPoSuperDelta = root.tvrSibling(TvrSetDelta.class).rel.get();
    init(root);

    // Avoid multiple fire on the same node
    TvrOuterJoinViewDoneProperty doneProperty =
        new TvrOuterJoinViewDoneProperty(deltaInteval);
    if (tvr.getTvrPropertyLinks(TvrOuterJoinViewDoneProperty.class)
        .containsKey(doneProperty)) {
      return;
    }

    LOGGER.info("TvrOuterJoinViewRule firing on {} and {}", tvr, relNode);

    // See if the delta result is positiveOnly
    boolean isResultPoDelta = true;
    for (Integer changingTable : changingTablesOrdered) {
      // matches() ensures that this delta (+/+-) already exists
      Pair<RelNode, TvrSetDelta> vdDeltaPair = findVdDelta(changingTable);
      assert vdDeltaPair != null;
      if (!vdDeltaPair.right.isPositiveOnly()) {
        // we have a +- VD delta
        isResultPoDelta = false;
        break;
      }
      if (!findViTerms(property.getTerms(), changingTable).isEmpty()) {
        // we have VI term
        isResultPoDelta = false;
        break;
      }
    }

    TvrSetDelta deltaTrait =
        new TvrSetDelta(deltaInteval.from, deltaInteval.to, isResultPoDelta);
    if (changingTablesOrdered.isEmpty()) {
      RelNode empty = call.builder().values(relNode.getRowType()).build();
      transformToRootTvr(call, empty, deltaTrait);
    } else {
      RelNode delta = generateDelta(prevPoSuperDelta, isResultPoDelta);
      transformToRootTvr(call, delta, deltaTrait);
    }

    // Add a commit message so that I won't fire on this node again
    call.transformBuilder()
        .addPropertyLink(tvr, doneProperty, relNode, tvr.getTvrType())
        .transform();
  }

  private RelNode generateDelta(RelNode prevPoSuperDelta,
      boolean isResultPoDelta) {
    // Find the previous view result to start with
    RelNode viewResultSoFar;
    if (isResultPoDelta) {
      viewResultSoFar = prevPoSuperDelta;
    } else {
      viewResultSoFar = ProjectBuilder.anchor(prevPoSuperDelta).addAll()
          .addConstMultiplicity(BigDecimal.ONE).build();
    }

    // Update the views with a few rounds, one update table at a time
    Builder<RelNode> deltaComponents = ImmutableList.builder();
    for (Integer changingTable : changingTablesOrdered) {
      // matches() ensures that this delta (+/+-) already exists
      Pair<RelNode, TvrSetDelta> vdDeltaPair = findVdDelta(changingTable);
      assert vdDeltaPair != null;
      // The +- delta,
      RelNode vdDelta;
      if (vdDeltaPair.right.isPositiveOnly()) {
        vdDelta = ProjectBuilder.anchor(vdDeltaPair.left).addAll()
            .addConstMultiplicity(BigDecimal.ONE).build();
      } else {
        LOGGER.warn("Only +- VD delta found for {} at max default tvr {}",
            changingTable, tvr);
        vdDelta = vdDeltaPair.left;
      }

      if (isResultPoDelta) {
        assert vdDeltaPair.right.isPositiveOnly();
        deltaComponents.add(vdDeltaPair.left);
        // No need to compute vi and update viewResultSoFar below
        continue;
      } else {
        deltaComponents.add(vdDelta);
      }

      Set<LinkedHashSet<Integer>> viTerms =
          findViTerms(property.getTerms(), changingTable);
      if (viTerms.isEmpty()) {
        // Update the view result after this round
        viewResultSoFar = LogicalUnion
            .create(ImmutableList.of(viewResultSoFar, vdDelta), true);
        continue;
      }

      // Consolidate the +- VD delta if needed, separate the + part and - part
      RelNode vdDeltaP, vdDeltaN;
      if (vdDeltaPair.right.isPositiveOnly()) {
        vdDeltaP = vdDelta;
        vdDeltaN = null;
      } else {
        RelNode c = TvrSetDelta
            .consolidate(vdDeltaPair.left, rexBuilder, vdDeltaPair.right,
                false);
        int mulIndex = vdDeltaPair.right.getIndexOfMultiplicity(c);
        RexInputRef mulRef = rexBuilder.makeInputRef(c, mulIndex);
        RexLiteral zero = rexBuilder.makeBigintLiteral(BigDecimal.ZERO);
        RexNode positive = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
            ImmutableList.of(mulRef, zero));
        RexNode negative = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
            ImmutableList.of(mulRef, zero));
        vdDeltaP = LogicalFilter.create(c, positive);
        vdDeltaN = LogicalFilter.create(c, negative);
      }

      // No need to include vd of this round into viewResultSoFar yet
      RelNode viDeltaVdP =
          buildViTermDeltaVdP(viTerms, viewResultSoFar, vdDeltaP);
      deltaComponents.add(viDeltaVdP);

      // Update the view result
      viewResultSoFar = LogicalUnion
          .create(ImmutableList.of(viewResultSoFar, vdDelta, viDeltaVdP), true);

      if (vdDeltaN == null) {
        // VD is positive only
        continue;
      }

      // Update and consolidate viewResultSoFar first
      RelNode viewResultConsolidated = TvrSetDelta
          .consolidate(viewResultSoFar, rexBuilder,
              new TvrSetDelta(TvrVersion.MIN, deltaInteval.from, false), false);

      RelNode viDeltaVdN =
          buildViTermDeltaVdN(viTerms, viewResultConsolidated, vdDeltaN);
      // Update the view result for the last time
      viewResultSoFar = LogicalUnion
          .create(ImmutableList.of(viewResultConsolidated, viDeltaVdN), true);
    }
    return LogicalUnion.create(deltaComponents.build(), true);
  }

  /**
   * For the positive part of VD delta, assemble the big semi join combining all
   * the semi joins for each viTerms described in the paper.
   */
  private RelNode buildViTermDeltaVdP(Set<LinkedHashSet<Integer>> viTerms,
      RelNode viewResultSoFar, RelNode vdDeltaP) {

    Builder<RelNode> semiJoins = ImmutableList.builder();

    for (LinkedHashSet<Integer> viTerm : viTerms) {
      Builder<RexNode> filterOnView = ImmutableList.builder();
      Builder<RexNode> filterOnVD = ImmutableList.builder();
      Builder<RexNode> joinConditions = ImmutableList.builder();

      List<RelDataTypeField> leftFields =
          viewResultSoFar.getRowType().getFieldList();
      // Go through the normal fields of the view, except the multiplicity
      // column
      int columnIndex = 0;
      for (LinkedHashSet<Integer> nonNullTermTable : property
          .getNonNullTermTables()) {
        Set<Integer> set = new HashSet<>(nonNullTermTable);
        set.retainAll(viTerm);
        RexNode fieldRef = rexBuilder
            .makeInputRef(leftFields.get(columnIndex).getType(), columnIndex);
        if (set.isEmpty() && !nonNullTermTable.isEmpty()) {
          // Column not from VI term tables
          RexNode condition =
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, fieldRef);
          filterOnView.add(condition);
        } else {
          // Column from VI term tables
          RexNode condition =
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, fieldRef);
          // Both view and VD have the same rowType (except the multiplicity
          // column), they can share inputRef and thus conditions
          filterOnView.add(condition);
          filterOnVD.add(condition);

          joinConditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
              ImmutableList.of(fieldRef, rexBuilder
                  .makeInputRef(leftFields.get(columnIndex).getType(),
                      columnIndex + leftFields.size()))));
        }
        columnIndex++;
      }

      RelNode left = LogicalFilter
          .create(viewResultSoFar, buildAndCondition(filterOnView, rexBuilder));
      RelNode right = LogicalFilter
          .create(vdDeltaP, buildAndCondition(filterOnVD, rexBuilder));

      // Build the semi join
      semiJoins.add(TvrJoinUtils.createJoin(left, right,
          buildAndCondition(joinConditions, rexBuilder),
          TvrJoinUtils.JoinType.LEFT_SEMI, false));
    }

    RelNode joins = LogicalUnion.create(semiJoins.build(), true);

    // Add normal columns first (without multiplicity), then flip the
    // multiplicity sign
    int n = joins.getRowType().getFieldCount();
    RexNode negMultiplicity = rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS,
        rexBuilder.makeInputRef(joins, n - 1));
    return ProjectBuilder.anchor(joins).addAllBut(n - 1)
        .add(negMultiplicity, TvrUtils.getMultiplicityName()).build();
  }

  /**
   * For the negative part of VD delta, assemble the big semi join combining all
   * the semi joins for each viTerms described in the paper.
   */
  private RelNode buildViTermDeltaVdN(Set<LinkedHashSet<Integer>> viTerms,
      RelNode viewResultConsolidated, RelNode vdDeltaN) {

    Builder<RelNode> semiJoins = ImmutableList.builder();

    for (LinkedHashSet<Integer> viTerm : viTerms) {
      Builder<RexNode> filterOnVD = ImmutableList.builder();
      List<Integer> aggGroup = new ArrayList<>();
      List<RexNode> projectOnAggP = new ArrayList<>();
      List<String> projectOnAggN = new ArrayList<>();
      Builder<RexNode> joinConditions = ImmutableList.builder();

      List<RelDataTypeField> leftFields = vdDeltaN.getRowType().getFieldList();
      // Go through the normal fields of the view, except the multiplicity
      // column
      int columnIndex = 0;
      for (LinkedHashSet<Integer> nonNullTermTable : property
          .getNonNullTermTables()) {
        Set<Integer> set = new HashSet<>(nonNullTermTable);
        set.retainAll(viTerm);

        RelDataTypeField field = leftFields.get(columnIndex);
        RexNode fieldRef =
            rexBuilder.makeInputRef(field.getType(), columnIndex);

        if (set.isEmpty() && !nonNullTermTable.isEmpty()) {
          // Column not from VI term tables, set to null in projectOnFilter
          projectOnAggP.add(rexBuilder.makeNullLiteral(field.getType()));
          projectOnAggN.add(field.getName());
        } else {
          // Column from VI term tables
          filterOnVD.add(
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, fieldRef));

          projectOnAggP
              .add(rexBuilder.makeInputRef(field.getType(), aggGroup.size()));
          projectOnAggN.add(field.getName());
          aggGroup.add(columnIndex);

          // Both VD and view have the same rowType (except the multiplicity
          // column), they can share inputRef and thus conditions
          joinConditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
              ImmutableList.of(fieldRef, rexBuilder
                  .makeInputRef(leftFields.get(columnIndex).getType(),
                      columnIndex + leftFields.size() - 1))));
        }

        columnIndex++;
      }

      RelNode filteredVD = LogicalFilter
          .create(vdDeltaN, buildAndCondition(filterOnVD, rexBuilder));
      // dedup with aggregate, note that multiplicity column is dropped here
      LogicalAggregate logicalAggregate = LogicalAggregate
          .create(filteredVD, false, ImmutableBitSet.of(aggGroup), null,
              ImmutableList.of());
      // Add back all null fields
      RelNode projectOnAgg = LogicalProject
          .create(logicalAggregate, projectOnAggP, projectOnAggN);

      // Build the semi join
      semiJoins.add(TvrJoinUtils
          .createJoin(projectOnAgg, viewResultConsolidated,
              buildAndCondition(joinConditions, rexBuilder),
              TvrJoinUtils.JoinType.LEFT_ANTI, false));
    }

    RelNode joins = LogicalUnion.create(semiJoins.build(), true);

    // Add the multiplicity column back
    return ProjectBuilder.anchor(joins).addAll()
        .addConstMultiplicity(BigDecimal.ONE).build();
  }

  private RexNode buildAndCondition(Builder<RexNode> conditions,
      RexBuilder rexBuilder) {
    List<RexNode> parts = conditions.build();
    assert parts.size() > 0;
    if (parts.size() == 1) {
      return parts.get(0);
    } else {
      return rexBuilder.makeCall(SqlStdOperatorTable.AND, parts);
    }
  }

  private void buildTermSubsumptionGraph(
      List<LinkedHashSet<Integer>> allTerms) {
    assert allTerms.stream().distinct().count() == allTerms.size();

    // Build ancestor map
    Multimap<LinkedHashSet<Integer>, LinkedHashSet<Integer>> ancestorTerms =
        LinkedListMultimap.create();
    allTerms.forEach(term -> allTerms.stream()
        .filter(ancestor -> ancestor != term && ancestor.containsAll(term))
        .forEach(ancestor -> ancestorTerms.put(term, ancestor)));

    // Build immediate parent map
    this.parentTerms = LinkedListMultimap.create(ancestorTerms);
    allTerms.forEach(term -> {
      Collection<LinkedHashSet<Integer>> myParentTerms =
          this.parentTerms.get(term);
      Collection<LinkedHashSet<Integer>> myAncestors = ancestorTerms.get(term);
      myAncestors.forEach(
          ancestor -> myParentTerms.removeAll(ancestorTerms.get(ancestor)));
    });
  }

  private Set<LinkedHashSet<Integer>> findViTerms(
      List<LinkedHashSet<Integer>> allTerms, Integer changingTable) {
    Set<LinkedHashSet<Integer>> vdTerms =
        allTerms.stream().filter(term -> term.contains(changingTable))
            .collect(Collectors.toSet());

    return allTerms.stream().filter(term -> {
      if (vdTerms.contains(term)) {
        return false;
      }
      // VI term should have an immediate VD parent
      Set<LinkedHashSet<Integer>> parents =
          new HashSet<>(this.parentTerms.get(term));
      parents.retainAll(vdTerms);
      return !parents.isEmpty();
    }).collect(Collectors.toSet());
  }

  // Find the +/+- delta of the VD tvr. Prefer + over +- delta
  private Pair<RelNode, TvrSetDelta> findVdDelta(Integer changingTable) {
    TvrMetaSet vdTvr = vdProperties
        .get(new TvrUpdateOneTableProperty(changingTable, TvrUpdateOneTableProperty.PropertyType.OJV));
    assert vdTvr != null : "VdTvr for " + changingTable + " not found";

    // Assume in default tvrType, all tables always have the same version
    long tableFrom = deltaInteval.from.get(0);
    long tableTo = deltaInteval.to.get(0);

    // Find the delta interval in VdTvr, which updates the give table
    VersionInterval d =
        vdTvr.getTvrType().findFirstDelta(changingTable, tableFrom, tableTo);

    // Prefer +delta, if not exist, see if +-delta is there
    TvrSetDelta po = new TvrSetDelta(d.from, d.to, true);
    RelNode poSubset = vdTvr.getSubset(po, cluster.traitSet());
    if (poSubset != null) {
      return Pair.of(poSubset, po);
    }

    TvrSetDelta nonPo = new TvrSetDelta(d.from, d.to, false);
    RelNode nonPoSubset = vdTvr.getSubset(nonPo, cluster.traitSet());
    if (nonPoSubset != null) {
      return Pair.of(nonPoSubset, nonPo);
    }

    return null;
  }
}
