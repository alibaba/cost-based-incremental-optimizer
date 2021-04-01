package org.apache.calcite.rel.tvr.rules.outerjoinview;

import org.apache.calcite.plan.*;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrPropertyEdgeRuleOperand;
import org.apache.calcite.plan.volcano.TvrRelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.tvr.trait.property.TvrOuterJoinViewProperty;
import org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil;
import org.apache.calcite.rel.tvr.trait.property.TvrUpdateOneTableProperty;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil.updateOneTableProperties;
import static org.apache.calcite.rel.tvr.utils.TvrUtils.matchNullable;


public class TvrOjvProjectPropRule extends RelOptTvrRule
    implements InterTvrRule {

  private final Logger LOGGER = CalciteTrace.getPlannerTracer();

  public static TvrOjvProjectPropRule INSTANCE = new TvrOjvProjectPropRule();

  private static RelOptRuleOperand getOp() {
    // A tvr with a tvr property self loop
    TvrPropertyEdgeRuleOperand propertyEdge =
        tvrProperty(TvrOuterJoinViewProperty.class, tvr());
    TvrRelOptRuleOperand tvr = tvr(propertyEdge);
    propertyEdge.setToTvrOp(tvr);

    return operand(LogicalProject.class, tvrEdgeSSMax(tvr()),
        operand(RelSubset.class, tvrEdgeSSMax(tvr), any()));
  }

  private TvrOjvProjectPropRule() {
    super(getOp());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalProject project = getRoot(call).get();
    TvrMetaSet tvr = getRoot(call).tvr().get();
    TvrMatch childTvr = getRoot(call).input(0).tvr();

    TvrOuterJoinViewProperty childOjvProperty =
        childTvr.property(TvrOuterJoinViewProperty.class);
    Map<TvrUpdateOneTableProperty, TvrMetaSet> childVDProperties =
        updateOneTableProperties(childTvr.get(), TvrUpdateOneTableProperty.PropertyType.OJV);

    RelOptCluster cluster = project.getCluster();
    // LinkedHashMap are used to preserve register order, especially that
    // TvrOuterJoinViewProperty is registered last
    RelOptRuleCall.TransformBuilder builder = call.transformBuilder();

    // Compute all new VD property links
    childVDProperties.forEach((vdProperty, toTvr) -> {
      RelSubset child =
          toTvr.getSubset(TvrSemantics.SET_SNAPSHOT_MAX, cluster.traitSet());
      assert child != null;
      RelNode newRel = copyWithNewInputType(project, child, cluster, call);
      builder.addPropertyLink(tvr, vdProperty, newRel, toTvr.getTvrType());
    });

    List<LinkedHashSet<Integer>> nonNullTermTables = new ArrayList<>();
    Set<Integer> allReferredTables = new HashSet<>();
    for (RexNode field : project.getProjects()) {
      Set<Integer> referredTables = new HashSet<>();
      field.accept(new RexVisitorImpl<Object>(true) {
        @Override
        public Object visitInputRef(RexInputRef inputRef) {
          Set<Integer> set =
              childOjvProperty.getNonNullTermTables().get(inputRef.getIndex());
          referredTables.addAll(set);
          return null;
        }
      });
      allReferredTables.addAll(referredTables);
      nonNullTermTables
          .add(TvrOuterJoinViewProperty.getOrderedTableSet(referredTables));
    }

    Set<Integer> allChangingTables =
        new HashSet<>(childOjvProperty.allChangingTermTables());
    allChangingTables.removeAll(allReferredTables);
    if (!allChangingTables.isEmpty()) {
      LOGGER.warn("Not supported by ojv algorithm because changing tables "
          + allChangingTables + " got dropped by project " + project);
      return;
    }
    List<LinkedHashSet<Integer>> newTerms = childOjvProperty.getTerms();

    TvrOuterJoinViewProperty newOjvProperty =
        new TvrOuterJoinViewProperty(newTerms, nonNullTermTables,
            childOjvProperty.allChangingTermTables());
    assert TvrPropertyUtil
        .checkExisitingOJVProperty(call, tvr, project, newOjvProperty) :
        "TvrOuterJoinViewProperty doesn't match at " + project.getId();

    // Add the TvrOuterJoinViewProperty self loop on root tvr
    builder.addPropertyLink(tvr, newOjvProperty, project, tvr.getTvrType());

    builder.transform();
  }

  private RelNode copyWithNewInputType(LogicalProject project,
      RelNode child, RelOptCluster cluster, RelOptRuleCall call) {
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    RelDataType childRowType = child.getRowType();
    RelOptUtil.RexInputConverter converter =
        new RelOptUtil.RexInputConverter(rexBuilder, null,
            childRowType.getFieldList(), new int[childRowType.getFieldCount()]);

    List<Pair<RexNode, String>> namedProjects = project.getNamedProjects();
    List<RexNode> projects =
        Pair.left(namedProjects).stream().map(expr -> expr.accept(converter))
            .collect(Collectors.toList());
    RelDataType rowType = RexUtil
        .createStructType(typeFactory, projects, Pair.right(namedProjects),
            SqlValidatorUtil.F_SUGGESTER);
    RelNode newRel = project.copy(project.getTraitSet(), child, projects, rowType);
    return matchNullable(newRel, project.getRowType(), call.builder());
  }
}
