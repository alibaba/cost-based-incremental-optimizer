package org.apache.calcite.rel.tvr.rules.outerjoinview;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.tvr.trait.property.TvrOuterJoinViewProperty;
import org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil;
import org.apache.calcite.rel.tvr.trait.property.TvrUpdateOneTableProperty;
import org.apache.calcite.rel.type.RelDataType;

import java.util.*;
import java.util.stream.Collectors;
import static org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil.updateOneTableProperties;

import static org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil.updateOneTableProperties;

public class TvrOjvUnionPropRule extends RelOptTvrRule
    implements InterTvrRule {

  public static TvrOjvUnionPropRule INSTANCE = new TvrOjvUnionPropRule();

  private static RelOptRuleOperand getOp() {
    // A tvr with a tvr property self loop
    TvrPropertyEdgeRuleOperand propertyEdge =
        tvrProperty(TvrOuterJoinViewProperty.class, tvr());
    TvrRelOptRuleOperand tvr = tvr(propertyEdge);
    propertyEdge.setToTvrOp(tvr);

    return operand(LogicalUnion.class, tvrEdgeSSMax(tvr()),
        unordered(operand(RelSubset.class, tvrEdgeSSMax(tvr), any())));
  }

  private TvrOjvUnionPropRule() {
    super(getOp());
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    LogicalUnion union = getRoot(call).get();
    VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();
    TvrMetaSet tvr = root.tvr().get();
    // Fire when all inputs have TvrOuterJoinViewProperty
    return union.getInputs().stream().allMatch(input -> {
      RelSet set = planner.getSet(input);
      TvrMetaSet childTvr = set.getTvrForTvrSet(tvr.getTvrType());
      return childTvr.getTvrPropertyLinks(TvrOuterJoinViewProperty.class).size()
          > 0;
    });
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    LogicalUnion union = getRoot(call).get();
    VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();
    TvrMetaSet tvr = root.tvr().get();
    RelOptCluster cluster = union.getCluster();

    Map<RelNode, TvrOuterJoinViewProperty> childProperties = new IdentityHashMap<>();
    Map<RelNode, Map<TvrUpdateOneTableProperty, TvrMetaSet>> childVDProperties =
        new IdentityHashMap<>();
    union.getInputs().forEach(input -> {
      RelSet set = planner.getSet(input);
      TvrMetaSet childTvr = set.getTvrForTvrSet(tvr.getTvrType());

      Map<TvrOuterJoinViewProperty, TvrMetaSet> map =
          childTvr.getTvrPropertyLinks(TvrOuterJoinViewProperty.class);
      childProperties.put(input, map.keySet().iterator().next());

      childVDProperties.put(input, updateOneTableProperties(childTvr,
          TvrUpdateOneTableProperty.PropertyType.OJV));
    });

    // Compute all new VD property links
    RelOptRuleCall.TransformBuilder builder = call.transformBuilder();
    Map<TvrUpdateOneTableProperty, TvrMetaSet> oneChildVds =
        childVDProperties.values().iterator().next();
    oneChildVds.forEach((vdProperty, oneChildToTvr) -> {
      List<RelNode> newInputs = childVDProperties.values().stream().map(
          property2tvr -> property2tvr.get(vdProperty)
              .getSubset(TvrSemantics.SET_SNAPSHOT_MAX, cluster.traitSet()))
          .collect(Collectors.toList());

      // different inputs might have different nullable properties
      // we need derive row type first
      RelNode newUnion = union.copy(union.getTraitSet(), newInputs);
      RelDataType rowType = newUnion.getRowType();
      newUnion = call.builder().push(newUnion).convert(rowType, true).build();

      builder.addPropertyLink(tvr, vdProperty, newUnion,
          oneChildToTvr.getTvrType());
    });

    // Use set of terms first to dedup
    Set<LinkedHashSet<Integer>> newTerms = new HashSet<>();
    Set<Integer> allChangingTables = new HashSet<>();
    List<LinkedHashSet<Integer>> nonNullTermTables = new ArrayList<>();
    for (int i = 0; i < union.getRowType().getFieldCount(); i++) {
      nonNullTermTables.add(new LinkedHashSet<>());
    }

    childProperties.forEach((input, childProperty) -> {
      newTerms.addAll(childProperty.getTerms());
      allChangingTables.addAll(childProperty.allChangingTermTables());
      for (Ord<LinkedHashSet<Integer>> ord : Ord
          .zip(childProperty.getNonNullTermTables())) {
        nonNullTermTables.get(ord.i).addAll(ord.e);
      }
    });

    TvrOuterJoinViewProperty newOjvProperty =
        new TvrOuterJoinViewProperty(ImmutableList.copyOf(newTerms),
            nonNullTermTables, allChangingTables);
    assert TvrPropertyUtil
        .checkExisitingOJVProperty(call, tvr, union, newOjvProperty) :
        "TvrOuterJoinViewProperty doesn't match at " + union.getId();

    // Add the TvrOuterJoinViewProperty self loop on root tvr
    builder.addPropertyLink(tvr, newOjvProperty, union, tvr.getTvrType());

    builder.transform();
  }
}
