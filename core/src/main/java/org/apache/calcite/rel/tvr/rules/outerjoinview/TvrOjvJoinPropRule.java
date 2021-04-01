package org.apache.calcite.rel.tvr.rules.outerjoinview;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrPropertyEdgeRuleOperand;
import org.apache.calcite.plan.volcano.TvrRelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.tvr.trait.property.TvrOuterJoinViewProperty;
import org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil;
import org.apache.calcite.rel.tvr.trait.property.TvrUpdateOneTableProperty;
import org.apache.calcite.rel.tvr.utils.TvrJoinUtils;
import org.apache.calcite.rel.tvr.utils.TvrRelOptUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;

import java.util.*;
import java.util.function.Supplier;

import static org.apache.calcite.rel.tvr.utils.TvrJoinUtils.*;
import static org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil.updateOneTableProperties;
import static org.apache.calcite.rel.tvr.utils.TvrJoinUtils.getLogicalJoinType;
import static org.apache.calcite.rel.tvr.utils.TvrUtils.matchNullable;


public class TvrOjvJoinPropRule extends RelOptTvrRule
    implements InterTvrRule {

  public static TvrOjvJoinPropRule INSTANCE = new TvrOjvJoinPropRule();

  private static RelOptRuleOperand getOp() {
    // A tvr with a tvr property self loop
    Supplier<TvrRelOptRuleOperand> t = () -> {
      TvrPropertyEdgeRuleOperand propertyEdge =
          tvrProperty(TvrOuterJoinViewProperty.class, tvr());
      TvrRelOptRuleOperand tvr = tvr(propertyEdge);
      propertyEdge.setToTvrOp(tvr);
      return tvr;
    };
    return operand(MultiJoin.class, tvrEdgeSSMax(tvr()),
        operand(RelSubset.class, tvrEdgeSSMax(t.get()), any()),
        operand(RelSubset.class, tvrEdgeSSMax(t.get()), any()));
  }


  private TvrOjvJoinPropRule() {
    super(getOp());
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    MultiJoin join = root.get();
    TvrOuterJoinViewProperty leftOjvProperty = root.input(0).tvr().property();
    TvrOuterJoinViewProperty rightOjvProperty = root.input(1).tvr().property();

    // Do not support semi join and anti join
    JoinType joinType = getLogicalJoinType(join);
    if (joinType == JoinType.LEFT_SEMI || joinType == JoinType.LEFT_ANTI) {
      return false;
    }

    // only supports 2 inputs MultiJoin
    if (join.getInputs().size() != 2) {
      return false;
    }

    // Inputs should not share any changing term table
    Set<Integer> tables =
        new HashSet<>(leftOjvProperty.allChangingTermTables());
    if (tables.removeAll(rightOjvProperty.allChangingTermTables())) {
      return false;
    }

    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    TvrMetaSet tvr = root.tvr().get();
    MultiJoin join = root.get();

    RelNode left = root.input(0).get();
    TvrMatch leftTvr = root.input(0).tvr();
    TvrMatch rightTvr = root.input(1).tvr();
    TvrOuterJoinViewProperty leftOjvProperty = leftTvr.property();
    TvrOuterJoinViewProperty rightOjvProperty = rightTvr.property();

    Map<TvrUpdateOneTableProperty, TvrMetaSet> leftVDProperties =
        updateOneTableProperties(leftTvr.get(), TvrUpdateOneTableProperty.PropertyType.OJV);
    Map<TvrUpdateOneTableProperty, TvrMetaSet> rightVDProperties =
        updateOneTableProperties(rightTvr.get(), TvrUpdateOneTableProperty.PropertyType.OJV);

    JoinType joinType = getLogicalJoinType(join);
    RexNode condition = getLogicalJoinCondition(join);
    RelOptCluster cluster = join.getCluster();

    // LinkedHashMap are used to preserve register order, especially that
    // TvrOuterJoinViewProperty is registered last
    RelOptRuleCall.TransformBuilder builder = call.transformBuilder();

    // Compute all new VD property links
    leftVDProperties.forEach((vdProperty, leftChildTvr) -> {
      TvrMetaSet rightChildTvr = rightVDProperties.get(vdProperty);
      assert rightChildTvr != null;
      assert leftChildTvr.getTvrType().equals(rightChildTvr.getTvrType());

      RelSubset leftChild = leftChildTvr
          .getSubset(TvrSemantics.SET_SNAPSHOT_MAX, cluster.traitSet());
      RelSubset rightChild = rightChildTvr
          .getSubset(TvrSemantics.SET_SNAPSHOT_MAX, cluster.traitSet());

      RelNode newRel;
      int changingTable = vdProperty.getChangingTable();
      if (leftOjvProperty.allChangingTermTables().contains(changingTable)) {
        newRel =
            getVDJoin(join, leftChild, rightChild, condition, joinType, true,
                call);
      } else if (rightOjvProperty.allChangingTermTables()
          .contains(changingTable)) {
        newRel =
            getVDJoin(join, leftChild, rightChild, condition, joinType, false,
                call);
      } else {
        // Changing table not in this join at all, simply copy join
        newRel = join.copy(join.getTraitSet(),
            ImmutableList.of(leftChild, rightChild));
      }
      builder
          .addPropertyLink(tvr, vdProperty, newRel, leftChildTvr.getTvrType());
    });

    int leftN = left.getRowType().getFieldCount();
    List<LinkedHashSet<Integer>> newTerms =
        getNewTerms(leftOjvProperty, rightOjvProperty, condition, joinType,
            leftN);

    // Combine inputs' nonNullTermTables into the new nonNullTermTables
    List<LinkedHashSet<Integer>> nonNullTermTables =
        new ArrayList<>(leftOjvProperty.getNonNullTermTables());
    nonNullTermTables.addAll(rightOjvProperty.getNonNullTermTables());

    // Preserve order of left and right changing tables
    Set<Integer> newChangingTables =
        new LinkedHashSet<>(leftOjvProperty.allChangingTermTables());
    newChangingTables.addAll(rightOjvProperty.allChangingTermTables());

    TvrOuterJoinViewProperty newOjvProperty =
        new TvrOuterJoinViewProperty(newTerms, nonNullTermTables,
            newChangingTables);
    assert TvrPropertyUtil
        .checkExisitingOJVProperty(call, tvr, join, newOjvProperty) :
        "TvrOuterJoinViewProperty doesn't match at " + join.getId();

    // Add the TvrOuterJoinViewProperty self loop on root tvr
    builder.addPropertyLink(tvr, newOjvProperty, join, tvr.getTvrType());

    builder.transform();
  }

  private RelNode getVDJoin(RelNode join, RelNode left, RelNode right,
      RexNode condition, JoinType joinType, boolean changeTableInLeft,
      RelOptRuleCall call) {
    RelOptCluster cluster = left.getCluster();
    int leftN = left.getRowType().getFieldCount();
    int rightN = right.getRowType().getFieldCount();

    switch (joinType) {
    case LEFT_SEMI:
    case LEFT_ANTI:
      assert false : "MultiJoin should not be of type " + joinType;
    case INNER:
      return join.copy(join.getTraitSet(), ImmutableList.of(left, right));
    case LEFT:
      if (changeTableInLeft) {
        return join.copy(join.getTraitSet(), ImmutableList.of(left, right));
      } else {
        // For changing tables on the right, change to inner join
        return TvrJoinUtils
            .createJoin(left, right, condition, JoinType.INNER, false);
      }
    case RIGHT:
      if (changeTableInLeft) {
        // For changing tables on the left, change to inner join
        return TvrJoinUtils
            .createJoin(left, right, condition, JoinType.INNER, false);
      } else {
        return join.copy(join.getTraitSet(), ImmutableList.of(left, right));
      }
    default:   // FULL
      if (changeTableInLeft) {
        // For changing tables on the left, change to left outer join
        return TvrJoinUtils
            .createJoin(left, right, condition, JoinType.LEFT, false);
      } else {
        /*
         * For changing tables on the right, should change to right outer join.
         * Since join type only support left outer, create a left outer join
         * with swapped inputs and add a project so that output rowType remains
         * the same.
         */
        JoinRelType joinRelType = getJoinType(joinType);
        RelDataType swappedJoinType = TvrRelOptUtils
            .deriveJoinRowType(right.getRowType(), left.getRowType(),
                joinRelType, cluster.getTypeFactory(), Collections.emptyList());

        // New condition swapping left and right input
        int[] adjustments = new int[leftN + rightN];
        for (int i = 0; i < leftN + rightN; i++) {
          adjustments[i] = i < leftN ? rightN : -leftN;
        }
        RexNode swappedCondition = condition.accept(
            new RelOptUtil.RexInputConverter(cluster.getRexBuilder(),
                join.getRowType().getFieldList(),
                swappedJoinType.getFieldList(), adjustments));

        // The project on top of the swappedJoin
        ArrayList<RexNode> projects = new ArrayList<>();
        ArrayList<String> names = new ArrayList<>();
        for (int outIndex = 0; outIndex < leftN + rightN; outIndex++) {
          int inIndex = outIndex < leftN ? outIndex + rightN : outIndex - leftN;
          RelDataTypeField field = swappedJoinType.getFieldList().get(inIndex);
          projects.add(
              cluster.getRexBuilder().makeInputRef(field.getType(), inIndex));
          names.add(field.getName());
        }

        RelNode swappedJoin = TvrJoinUtils
            .createJoin(right, left, swappedCondition, JoinType.LEFT, false);
        swappedJoin =
            matchNullable(swappedJoin, join.getRowType(), call.builder());
        return TvrRelOptUtils
            .strip(LogicalProject.create(swappedJoin, projects, names));
      }
    }
  }

  private List<LinkedHashSet<Integer>> getNewTerms(
      TvrOuterJoinViewProperty leftProperty,
      TvrOuterJoinViewProperty rightProperty, RexNode condition,
      JoinType joinType, int leftN) {
    List<LinkedHashSet<Integer>> newTerms = new ArrayList<>();

    Set<Integer> keyRefLeftTables = new HashSet<>();
    Set<Integer> keyRefRightTables = new HashSet<>();
    condition.accept(new RexVisitorImpl<Object>(true) {
      @Override
      public Object visitInputRef(RexInputRef inputRef) {
        int index = inputRef.getIndex();
        Set<Integer> tables;
        if (index < leftN) {
          tables = leftProperty.getNonNullTermTables().get(index);
          keyRefLeftTables.addAll(tables);
        } else {
          tables = rightProperty.getNonNullTermTables().get(index - leftN);
          keyRefRightTables.addAll(tables);
        }
        return super.visitInputRef(inputRef);
      }
    });

    // Add the terms from the inner join part first
    for (Set<Integer> leftTerm : leftProperty.getTerms()) {
      Set<Integer> leftCopy = new HashSet<>(leftTerm);
      if (!leftCopy.containsAll(keyRefLeftTables)) {
        // leftTerm doesn't have some table columns referenced in join key
        continue;
      }
      for (Set<Integer> rightTerm : rightProperty.getTerms()) {
        Set<Integer> rightCopy = new HashSet<>(rightTerm);
        if (!rightCopy.containsAll(keyRefRightTables)) {
          // rightTerm doesn't have some table columns referenced in join key
          continue;
        }
        rightCopy.addAll(leftTerm);
        newTerms.add(TvrOuterJoinViewProperty.getOrderedTableSet(rightCopy));
      }
    }

    // Add the terms from the outer join part if any
    switch (joinType) {
    case LEFT_SEMI:
    case LEFT_ANTI:
      assert false : "MultiJoin should not be of type " + joinType;
    case INNER:
      break;
    case LEFT:
      newTerms.addAll(leftProperty.getTerms());
      break;
    case RIGHT:
      newTerms.addAll(rightProperty.getTerms());
      break;
    default:  // FULL
      newTerms.addAll(leftProperty.getTerms());
      newTerms.addAll(rightProperty.getTerms());
      break;
    }
    return newTerms;
  }

}
