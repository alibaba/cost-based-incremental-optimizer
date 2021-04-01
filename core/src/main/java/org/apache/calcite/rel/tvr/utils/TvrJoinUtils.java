package org.apache.calcite.rel.tvr.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Join related utils.
 */
public class TvrJoinUtils {

  public enum JoinType {
    INNER, LEFT, RIGHT, FULL, LEFT_SEMI, LEFT_ANTI
  }

  public static JoinType getTvrJoinType(JoinRelType type) {
    switch (type) {
      case INNER:
        return JoinType.INNER;
      case LEFT:
        return JoinType.LEFT;
      case RIGHT:
        return JoinType.RIGHT;
      case FULL:
        return JoinType.FULL;
      default:
        throw new RuntimeException("Tvr Join: unknown join type");
    }
  }

  public static JoinType getTvrJoinType(SemiJoinType semiJoinType) {
    switch (semiJoinType) {
      case LEFT:
        return JoinType.LEFT_SEMI;
      case ANTI:
        return JoinType.LEFT_ANTI;
      default:
        throw new RuntimeException("Tvr Join: unknown join type");
    }
  }

  public static JoinRelType getJoinType(
          JoinType joinType) {
    JoinRelType joinRelType;
    switch (joinType) {
      case FULL:
        joinRelType = JoinRelType.FULL;
        break;
      case RIGHT:
        joinRelType = JoinRelType.RIGHT;
        break;
      case LEFT:
        joinRelType = JoinRelType.LEFT;
        break;
      case INNER:
        joinRelType = JoinRelType.INNER;
        break;
      case LEFT_ANTI:
        joinRelType = JoinRelType.ANTI;
        break;
      case LEFT_SEMI:
        joinRelType = JoinRelType.SEMI;
        break;
      default:
        throw new RuntimeException("Tvr Join: unknown join type");
    }
    return joinRelType;
  }

  public static RexNode getLogicalJoinCondition(RelNode join) {
    if (join instanceof MultiJoin) {
      return getTwoInputMultiJoinCondition((MultiJoin) join);
    }
    return ((SemiJoin) join).getCondition();
  }

  public static JoinType getLogicalJoinType(RelNode join) {
    if (join instanceof MultiJoin) {
      return getTvrJoinType(getTwoInputMultiJoinType((MultiJoin) join));
    }
    return getTvrJoinType(((SemiJoin) join).getJoinType());
  }

  public static RexNode getTwoInputMultiJoinCondition(MultiJoin multiJoin) {
    // The logic is only true for MultiJoin with two inputs
    assert multiJoin.getInputs().size() == 2;

    // Refer to combineOuterJoins() for the join
    // type logic of MultiJoin
    if (multiJoin.isFullOuterJoin()) {
      return multiJoin.getJoinFilter();
    }
    if (multiJoin.getJoinTypes().get(0).equals(JoinRelType.INNER)) {
      switch (multiJoin.getJoinTypes().get(1)) {
        case INNER:
          return multiJoin.getJoinFilter();
        case LEFT:
          return multiJoin.getOuterJoinConditions().get(1);
        default:
          throw new RuntimeException(
                  "MultiJoin: not expecting join type " + multiJoin.getJoinTypes()
                          .get(1));
      }
    }
    assert multiJoin.getJoinTypes().get(0).equals(JoinRelType.RIGHT);
    return multiJoin.getOuterJoinConditions().get(0);
  }

  public static JoinRelType getTwoInputMultiJoinType(MultiJoin multiJoin) {
    // The logic is only true for MultiJoin with two inputs
    assert multiJoin.getInputs().size() == 2;

    // Refer to combineOuterJoins() for the join
    // type logic of MultiJoin
    if (multiJoin.isFullOuterJoin()) {
      return JoinRelType.FULL;
    }
    if (multiJoin.getJoinTypes().get(0).equals(JoinRelType.INNER)) {
      switch (multiJoin.getJoinTypes().get(1)) {
        case INNER:
          return JoinRelType.INNER;
        case LEFT:
          return JoinRelType.LEFT;
        default:
          throw new RuntimeException(
                  "MultiJoin: not expecting join type " + multiJoin.getJoinTypes()
                          .get(1));
      }
    }
    assert multiJoin.getJoinTypes().get(0).equals(JoinRelType.RIGHT);
    assert multiJoin.getJoinTypes().get(1).equals(JoinRelType.INNER);
    return JoinRelType.RIGHT;
  }

  public static RelNode createJoin(RelNode left, RelNode right,
                                   RexNode condition, JoinType joinType, boolean notUseCalciteMultiJoin) {
    assert ! notUseCalciteMultiJoin;
    return createMultiJoin(left, right, condition, getJoinType(joinType));
  }

  public static RelNode createMultiJoin(RelNode left, RelNode right,
                                        RexNode condition, JoinRelType joinType) {
    RelOptCluster cluster = left.getCluster();
    RexBuilder builder = cluster.getRexBuilder();
    RelDataType outputRowType = TvrRelOptUtils
            .deriveJoinRowType(left.getRowType(), right.getRowType(), joinType,
                    cluster.getTypeFactory(), Collections.emptyList());

    List<RelNode> inputs = ImmutableList.of(left, right);

    List<JoinRelType> joinTypes;
    List<RexNode> outerJoinConditions;
    List<RexNode> joinFilters = Lists.newArrayList();
    switch (joinType) {
      case LEFT:
        joinTypes = ImmutableList.of(JoinRelType.INNER, JoinRelType.LEFT);
        outerJoinConditions = ImmutableNullableList.of(null, condition);
        break;
      case RIGHT:
        joinTypes = ImmutableList.of(JoinRelType.RIGHT, JoinRelType.INNER);
        outerJoinConditions = ImmutableNullableList.of(condition, null);
        break;
      default:  // both FULL and INNER
        joinTypes = ImmutableList.of(JoinRelType.INNER, JoinRelType.INNER);
        outerJoinConditions = ImmutableNullableList.of(null, null);
        joinFilters.add(condition);
    }

    List<Integer> joinFieldRefCounts =
            new ArrayList<>(Collections.nCopies(outputRowType.getFieldCount(), 0));
    RexVisitor visitor = new RexVisitorImpl(true) {
      @Override
      public Object visitInputRef(RexInputRef inputRef) {
        int index = inputRef.getIndex();
        joinFieldRefCounts.set(index, joinFieldRefCounts.get(index) + 1);
        return super.visitInputRef(inputRef);
      }
    };
    condition.accept(visitor);

    int leftFieldCount = left.getRowType().getFieldCount();
    ImmutableMap<Integer, ImmutableIntList> joinFieldRefCountsMap = ImmutableMap
            .of(0, ImmutableIntList
                            .copyOf(joinFieldRefCounts.subList(0, leftFieldCount)), 1,
                    ImmutableIntList.copyOf(joinFieldRefCounts
                            .subList(leftFieldCount, outputRowType.getFieldCount())));

    return new MultiJoin(cluster, inputs,
            RexUtil.composeConjunction(builder, joinFilters, false), outputRowType,
            joinType == JoinRelType.FULL, outerJoinConditions, joinTypes,
            ImmutableNullableList.of(null, null), joinFieldRefCountsMap, null);
  }

  /**
   * We don't have RIGHT_SEMI/ANTI, so use LEFT_SEMI/ANTI instead, swap both inputs
   */
  public static RelNode createRightSemiAntiJoin(RexBuilder rexBuilder, RelNode left,
                                                RelNode right, RexNode condition, boolean useAnti, boolean padNull,
                                                boolean generateLogical) {
    int leftCount = left.getRowType().getFieldCount();
    int rightCount = right.getRowType().getFieldCount();
    RexNode newCondition = condition.accept(new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        int index = inputRef.getIndex();
        if (index < leftCount) {
          return rexBuilder
                  .makeInputRef(inputRef.getType(), rightCount + index);
        } else {
          return rexBuilder.makeInputRef(inputRef.getType(), index - leftCount);
        }
      }
    });
    JoinType type = useAnti ? JoinType.LEFT_ANTI : JoinType.LEFT_SEMI;
    RelNode leftSemi =
            createJoin(right, left, newCondition, type, generateLogical);

    ProjectBuilder projectBuilder = ProjectBuilder.anchor(leftSemi);
    // Pad left fields as null if needed
    if (padNull) {
      left.getRowType().getFieldList().forEach(field -> projectBuilder
              .add(rexBuilder.makeNullLiteral(field.getType()), field.getName()));
    }
    return projectBuilder.addAll(0, rightCount).build();
  }
}
