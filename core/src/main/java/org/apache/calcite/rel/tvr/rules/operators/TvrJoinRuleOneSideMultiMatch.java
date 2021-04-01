package org.apache.calcite.rel.tvr.rules.operators;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleCall.TransformBuilder;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.utils.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.VersionInterval;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static org.apache.calcite.rel.tvr.utils.TvrJoinUtils.*;
import static org.apache.calcite.rel.tvr.utils.TvrJoinUtils.JoinType.*;
import static org.apache.calcite.rel.tvr.utils.TvrUtils.*;
import static java.util.Arrays.asList;

/**
 * The Tvr Join rule that assumes:
 * 1. Join conditions are all equal predicates
 * 2. Each tuple in one side can join with multiple tuples from the other side
 */
public class TvrJoinRuleOneSideMultiMatch extends RelOptTvrRule {

  // number of extra columns compared to snapshot schema (multiplicity)
  private boolean positiveOnlyLeft;
  private int extraColumnsLeft;
  private boolean positiveOnlyRight;
  private int extraColumnsRight;

  private TvrMetaSet leftTvr;
  private TvrMetaSet rightTvr;

  public static final TvrJoinRuleOneSideMultiMatch INSTANCE =
      new TvrJoinRuleOneSideMultiMatch(MultiJoin.class,
          // Do not match subclasses like OdpsMultiMapJoin
          join -> join.getClass().equals(MultiJoin.class));

  /**
   * Matches:
   *
   *    Join     --SET_SNAPSHOT_MAX--    JoinTVR
   *   /   \
   * Left   \   --SET_SNAPSHOT_MAX--  LeftTVR ---SetDelta(MIN, t')-- LeftPre
   *         \                                \--SetDelta(t' - t)-- LeftDelta
   *       Right    --SET_SNAPSHOT_MAX--  RightTVR ---SetDelta(MIN, t')-- RightPre
   *                                               \--SetDelta(t' - t)-- RightDelta
   *
   * Transforms To:
   *  JoinTVR --SetDelta(t' - t) ---\
   *                              Union
   *                     --------/     \-------
   *                    /                      \
   *                  Join                   Join
   *            -----/    \                  /    \-------
   *            /          \                /             \
   *          LeftDelta   RightPre        RightDelta     Union
   *                                                    /     \
   *                                              LeftDelta  LeftPre
   *
   */
  private TvrJoinRuleOneSideMultiMatch(Class<? extends RelNode> clz,
      Predicate<? super RelNode> relPredicate) {
    super(operand(clz, relPredicate, tvrEdgeSSMax(tvr()),
        operand(RelSubset.class, tvrEdgeSSMax(tvr(adjacentTimeEdges(
            tvrEdge(TvrSetDelta.class, isSnapshotTime, IDENTICAL_TIME,
                logicalSubset()),
            tvrEdge(TvrSetDelta.class, isSnapshotTime.negate(),
                IDENTICAL_TIME_2, logicalSubset())))), any()),
        operand(RelSubset.class, tvrEdgeSSMax(tvr(adjacentTimeEdges(
            tvrEdge(TvrSetDelta.class, isSnapshotTime, IDENTICAL_TIME,
                logicalSubset()),
            tvrEdge(TvrSetDelta.class, isSnapshotTime.negate(),
                IDENTICAL_TIME_2, logicalSubset())))), any())));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    RelNode join = root.get();

    TvrContext ctx = TvrContext.getInstance(join.getCluster());
    TvrMetaSet rootTvr = root.tvr().get();
    if (TvrUtils.progressiveOuterJoinViewStrict(ctx) && ctx.getVersionDim() > 1
        && rootTvr.getTvrType().equals(ctx.getDefaultTvrType())) {
      // Disallow delta join rules on normal/default tvr
      return false;
    }
    if (TvrUtils.progressiveDbToasterStrict(ctx) && ctx.getVersionDim() > 1
        && rootTvr.getTvrType().equals(ctx.getDefaultTvrType())) {
      // Disallow delta join rules on normal/default tvr
      // unless the dimension of the version is only 1
      return false;
    }

    // make sure multi join rule is disabled: MultiJoin only have 2 inputs
    assert join.getInputs().size() == 2;

    // make sure all leaf's snapshot and delta times are the same
    List<RelMatch> inputs = root.inputs();
    return inputs.stream()
        .map(i -> i.tvrSibling(isSnapshotTime).tvrSemantics.toVersion)
        .distinct().count() == 1 && inputs.stream()
        .map(i -> i.tvrSibling(isSnapshotTime.negate()).tvrSemantics.toVersion)
        .distinct().count() == 1;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    RelNode join = root.get();
    RexBuilder rexBuilder = join.getCluster().getRexBuilder();

    RelOfTvr leftPrevTvr = root.input(0).tvrSibling(equivSnapshot);
    RelOfTvr rightPrevTvr = root.input(1).tvrSibling(equivSnapshot);
    RelOfTvr leftDeltaTvr = root.input(0).tvrSibling(isNormalSetDelta);
    RelOfTvr rightDeltaTvr = root.input(1).tvrSibling(isNormalSetDelta);
    TvrMetaSet rootTvr = root.tvr().get();
    leftTvr = root.input(0).tvr().get();
    rightTvr = root.input(1).tvr().get();

    JoinType joinType = getLogicalJoinType(join);
    RexNode condition = getLogicalJoinCondition(join);

    RelNode leftPrev = leftPrevTvr.rel.get();
    RelNode rightPrev = rightPrevTvr.rel.get();
    RelNode leftDelta = leftDeltaTvr.rel.get();
    RelNode rightDelta = rightDeltaTvr.rel.get();
    TvrSetDelta leftPrevTrait = (TvrSetDelta) leftPrevTvr.tvrSemantics;
    TvrSetDelta rightPrevTrait = (TvrSetDelta) rightPrevTvr.tvrSemantics;
    TvrSetDelta leftDeltaTrait = (TvrSetDelta) leftDeltaTvr.tvrSemantics;
    TvrSetDelta rightDeltaTrait = (TvrSetDelta) rightDeltaTvr.tvrSemantics;

    TransformBuilder transformBuilder = call.transformBuilder();

    // 1. apply left update
    if (requireRightPrevPositiveOnly(joinType, rightPrevTrait)) {
      rightPrev =
          TvrSetDelta.consolidate(rightPrev, rexBuilder, rightPrevTrait, true);
      rightPrevTrait =
          new TvrSetDelta(rightPrevTrait.fromVersion, rightPrevTrait.toVersion,
              true);
      // Register the consolidated result
      transformBuilder.addTvrLink(rightPrev, rightPrevTrait, rightTvr);
    }
    boolean leftTypeSatisfied =
        (!joinType.equals(FULL)) && (!joinType.equals(RIGHT));
    boolean leftPositiveOnly =
        positiveOnly(leftDeltaTrait, rightPrevTrait, leftTypeSatisfied);

    TvrSetDelta leftJoinTrait =
        new TvrSetDelta(leftDeltaTrait.fromVersion, leftDeltaTrait.toVersion,
            leftPositiveOnly);
    RelNode leftResult = null;
    if (!TvrUtils.isEmpty(leftDelta)) {
      leftResult =
          join(transformBuilder, joinType, condition, leftPrev, leftPrevTrait,
              rightPrev, rightPrevTrait, leftDelta, leftDeltaTrait, null, null);
    }

    // 2. apply right update
    Pair<RelNode, TvrSetDelta> newLeftPrev =
        mergeUnion(leftPrev, leftPrevTrait, leftDelta, leftDeltaTrait,
            VersionInterval.of(leftPrevTrait.fromVersion, leftDeltaTrait.toVersion));
    if (requireLeftPrevPositiveOnly(joinType, newLeftPrev.right)) {
      newLeftPrev = Pair.of(TvrSetDelta
              .consolidate(newLeftPrev.left, rexBuilder, newLeftPrev.right,
                  true),
          new TvrSetDelta(newLeftPrev.right.fromVersion,
              newLeftPrev.right.toVersion, true));
      // Register the consolidated result
      transformBuilder.addTvrLink(newLeftPrev.left, newLeftPrev.right, leftTvr);
    }

    boolean rightTypeSatisfied =
        (joinType.equals(INNER) || joinType.equals(RIGHT));
    boolean rightPositiveOnly =
        positiveOnly(newLeftPrev.right, rightDeltaTrait, rightTypeSatisfied);

    TvrSetDelta rightJoinTrait =
        new TvrSetDelta(leftDeltaTrait.fromVersion, leftDeltaTrait.toVersion,
            rightPositiveOnly);
    RelNode rightResult = null;
    if (!TvrUtils.isEmpty(rightDelta)) {
      rightResult =
          join(transformBuilder, joinType, condition, newLeftPrev.left,
              newLeftPrev.right, rightPrev, rightPrevTrait, null, null,
              rightDelta, rightDeltaTrait);
    }

    // 3. union left and right result
    Pair<RelNode, TvrSetDelta> res;
    if (leftResult == null) {
      if (rightResult == null) {
        res = Pair.of(call.builder().values(join.getRowType()).build(),
            new TvrSetDelta(leftDeltaTrait.fromVersion,
                leftDeltaTrait.toVersion, true));
      } else {
        res = Pair.of(rightResult, rightJoinTrait);
      }
    } else if (rightResult == null) {
      res = Pair.of(leftResult, leftJoinTrait);
    } else {
      res = mergeUnion(leftResult, leftJoinTrait, rightResult, rightJoinTrait,
          VersionInterval.of(leftDeltaTrait.fromVersion, leftDeltaTrait.toVersion));
    }
    RelNode relNode = call.builder().push(res.left).convert(
        deriveJoinRowType(joinType, leftPrevTvr, leftDeltaTvr, rightPrevTvr,
            rightDeltaTvr, res.right.isPositiveOnly()), true).build();
    relNode = matchNullable(relNode, join.getRowType(), call.builder());

    transformBuilder.addTvrLink(relNode, res.right, rootTvr).transform();
  }

  private boolean positiveOnly(TvrSetDelta leftTrait, TvrSetDelta rightTrait,
      boolean typeMatch) {
    positiveOnlyLeft = leftTrait.isPositiveOnly();
    positiveOnlyRight = rightTrait.isPositiveOnly();

    extraColumnsLeft = positiveOnlyLeft ? 0 : 1;
    extraColumnsRight = positiveOnlyRight ? 0 : 1;

    return typeMatch && positiveOnlyLeft && positiveOnlyRight;
  }

  private boolean requireLeftPrevPositiveOnly(JoinType joinType,
                                              TvrSetDelta leftPrevTrait) {
    if (leftPrevTrait.isPositiveOnly()) {
      return false;
    }
    // TODO: relax this if possible
    return !joinType.equals(INNER);
  }

  private boolean requireRightPrevPositiveOnly(JoinType joinType,
                                               TvrSetDelta rightPrevTrait) {
    if (rightPrevTrait.isPositiveOnly()) {
      return false;
    }
    // TODO: relax this if possible
    return !joinType.equals(INNER);
  }

  private Pair<RelNode, TvrSetDelta> mergeUnion(RelNode left,
      TvrSetDelta leftTrait, RelNode right, TvrSetDelta rightTrait,
      VersionInterval inv) {

    boolean positiveOnly =
        leftTrait.isPositiveOnly() && rightTrait.isPositiveOnly();
    if (!positiveOnly) {
      left = convertToSetDeltaIfNot(left, leftTrait);
      right = convertToSetDeltaIfNot(right, rightTrait);
    }
    RelNode rel = LogicalUnion.create(asList(left, right), true);
    TvrSetDelta tvrSemantics = new TvrSetDelta(inv.from, inv.to, positiveOnly);

    return Pair.of(rel, tvrSemantics);
  }

  private RelDataType deriveJoinRowType(JoinType joinType, RelOfTvr leftPrevTvr,
      RelOfTvr leftDeltaTvr, RelOfTvr rightPrevTvr, RelOfTvr rightDeltaTvr,
      boolean positiveOnly) {
    RelNode leftPrev = leftPrevTvr.rel.get();
    RelDataTypeFactory typeFactory = leftPrev.getCluster().getTypeFactory();
    RelDataType leftRowType = getUnionRowType(leftPrevTvr, leftDeltaTvr);
    RelDataType rightRowType = getUnionRowType(rightPrevTvr, rightDeltaTvr);
    RelDataType rowType;
    JoinRelType joinRelType = getJoinType(joinType);
    if (joinRelType == JoinRelType.ANTI || joinRelType == JoinRelType.SEMI) {
      rowType = leftRowType;
    } else {
      rowType = TvrRelOptUtils
          .deriveJoinRowType(leftRowType, rightRowType, joinRelType, typeFactory,
              ImmutableList.of());
    }
    if (!positiveOnly) {
      rowType = typeFactory.builder().addAll(rowType.getFieldList())
          .add(getMultiplicityName(), getMultiplicityType(typeFactory, false))
          .build();
    }
    return rowType;
  }

  private RelDataType getUnionRowType(RelOfTvr snapshotTvr, RelOfTvr deltaTvr) {
    RelNode snapshot = snapshotTvr.rel.get();
    RelNode delta = deltaTvr.rel.get();
    TvrSemantics snapshotTrait = snapshotTvr.tvrSemantics;
    TvrSemantics deltaTrait = deltaTvr.tvrSemantics;
    RelDataType snapshotRowType = snapshot.getRowType();
    RelDataType deltaRowType = delta.getRowType();
    RelDataTypeFactory typeFactory = snapshot.getCluster().getTypeFactory();
    if (isPositiveNegativeSetDelta.test(snapshotTrait)) {
      int fieldCount = snapshotRowType.getFieldCount();
      snapshotRowType = typeFactory.builder()
          .addAll(snapshotRowType.getFieldList().subList(0, fieldCount - 1))
          .build();
    }
    if (isPositiveNegativeSetDelta.test(deltaTrait)) {
      int fieldCount = deltaRowType.getFieldCount();
      deltaRowType = typeFactory.builder()
          .addAll(deltaRowType.getFieldList().subList(0, fieldCount - 1))
          .build();
    }
    return typeFactory.leastRestrictive(asList(snapshotRowType, deltaRowType));
  }

  private RelNode createJoin(RelNode left, RelNode right, RexNode joinCondition,
      JoinType joinType) {
    RexNode newCondition =
        createJoinConditionWithMul(left.getCluster().getRexBuilder(),
            joinCondition, left, right);
    return TvrJoinUtils.createJoin(left, right, newCondition, joinType, false);
  }

  private enum Schema {
    // keep all right columns
    KEEP, // discard all right columns, as in anti/semi join
    DISCARD_RIGHT, // pad all right columns with null, as in outer join
    PAD_NULL_RIGHT, // pad all left columns with null, as in outer join
    PAD_NULL_LEFT,
  }

  private enum Multiplicity {
    // no multiplicity column if positive only
    NORMAL, // force adding multiplicity column if positive only
    REQUIRE, // force adding multiplicity column, and inverse the value
    INVERSE
  }

  private RelNode innerJoin(RexNode condition, RelNode left, RelNode right,
      Schema schema, Multiplicity multiplicity) {
    return projectFullJoin(INNER, condition, left, right, schema, multiplicity);
  }

  private RelNode leftOuterJoin(RexNode condition, RelNode left, RelNode right,
      Multiplicity multiplicity) {
    return projectFullJoin(LEFT, condition, left, right, Schema.KEEP,
        multiplicity);
  }

  private RelNode rightOuterJoin(RexNode condition, RelNode left, RelNode right,
      Multiplicity multiplicity) {
    return projectFullJoin(RIGHT, condition, left, right, Schema.KEEP,
        multiplicity);
  }

  private RelNode semiJoin(RexNode condition, RelNode left, RelNode right) {
    return createJoin(left, right, condition, LEFT_SEMI);
  }

  private RelNode leftAntiJoin(RexNode condition, RelNode left, RelNode right) {
    return createJoin(left, right, condition, LEFT_ANTI);
  }

  private RelNode projectFullJoin(JoinType joinType, RexNode condition,
      RelNode left, RelNode right, Schema schema, Multiplicity multiplicity) {
    RexBuilder rexBuilder = left.getCluster().getRexBuilder();

    boolean isSemiJoin = (joinType == LEFT_ANTI || joinType == LEFT_SEMI);
    assert !isSemiJoin;

    int leftFieldCount = left.getRowType().getFieldCount();
    int rightFieldCount = right.getRowType().getFieldCount();

    RelNode join = createJoin(left, right, condition, joinType);
    RelDataType joinRowType = join.getRowType();

    // left and right indices, excluding left and right multiplicity
    IntStream leftIndices =
        IntStream.range(0, leftFieldCount - extraColumnsLeft);
    IntStream rightIndices = IntStream.range(leftFieldCount,
        leftFieldCount + rightFieldCount - extraColumnsRight);
    // only add non-multiplicity columns
    ArrayList<RexNode> projects = new ArrayList<>();
    ArrayList<String> names = new ArrayList<>();

    leftIndices.forEach(i -> {
      RexNode ref;
      RelDataType fieldType = joinRowType.getFieldList().get(i).getType();
      if (schema == Schema.PAD_NULL_LEFT) {
        ref = rexBuilder.makeNullLiteral(fieldType);
      } else {
        ref = rexBuilder.makeInputRef(fieldType, i);
      }
      projects.add(ref);
      names.add(joinRowType.getFieldNames().get(i));
    });
    // derive rowType and names, when right columns are lost in anti/semi join
    rightIndices.forEach(i -> {
      RexNode ref;
      RelDataType fieldType = joinRowType.getFieldList().get(i).getType();
      if (schema == Schema.DISCARD_RIGHT) {
        return;
      } else if (schema == Schema.PAD_NULL_RIGHT) {
        ref = rexBuilder.makeNullLiteral(fieldType);
      } else {
        assert schema == Schema.KEEP || schema == Schema.PAD_NULL_LEFT;
        ref = rexBuilder.makeInputRef(fieldType, i);
      }
      projects.add(ref);
      names.add(joinRowType.getFieldNames().get(i));
    });

    // construct new multiplicity: left * right
    Integer leftMultiplicityIndex;
    Integer rightMultiplicityIndex;

    leftMultiplicityIndex =
        positiveOnlyLeft ? null : left.getRowType().getFieldCount() - 1;
    rightMultiplicityIndex =
        positiveOnlyRight ? null : join.getRowType().getFieldCount() - 1;

    RexNode multExpr = null;
    if (leftMultiplicityIndex != null && rightMultiplicityIndex != null) {
      multExpr = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, ImmutableList
          .of(rexBuilder.makeInputRef(join, leftMultiplicityIndex),
              rexBuilder.makeInputRef(join, rightMultiplicityIndex)));
    } else if (leftMultiplicityIndex != null) {
      multExpr = rexBuilder.makeInputRef(join, leftMultiplicityIndex);
    } else if (rightMultiplicityIndex != null) {
      multExpr = rexBuilder.makeInputRef(join, rightMultiplicityIndex);
    }

    if (multiplicity == Multiplicity.INVERSE) {
      if (multExpr == null) {
        multExpr = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(-1L));
      } else {
        multExpr = rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, multExpr);
      }
    } else if (multiplicity == Multiplicity.REQUIRE) {
      if (multExpr == null) {
        multExpr = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(1L));
      }
    }

    if (multExpr != null) {
      projects.add(multExpr);
      names.add(TvrUtils.getMultiplicityName());
    }

    return TvrRelOptUtils
        .strip(LogicalProject.create(join, projects, names));
  }

  private RelNode join(TransformBuilder transformBuilder, JoinType joinType,
      RexNode joinCondition, RelNode leftPrev, TvrSetDelta leftPrevTrait,
      RelNode rightPrev, TvrSetDelta rightPrevTrait, RelNode leftDelta,
      TvrSetDelta leftDeltaTrait, RelNode rightDelta,
      TvrSetDelta rightDeltaTrait) {
    boolean isLeft = (leftDelta != null);
    assert (isLeft && rightDelta == null) || (!isLeft && rightDelta != null);

    RelNode result;
    switch (joinType) {
    case INNER:
      if (isLeft) {
        result = innerJoin(joinCondition, leftDelta, rightPrev, Schema.KEEP,
            Multiplicity.NORMAL);
      } else {
        result = innerJoin(joinCondition, leftPrev, rightDelta, Schema.KEEP,
            Multiplicity.NORMAL);
      }
      break;
    case LEFT_SEMI:
      if (isLeft) {
        // rightPrev must be positive only (consolidated)
        result = semiJoin(joinCondition, leftDelta, rightPrev);
      } else {
        result = buildUpdateRightDeltaForLeftAntiSemi(transformBuilder,
            joinCondition, leftPrev, leftPrevTrait, rightPrev, rightPrevTrait,
            rightDelta, rightDeltaTrait, false, true);
      }
      break;
    case LEFT_ANTI:
      if (isLeft) {
        // rightPrev must be positive only (consolidated)
        result = leftAntiJoin(joinCondition, leftDelta, rightPrev);
      } else {
        result = buildUpdateRightDeltaForLeftAntiSemi(transformBuilder,
            joinCondition, leftPrev, leftPrevTrait, rightPrev, rightPrevTrait,
            rightDelta, rightDeltaTrait, true, true);
      }
      break;
    case LEFT:
      if (isLeft) {
        // rightPrev must be positive only (consolidated)
        result = leftOuterJoin(joinCondition, leftDelta, rightPrev,
            Multiplicity.NORMAL);
      } else {
        List<RelNode> rels = asList(
            // inner join
            innerJoin(joinCondition, leftPrev, rightDelta, Schema.KEEP,
                Multiplicity.REQUIRE),
            // left anti
            buildUpdateRightDeltaForLeftAntiSemi(transformBuilder,
                joinCondition, leftPrev, leftPrevTrait, rightPrev,
                rightPrevTrait, rightDelta, rightDeltaTrait, true, false));
        result = LogicalUnion.create(rels, true);
      }
      break;
    case RIGHT:
      if (isLeft) {
        List<RelNode> rels = asList(
            // inner join
            innerJoin(joinCondition, leftDelta, rightPrev, Schema.KEEP,
                Multiplicity.REQUIRE),
            // right anti
            buildUpdateLeftDeltaForRightAntiSemi(transformBuilder,
                joinCondition, leftPrev, leftPrevTrait, rightPrev,
                rightPrevTrait, leftDelta, leftDeltaTrait, true, false));
        result = LogicalUnion.create(rels, true);
      } else {
        // leftPrev must be positive only (consolidated)
        result = rightOuterJoin(joinCondition, leftPrev, rightDelta,
            Multiplicity.NORMAL);
      }
      break;
    case FULL:
      if (isLeft) {
        List<RelNode> rels = asList(
            // inner join
            innerJoin(joinCondition, leftDelta, rightPrev, Schema.KEEP,
                Multiplicity.REQUIRE),
            // right anti
            buildUpdateLeftDeltaForRightAntiSemi(transformBuilder,
                joinCondition, leftPrev, leftPrevTrait, rightPrev,
                rightPrevTrait, leftDelta, leftDeltaTrait, true, false));
        result = LogicalUnion.create(rels, true);
      } else {
        List<RelNode> rels = asList(
            // inner join
            innerJoin(joinCondition, leftPrev, rightDelta, Schema.KEEP,
                Multiplicity.REQUIRE),
            // left anti
            buildUpdateRightDeltaForLeftAntiSemi(transformBuilder,
                joinCondition, leftPrev, leftPrevTrait, rightPrev,
                rightPrevTrait, rightDelta, rightDeltaTrait, true, false));
        result = LogicalUnion.create(rels, true);
      }
      break;
    default:
      throw new UnsupportedOperationException("unimplemented");
    }

    return result;
  }

  private RexNode createJoinConditionWithMul(RexBuilder rexBuilder,
      RexNode condition, RelNode left, RelNode right) {
    int leftFieldCount = left.getRowType().getFieldCount();
    int originalRightFirst = leftFieldCount - extraColumnsLeft;

    // shift extra columns
    condition = RexUtil.shift(condition, originalRightFirst, extraColumnsLeft);
    RexShuttle shuttle;

    // no shift / swap required
    // workaround for nullable bug on join type inference
    // create inputRef to join's input
    shuttle = new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        int index = inputRef.getIndex();
        if (index < leftFieldCount) {
          return rexBuilder.makeInputRef(
              left.getRowType().getFieldList().get(index).getType(), index);
        } else {
          return rexBuilder.makeInputRef(
              right.getRowType().getFieldList().get(index - leftFieldCount)
                  .getType(), index);
        }
      }
    };
    condition = condition.accept(shuttle);
    return condition;
  }

  // Return: refIndex of left(right)keys in join condition -> refIndex in
  // aggregated left(right) with only these key columns
  private LinkedHashMap<Integer, Integer> findJoinKeyMap(RexNode condition,
      int originalLeftCount, boolean isLeft) {
    LinkedHashMap<Integer, Integer> ret = new LinkedHashMap<>();
    condition.accept(new RexVisitorImpl<Object>(true) {
      @Override
      public Object visitInputRef(RexInputRef inputRef) {
        int index = inputRef.getIndex();
        if (isLeft) {
          if (index < originalLeftCount) {
            ret.put(index, ret.size());
          }
        } else {
          if (index >= originalLeftCount) {
            ret.put(index - originalLeftCount, ret.size());
          }
        }
        return super.visitInputRef(inputRef);
      }
    });
    return ret;
  }

  private RexNode buildAndCondition(ImmutableList.Builder<RexNode> conditions,
      RexBuilder rexBuilder) {
    List<RexNode> parts = conditions.build();
    assert parts.size() > 0;
    if (parts.size() == 1) {
      return parts.get(0);
    } else {
      return rexBuilder.makeCall(SqlStdOperatorTable.AND, parts);
    }
  }

  private RelNode buildDeltaLeftAntiSelfJoinSnapshot(RexBuilder rexBuilder,
      RelNode delta, RelNode snapshot, Set<Integer> joinKeys) {

    List<RelDataTypeField> deltaFields = delta.getRowType().getFieldList();
    ImmutableList.Builder<RexNode> antiJoinCondition = ImmutableList.builder();
    joinKeys.forEach(index -> {
      RexNode keyLeft = rexBuilder.makeInputRef(delta, index);
      RexNode keyRight = rexBuilder
          .makeInputRef(deltaFields.get(index).getType(),
              deltaFields.size() + index);
      antiJoinCondition.add(
          rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, keyLeft, keyRight));
    });
    return TvrJoinUtils.createJoin(delta, snapshot,
        buildAndCondition(antiJoinCondition, rexBuilder), LEFT_ANTI, false);
  }

  // Result always has multiplicity column
  // rightPrev can be +-
  private RelNode buildUpdateRightDeltaForLeftAntiSemi(
      TransformBuilder transformBuilder, RexNode condition, RelNode leftPrev,
      TvrSetDelta leftPrevTrait, RelNode rightPrev, TvrSetDelta rightPrevTrait,
      RelNode rightDelta, TvrSetDelta rightDeltaTrait, boolean isAntiJoin,
      boolean discardRight) {

    RexBuilder rexBuilder = leftPrev.getCluster().getRexBuilder();

    int leftCount = leftPrev.getRowType().getFieldCount();
    int originalLeftCount =
        leftPrevTrait.isPositiveOnly() ? leftCount : leftCount - 1;

    Map<Integer, Integer> joinRightKeyMap =
        findJoinKeyMap(condition, originalLeftCount, false);

    Pair<RelNode, TvrSetDelta> rightNext =
        mergeUnion(rightPrev, rightPrevTrait, rightDelta, rightDeltaTrait,
            VersionInterval.of(rightPrevTrait.fromVersion,
                rightDeltaTrait.toVersion));
    RelNode rightNextPo = TvrSetDelta
        .consolidate(rightNext.left, rexBuilder, rightNext.right, true);

    // Register the consolidated result
    transformBuilder.addTvrLink(rightNextPo,
        new TvrSetDelta(rightNext.right.fromVersion, rightNext.right.toVersion,
            true), rightTvr);

    RelNode rightPrevPo =
        TvrSetDelta.consolidate(rightPrev, rexBuilder, rightPrevTrait, true);
    transformBuilder.addTvrLink(rightPrevPo,
        new TvrSetDelta(rightPrevTrait.fromVersion, rightPrevTrait.toVersion,
            true), rightTvr);

    RelNode rightDeltaAdd, rightDeltaDel;
    if (rightDeltaTrait.isPositiveOnly()) {
      rightDeltaAdd = rightDelta;
      // Set a non-null value for code simplicity, will be discarded later
      rightDeltaDel = rightDelta;
    } else {
      RexNode mulRef = rexBuilder.makeInputRef(rightDelta,
          rightDeltaTrait.getIndexOfMultiplicity(rightDelta));
      RexNode zero = rexBuilder.makeBigintLiteral(BigDecimal.ZERO);

      rightDeltaAdd = LogicalFilter.create(rightDelta,
          rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, mulRef, zero));
      rightDeltaDel = LogicalFilter.create(rightDelta,
          rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, mulRef, zero));
    }

    RelNode trueAdd =
        buildDeltaLeftAntiSelfJoinSnapshot(rexBuilder, rightDeltaAdd,
            rightPrevPo, joinRightKeyMap.keySet());
    RelNode trueDel =
        buildDeltaLeftAntiSelfJoinSnapshot(rexBuilder, rightDeltaDel,
            rightNextPo, joinRightKeyMap.keySet());

    // Now we have computed the "true add" and "true delete" of the right side,
    // compute the delta of the original anti/semi join
    RexNode newCondition = condition.accept(new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        int index = inputRef.getIndex();
        if (index < originalLeftCount) {
          return inputRef;
        } else {
          return rexBuilder.makeInputRef(inputRef.getType(),
              index - originalLeftCount + leftCount);
        }
      }
    });
    RelNode add_raw = TvrJoinUtils
        .createJoin(leftPrev, trueAdd, newCondition, LEFT_SEMI, false);
    RelNode delete_raw = TvrJoinUtils
        .createJoin(leftPrev, trueDel, newCondition, LEFT_SEMI, false);

    ProjectBuilder addBuilder =
        ProjectBuilder.anchor(add_raw).addAll(0, originalLeftCount);
    ProjectBuilder deleteBuilder =
        ProjectBuilder.anchor(delete_raw).addAll(0, originalLeftCount);
    // May need to pad right fields as null before adding multiplicity
    if (!discardRight) {
      for (RelDataTypeField rightField : rightPrevPo.getRowType()
          .getFieldList()) {
        addBuilder.add(rexBuilder.makeNullLiteral(rightField.getType()),
            rightField.getName());
        deleteBuilder.add(rexBuilder.makeNullLiteral(rightField.getType()),
            rightField.getName());
      }
    }

    // Add multiplicity
    RexNode addMul, deleteMul;
    if (leftPrevTrait.isPositiveOnly()) {
      if (isAntiJoin) {
        deleteMul = rexBuilder.makeBigintLiteral(BigDecimal.ONE);
        addMul = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(-1L));
      } else {
        deleteMul = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(-1L));
        addMul = rexBuilder.makeBigintLiteral(BigDecimal.ONE);
      }
    } else {
      RexNode leftMul = rexBuilder.makeInputRef(leftPrev,
          leftPrevTrait.getIndexOfMultiplicity(leftPrev));
      if (isAntiJoin) {
        deleteMul = leftMul;
        addMul = rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, leftMul);
      } else {
        addMul = leftMul;
        deleteMul = rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, leftMul);
      }
    }
    addBuilder.add(addMul, TvrUtils.getMultiplicityName());
    deleteBuilder.add(deleteMul, TvrUtils.getMultiplicityName());

    if (rightDeltaTrait.isPositiveOnly()) {
      // There's no right delete, ignore things in deleteBuilder
      return addBuilder.build();
    }
    return LogicalUnion
        .create(ImmutableList.of(addBuilder.build(), deleteBuilder.build()),
            true);
  }

  // Result always has multiplicity column
  // leftPrev can be +-
  private RelNode buildUpdateLeftDeltaForRightAntiSemi(
      TransformBuilder transformBuilder, RexNode condition, RelNode leftPrev,
      TvrSetDelta leftPrevTrait, RelNode rightPrev, TvrSetDelta rightPrevTrait,
      RelNode leftDelta, TvrSetDelta leftDeltaTrait, boolean isAntiJoin,
      boolean discardLeft) {

    RexBuilder rexBuilder = leftPrev.getCluster().getRexBuilder();

    int leftCount = leftPrev.getRowType().getFieldCount();
    int originalLeftCount =
        leftPrevTrait.isPositiveOnly() ? leftCount : leftCount - 1;

    Map<Integer, Integer> joinLeftKeyMap =
        findJoinKeyMap(condition, originalLeftCount, true);

    Pair<RelNode, TvrSetDelta> leftNext =
        mergeUnion(leftPrev, leftPrevTrait, leftDelta, leftDeltaTrait,
            VersionInterval.of(leftPrevTrait.fromVersion, leftDeltaTrait.toVersion));
    RelNode leftNextPo = TvrSetDelta
        .consolidate(leftNext.left, rexBuilder, leftNext.right, true);

    // Register the consolidated result
    transformBuilder.addTvrLink(leftNextPo,
        new TvrSetDelta(leftNext.right.fromVersion, leftNext.right.toVersion,
            true), leftTvr);

    RelNode leftPrevPo =
        TvrSetDelta.consolidate(leftPrev, rexBuilder, leftPrevTrait, true);
    transformBuilder.addTvrLink(leftPrevPo,
        new TvrSetDelta(leftPrevTrait.fromVersion, leftPrevTrait.toVersion,
            true), leftTvr);

    RelNode leftDeltaAdd, leftDeltaDel;
    if (leftDeltaTrait.isPositiveOnly()) {
      leftDeltaAdd = leftDelta;
      // Set a non-null value for code simplicity, will be discarded later
      leftDeltaDel = leftDelta;
    } else {
      RexNode mulRef = rexBuilder.makeInputRef(leftDelta,
          leftDeltaTrait.getIndexOfMultiplicity(leftDelta));
      RexNode zero = rexBuilder.makeBigintLiteral(BigDecimal.ZERO);

      leftDeltaAdd = LogicalFilter.create(leftDelta,
          rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, mulRef, zero));
      leftDeltaDel = LogicalFilter.create(leftDelta,
          rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, mulRef, zero));
    }

    RelNode trueAdd =
        buildDeltaLeftAntiSelfJoinSnapshot(rexBuilder, leftDeltaAdd, leftPrevPo,
            joinLeftKeyMap.keySet());
    RelNode trueDel =
        buildDeltaLeftAntiSelfJoinSnapshot(rexBuilder, leftDeltaDel, leftNextPo,
            joinLeftKeyMap.keySet());

    // Now we have computed the "true add" and "true delete" of the left side,
    // compute the delta of the original anti/semi join
    // We don't have RIGHT_SEMI, so use LEFT_SEMI instead, swap both inputs
    int rightCount = rightPrev.getRowType().getFieldCount();
    int originalRightCount =
        rightPrevTrait.isPositiveOnly() ? rightCount : rightCount - 1;

    RexNode newCondition = condition.accept(new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        int index = inputRef.getIndex();
        if (index < originalLeftCount) {
          return rexBuilder
              .makeInputRef(inputRef.getType(), rightCount + index);
        } else {
          return rexBuilder
              .makeInputRef(inputRef.getType(), index - originalLeftCount);
        }
      }
    });
    RelNode add_raw = TvrJoinUtils
        .createJoin(rightPrev, trueAdd, newCondition, LEFT_SEMI, false);
    RelNode delete_raw = TvrJoinUtils
        .createJoin(rightPrev, trueDel, newCondition, LEFT_SEMI, false);

    ProjectBuilder addBuilder = ProjectBuilder.anchor(add_raw);
    ProjectBuilder deleteBuilder = ProjectBuilder.anchor(delete_raw);
    // May need to pad left fields as null first
    if (!discardLeft) {
      for (RelDataTypeField field : leftPrevPo.getRowType().getFieldList()) {
        addBuilder
            .add(rexBuilder.makeNullLiteral(field.getType()), field.getName());
        deleteBuilder
            .add(rexBuilder.makeNullLiteral(field.getType()), field.getName());
      }
    }

    addBuilder.addAll(0, originalRightCount);
    deleteBuilder.addAll(0, originalRightCount);

    // Add multiplicity
    RexNode addMul, deleteMul;
    if (rightPrevTrait.isPositiveOnly()) {
      if (isAntiJoin) {
        deleteMul = rexBuilder.makeBigintLiteral(BigDecimal.ONE);
        addMul = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(-1L));
      } else {
        deleteMul = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(-1L));
        addMul = rexBuilder.makeBigintLiteral(BigDecimal.ONE);
      }
    } else {
      RexNode rightMul = rexBuilder.makeInputRef(rightPrev,
          rightPrevTrait.getIndexOfMultiplicity(rightPrev));
      if (isAntiJoin) {
        deleteMul = rightMul;
        addMul = rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, rightMul);
      } else {
        addMul = rightMul;
        deleteMul = rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, rightMul);
      }
    }
    addBuilder.add(addMul, TvrUtils.getMultiplicityName());
    deleteBuilder.add(deleteMul, TvrUtils.getMultiplicityName());

    if (leftDeltaTrait.isPositiveOnly()) {
      // There's no left delete, ignore things in deleteBuilder
      return addBuilder.build();
    }
    return LogicalUnion
        .create(ImmutableList.of(addBuilder.build(), deleteBuilder.build()),
            true);
  }
}
