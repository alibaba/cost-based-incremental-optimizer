package org.apache.calcite.rel.tvr.rules.operators;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.trait.TvrValueDelta;
import org.apache.calcite.rel.tvr.trait.transformer.TvrSortTransformer;
import org.apache.calcite.rel.tvr.utils.TvrRelOptUtils;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Pair;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.calcite.plan.RelOptTvrRule.tvr;
import static org.apache.calcite.plan.RelOptTvrRule.tvrEdgeSSMax;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.*;

public abstract class TvrWindowRules {
  public static TvrWindowRule WINDOW_RULE = new TvrWindowRule();

  public static TvrFilterDecompositionRule FILTER_WINDOW_DECOMPOSITION_RULE =
      new TvrFilterDecompositionRule();

  public static Pair<List<RexNode>, List<RexNode>> splitWindowConditions(
      List<RexNode> conditions, int indexOfRowNumber) {
    List<RexNode> pushedConditions = new ArrayList<>();
    List<RexNode> remainingConditions = new ArrayList<>();

    for (RexNode condition : conditions) {
      FilterConditionChecker checker =
          new FilterConditionChecker(indexOfRowNumber);
      boolean valid = condition.accept(checker);
      if (!valid) {
        return null;
      }

      if (checker.hasConditionOnWindowCol) {
        pushedConditions.add(condition);
      } else {
        remainingConditions.add(condition);
      }
    }

    return Pair.of(pushedConditions, remainingConditions);
  }
}

class TvrWindowRule extends RelOptTvrRule {

  TvrWindowRule() {
    super(operand(LogicalFilter.class, tvrEdgeSSMax(tvr()),
        operand(LogicalWindow.class, tvrEdgeSSMax(tvr()),
            operand(RelSubset.class, tvrEdgeSSMax(
                tvr(tvrEdge(TvrSetDelta.class, TvrSetDelta::isPositiveOnly,
                    logicalSubset()))), any()))));
  }

  /**
   * Matches:
   * <p>
   *
   *      LogicalFilter  --SET_SNAPSHOT_MAX-- Tvr0
   *           |
   *      LogicalWindow  --SET_SNAPSHOT_MAX-- Tvr1
   *           |                                         (SetDelta+)
   *         input       --SET_SNAPSHOT_MAX-- Tvr2 ------------------------ input
   *
   * <p>
   * Converts to:
   * <p>
   *
   *                                                      (ValueDelta)
   *      LogicalFilter  --SET_SNAPSHOT_MAX-- Tvr0 ---------------------------- LogicalFilter
   *           |                                                                      |
   *      LogicalWindow  --SET_SNAPSHOT_MAX-- Tvr1                              LogicalWindow
   *           |                                          (SetDelta+)                 |
   *         input       --SET_SNAPSHOT_MAX-- Tvr2 ----------------------------     input
   *
   * <p>
   */
  @Override
  public boolean matches(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    LogicalFilter logicalFilter = root.get();
    LogicalWindow window = root.input(0).get();
    return checkFilter(logicalFilter, window.getRowType().getFieldCount() - 1)
        && checkWindow(window);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    LogicalFilter filter = root.get();
    LogicalWindow window = root.input(0).get();

    int fieldCount = filter.getRowType().getFieldCount();
    final List<RexNode> conditions = RelOptUtil.conjunctions(filter.getCondition());
    Pair<List<RexNode>, List<RexNode>> pair = Objects.requireNonNull(
        TvrWindowRules.splitWindowConditions(conditions, fieldCount - 1));

    RexCall conditionOnWindow = (RexCall) pair.left.get(0);
    SqlOperator operator = conditionOnWindow.getOperator();
    BigDecimal literal;
    if (conditionOnWindow.operands.get(0) instanceof RexLiteral) {
      literal = (BigDecimal) ((RexLiteral) conditionOnWindow.operands.get(0)).getValue();
    } else {
      literal = (BigDecimal) ((RexLiteral) conditionOnWindow.operands.get(1)).getValue();
    }

    BigDecimal limit;
    if (operator == EQUALS || operator == LESS_THAN_OR_EQUAL || operator == GREATER_THAN_OR_EQUAL) {
      limit = literal.setScale(0, BigDecimal.ROUND_FLOOR);
    } else {
      limit = literal.setScale(0, BigDecimal.ROUND_FLOOR).subtract(BigDecimal.ONE);
    }

    Window.Group group = window.groups.get(0);
    TvrSortTransformer sortTransformer =
        new TvrSortTransformer(group.keys, group.orderKeys, limit,
            window.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE),
            fieldCount - 1, fieldCount);

    RelOfTvr inputSibling = root.input(0).input(0).tvrSibling();
    TvrSemantics inputTrait = inputSibling.tvrSemantics;
    TvrValueDelta outputTrait =
        new TvrValueDelta(inputTrait.fromVersion, inputTrait.toVersion,
            sortTransformer, TvrUtils.isSnapshotTime.test(inputTrait));

    LogicalWindow newWindow =
        window.copy(window.getTraitSet(), singletonList(inputSibling.rel.get()));
    RelNode newFilter = LogicalFilter
        .create(newWindow, filter.getCondition());

    transformToRootTvr(call, newFilter, outputTrait);
  }

  static boolean checkWindow(Window window) {
    if (window.groups.size() != 1) {
      return false;
    }
    ImmutableList<Window.RexWinAggCall> aggCalls =
        window.groups.get(0).aggCalls;
    if (aggCalls.size() != 1) {
      return false;
    }
    return "ROW_NUMBER".equals(aggCalls.get(0).getOperator().getName());
  }

  private static boolean checkFilter(Filter filter, int indexOfRowNumber) {
    final List<RexNode> conditions = RelOptUtil.conjunctions(filter.getCondition());
    Pair<List<RexNode>, List<RexNode>> pair =
        TvrWindowRules.splitWindowConditions(conditions, indexOfRowNumber);
    return pair != null && pair.left.size() == 1 && pair.right.isEmpty();
  }
}

/**
 * Decompose the conditions of the filter whose input operator is window
 * to trigger the {@link TvrWindowRule}.
 * <p>
 * Matches:
 * <p>
 *          Filter (ROW_NUMBER < / <= / = RexLiteral, ...)
 *            |
 *          Window (compute ROW_NUMBER OVER (PARTITION BY _ ORDER BY _ ))
 *
 * <p>
 * Converts to:
 * <p>
 *          Filter (..., which is not applied on the row number column)
 *            |
 *          Filter (ROW_NUMBER < / <= / = RexLiteral)
 *            |
 *          Window (compute ROW_NUMBER OVER (PARTITION BY _ ORDER BY _ ))
 *
 * NOTE: can not set the importance of these filters to 0 in
 * {@link org.apache.calcite.rel.rules.FilterMergeRule}
 */
class TvrFilterDecompositionRule extends RelOptRule {

  TvrFilterDecompositionRule() {
    super(operand(LogicalFilter.class, operand(LogicalWindow.class, any())),
        "TvrFilterDecompositionRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalFilter filter = call.rel(0);
    LogicalWindow window = call.rel(1);

    if (!TvrWindowRule.checkWindow(window)) {
      return false;
    }

    final List<RexNode> conditions = RelOptUtil.conjunctions(filter.getCondition());
    Pair<List<RexNode>, List<RexNode>> pair = TvrWindowRules
        .splitWindowConditions(conditions,
            window.getRowType().getFieldCount() - 1);
    return pair != null && pair.left.size() == 1 && pair.right.size() > 0;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalFilter filter = call.rel(0);
    LogicalWindow window = call.rel(1);
    final List<RexNode> conditions = RelOptUtil.conjunctions(filter.getCondition());
    Pair<List<RexNode>, List<RexNode>> pair = Objects.requireNonNull(
        TvrWindowRules.splitWindowConditions(conditions, window.getRowType().getFieldCount() - 1));

    RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    RelTraitSet traitSet = filter.getTraitSet();
    RelNode filterDown = LogicalFilter.create(window, TvrRelOptUtils
        .makePredicate(rexBuilder, pair.left));
    filterDown = filterDown.copy(traitSet, filterDown.getInputs());

    RelNode filterUp = LogicalFilter.create(filterDown, TvrRelOptUtils
        .makePredicate(rexBuilder, pair.right));
    filterUp = filterUp.copy(traitSet, filterUp.getInputs());
    call.transformTo(filterUp);
  }

}

/**
 * Check whether the filter condition is valid for the result of window operator.
 * The valid pattern is as follows:
 *    ROW_NUMBER '<' / '<=' / '=' CONSTANT  (specially, CONSTANT is 1 when the operator is '=')
 *    AND / OR
 *    the other conditions that do not involve the result of window
 */
class FilterConditionChecker implements RexVisitor<Boolean> {
  final int indexOfWindowCol;
  boolean hasConditionOnWindowCol = false;

  FilterConditionChecker(int indexOfWindowCol) {
    this.indexOfWindowCol = indexOfWindowCol;
  }

  @Override
  public Boolean visitInputRef(RexInputRef inputRef) {
    return true;
  }

  @Override
  public Boolean visitLocalRef(RexLocalRef localRef) {
    return false;
  }

  @Override
  public Boolean visitLiteral(RexLiteral literal) {
    return true;
  }

  @Override
  public Boolean visitCall(RexCall call) {
    List<RexNode> windowColInputRef = call.operands.stream().filter(
        operand -> operand instanceof RexInputRef
            && ((RexInputRef) operand).getIndex() == indexOfWindowCol)
        .collect(Collectors.toList());

    if (windowColInputRef.isEmpty()) {
      // this RexCall does not involve the result of window
      return call.operands.stream().allMatch(operand -> operand.accept(this));
    } else if (windowColInputRef.size() > 1) {
      // this RexCall involves several results of windows
      return false;
    } else {
      // the valid pattern is:
      // RexInputRef(window column)  < / <= / =  RexLiteral(a constant value)
      if (!(call.op instanceof SqlBinaryOperator)) {
        return false;
      }

      RexLiteral literal;
      int indexOfLiteral;
      if (call.operands.get(0) instanceof RexLiteral) {
        literal = (RexLiteral) call.operands.get(0);
        indexOfLiteral = 0;
      } else if (call.operands.get(1) instanceof RexLiteral) {
        literal = (RexLiteral) call.operands.get(1);
        indexOfLiteral = 1;
      } else {
        return false;
      }

      boolean valid = false;
      if (call.op == EQUALS) {
        valid = literal.getValue().equals(BigDecimal.valueOf(1));
      } else if (call.op == LESS_THAN_OR_EQUAL || call.op == LESS_THAN) {
        // ROW_NUMBER < / <= RexLiteral
        valid = indexOfLiteral == 1;
      } else if (call.op == GREATER_THAN_OR_EQUAL || call.op == GREATER_THAN) {
        // RexLiteral > / >= ROW_NUMBER
        valid = indexOfLiteral == 0;
      }

      this.hasConditionOnWindowCol = valid;
      return valid;
    }
  }

  @Override
  public Boolean visitOver(RexOver over) {
    return false;
  }

  @Override
  public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
    return false;
  }

  @Override
  public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
    return false;
  }

  @Override
  public Boolean visitRangeRef(RexRangeRef rangeRef) {
    return false;
  }

  @Override
  public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
    return false;
  }

  @Override
  public Boolean visitSubQuery(RexSubQuery subQuery) {
    return false;
  }

  @Override
  public Boolean visitTableInputRef(RexTableInputRef fieldRef) {
    return false;
  }

  @Override
  public Boolean visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    return false;
  }
}
