package org.apache.calcite.rel.tvr.rules.operators;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.trait.TvrValueDelta;
import org.apache.calcite.rel.tvr.trait.transformer.TvrAggregateTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrProjectTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrSemanticsTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrSortTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.predicate.TransformerPredicate;
import org.apache.calcite.rel.tvr.utils.RexInputRefReplacer;
import org.apache.calcite.rel.tvr.utils.TvrRelOptUtils;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;

import java.util.Map;

import static java.util.Collections.singletonList;

/**
 * Set delta and value snapshot can always directly pass through Filter.
 * Value delta can pass through Filter only if the expression doesn't
 * involve aggregated columns.
 * <p>
 * Matches:
 * <p>
 *
 *      LogicalFilter  --SET_SNAPSHOT_MAX-- Tvr0
 *           |                                            (*Delta)
 *         input       --SET_SNAPSHOT_MAX-- Tvr1 ------------------------     input
 *
 * <p>
 * Converts to:
 * <p>
 *
 *                                                (SetDelta/ValueDelta)
 *      LogicalFilter  --SET_SNAPSHOT_MAX-- Tvr0 ------------------------ newLogicalFilter
 *           |                                    (SetDelta/ValueDelta)         |
 *         input       --SET_SNAPSHOT_MAX-- Tvr1 ------------------------     input
 *
 * <p>
 */
public abstract class TvrFilterRules {
  public static final TvrSetDeltaFilterRule TVR_SET_DELTA_FILTER_RULE =
      new TvrSetDeltaFilterRule();
  public static final TvrValueSemanticsFilterRule
      TVR_VALUE_SEMANTICS_FILTER_RULE = new TvrValueSemanticsFilterRule();
}

abstract class TvrFilterRuleBase extends RelOptTvrRule {
  TvrFilterRuleBase(Class<? extends TvrSemantics> tvrClass) {
    super(operand(LogicalFilter.class, tvrEdgeSSMax(tvr()),
        operand(RelSubset.class,
            tvrEdgeSSMax(tvr(tvrEdge(tvrClass, logicalSubset()))), any())));
  }
}

/**
 * Tvr set delta filter rule
 */
class TvrSetDeltaFilterRule extends TvrFilterRuleBase {
  TvrSetDeltaFilterRule() {
    super(TvrSetDelta.class);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch filterAny = getRoot(call);
    LogicalFilter logicalFilter = filterAny.get();
    RelOfTvr input = filterAny.input(0).tvrSibling();

    RelNode newLogicalFilter = logicalFilter
        .copy(logicalFilter.getTraitSet(), singletonList(input.rel.get()));

    transformToRootTvr(call, newLogicalFilter, input.tvrSemantics);
  }
}

/**
 * Tvr value semantics filter rule
 */
class TvrValueSemanticsFilterRule extends TvrFilterRuleBase {
  TvrValueSemanticsFilterRule() {
    super(TvrValueDelta.class);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RelMatch filterAny = getRoot(call);
    LogicalFilter logicalFilter = filterAny.get();
    TvrValueDelta inputTrait =
        (TvrValueDelta) filterAny.input(0).tvrSibling().tvrSemantics;

    // Value delta matches this rule if and only if the filtered columns do not
    // include aggregation columns.
    TvrSemanticsTransformer transformer = inputTrait.getTransformer();
    TransformerPredicate predicate = new TransformerPredicate() {
      @Override public boolean test(TvrAggregateTransformer transformer, ImmutableIntList indices) {
        return indices.size() == 1 && indices.stream().noneMatch(
            transformer::referToPartialResult);
      }

      @Override public boolean test(TvrProjectTransformer transformer, ImmutableIntList indices) {
        return indices.size() == 1;
      }

      @Override public boolean test(TvrSortTransformer transformer, ImmutableIntList indices) {
        return false;
      }
    };

    return TvrRelOptUtils.findUniqueInputRef(logicalFilter.getCondition())
        .stream().allMatch(p -> transformer.isCompatible(predicate, ImmutableIntList.of(p.getIndex())));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch filterAny = getRoot(call);
    LogicalFilter logicalFilter = filterAny.get();
    RelOfTvr input = filterAny.input(0).tvrSibling();

    TvrValueDelta valueTrait = (TvrValueDelta) input.tvrSemantics;
    Map<Integer, ImmutableIntList> backwardMapping =
        valueTrait.getTransformer().backwardMapping();
    RexNode condition = logicalFilter.getCondition();
    RexInputRefReplacer replacer =
        new RexInputRefReplacer(logicalFilter.getCluster().getRexBuilder(),
            i -> backwardMapping.get(i).get(0));
    RexNode newCondition = replacer.apply(condition);
    RelNode newLogicalFilter = logicalFilter
        .copy(logicalFilter.getTraitSet(), input.rel.get(), newCondition);

    transformToRootTvr(call, newLogicalFilter, input.tvrSemantics);
  }
}

