package org.apache.calcite.rel.tvr.rules.operators;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrSetSemantics;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.trait.TvrValueDelta;
import org.apache.calcite.rel.tvr.trait.transformer.TvrAggregateTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrProjectTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrSemanticsTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrSortTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.predicate.TransformerPredicate;
import org.apache.calcite.rel.tvr.utils.ProjectBuilder;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.calcite.rel.tvr.rules.operators.TvrAggregateRules.SUPPORTED_AGG_FUNCS;
import static org.apache.calcite.rel.tvr.rules.operators.TvrAggregateRules.hasGroupingSets;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.*;

public class TvrAggregateRules {
  public static final RelOptTvrRule POSITIVE_AGG_RULE =
      new TvrLogicalAggregateRule();
  public static final RelOptTvrRule NON_POSITIVE_AGG_RULE =
      new TvrLinearAggregateSetDeltaRule();
  public static final RelOptTvrRule POSITIVE_VALUE_DELTA_AGG_RULE =
      new TvrAggregateValueDeltaRule();

  // we also support average, we rely on AggregateReduceFunctionsRule to rewrite AVG to SUM/COUNT
  public static final ImmutableSet<String> SUPPORTED_AGG_FUNCS =
          ImmutableSet.of("SUM", "COUNT", "MIN", "MAX");

  public static final ImmutableSet<String> RETRACTABLE_AGG_FUNCS =
      ImmutableSet.of("SUM", "COUNT");
  public static final ImmutableSet<String> CAN_OUTPUT_TO_NEXT_AGG_FUNC_TYPE =
      ImmutableSet.of("SUM", "COUNT");
  public static final ImmutableSet<String> CAN_RECEIVE_PREVIOUS_AGG_RESULT_FUNC_TYPE =
      ImmutableSet.of("SUM");

  public static boolean retractable(AggregateCall c) {
      SqlAggFunction aggr = c.getAggregation();
    return !c.isDistinct() && !c.isApproximate()
        && TvrAggregateRules.RETRACTABLE_AGG_FUNCS.contains(aggr.getName().toUpperCase());
  }

  public static boolean hasGroupingSets(Aggregate aggregate) {
    return aggregate.indicator;
  }
}

abstract class TvrAggregateRuleBase extends RelOptTvrRule {

  protected TvrAggregateRuleBase(RelOptRuleOperand operand) {
    super(operand);
  }


  protected void transform(RelOptRuleCall call, RelNode input,
      List<AggregateCall> newAggregateCalls, ImmutableBitSet newGroupSet,
      List<ImmutableBitSet> newGroupSets, boolean indicator, TvrVersion fromVersion,
      TvrVersion toVersion, boolean isPositiveOnly, RelDataTypeFactory typeFactory) {
    LogicalAggregate partialOnlyAgg =
        createPartialAgg(input, newAggregateCalls, newGroupSet, newGroupSets);

    TvrSemantics valueTrait = new TvrValueDelta(fromVersion, toVersion,
        createFinalTransformer(partialOnlyAgg, typeFactory), isPositiveOnly);

    transformToRootTvr(call, partialOnlyAgg, valueTrait);
  }

  private LogicalAggregate createPartialAgg(RelNode newInput,
      List<AggregateCall> newAggregateCalls, ImmutableBitSet newGroupSet,
      List<ImmutableBitSet> newGroupSets) {
    // generate the partial aggregate calls from the complete agg calls
    int groupCount = newGroupSet.cardinality();

    List<AggregateCall> partialAggregateCalls = new ArrayList<>();
    newAggregateCalls.forEach(aggCall -> {
      String aggFunc = aggCall.getAggregation().getName();
      if (aggFunc.equalsIgnoreCase("SUM") || aggFunc.equalsIgnoreCase("COUNT")
              || aggFunc.equalsIgnoreCase("MIN") || aggFunc.equalsIgnoreCase("MAX")) {
        partialAggregateCalls.add(aggCall);

      } else if (aggFunc.equalsIgnoreCase("AVG")) {
        // change to SUM and COUNT
        partialAggregateCalls.addAll(ImmutableList.of(
                AggregateCall.create(SUM, aggCall.isDistinct(), aggCall.isApproximate(),
                        aggCall.getArgList(), aggCall.filterArg, groupCount, newInput, null,
                        aggCall.name + "_sum_avg_Partial"),
                AggregateCall.create(COUNT, aggCall.isDistinct(), aggCall.isApproximate(),
                        aggCall.getArgList(), aggCall.filterArg, groupCount, newInput, null,
                        aggCall.name + "_count_avg_Partial"))
        );
      } else {
        throw new UnsupportedOperationException("unsupported aggregation function " + aggCall.name);
      }
    });

    return LogicalAggregate.create(newInput, newGroupSet, newGroupSets, partialAggregateCalls);
  }

  private List<List<Integer>> getPartialAggResolveMap(
          LogicalAggregate partial) {
    List<List<Integer>> resolveMap = new ArrayList<>();
    ImmutableBitSet groupSet = partial.getGroupSet();
    final Map<Integer, Integer> distinctColumnMap = Maps.newHashMap();
    int l = groupSet.cardinality();

    for (AggregateCall aggCall : partial.getAggCallList()) {
        AggregateCall c = aggCall;
      List<Integer> args = Lists.newArrayList();

      if (c.isDistinct()) {
        for (int i : c.getArgList()) {
          int idx = groupSet.indexOf(i);
          // Group set do not contain distinct key
          if (idx < 0) {
            //same column only add once
            if (distinctColumnMap.containsKey(i)) {
              idx = distinctColumnMap.get(i);
            } else {
              idx = l;
              ++l;
            }
          }

          args.add(idx);
          distinctColumnMap.put(i, idx);
        }
      } else {
        if (isAggregatePartialStruct(c.getType())) {
          int fieldCount = aggCall.getType().getFieldCount();
          while (fieldCount-- > 0) {
            args.add(l++);
          }
        } else {
          args.add(l++);
        }
      }
      resolveMap.add(args);
    }

    return resolveMap;
  }

    public static boolean isAggregatePartialStruct(RelDataType type) {
        return type.isStruct() && type.getStructKind() == StructKind.NONE;
    }

  private TvrSemanticsTransformer createFinalTransformer(
      LogicalAggregate partialAgg, RelDataTypeFactory typeFactory) {
//    assert partialAgg.getAggType() == LogicalAggregateType.Partial_Only;

    int groupCount = partialAgg.getGroupCount();
//    OdsAggregateCall.Stage stage =
//        TvrUtils.hasDistinctAggCall(partialAgg.getAggCallList()) ?
//            OdpsAggregateCall.Stage.Complete :
//            OdpsAggregateCall.Stage.Final;

    List<AggregateCall> finalAggregateCalls = Lists.newArrayList();
    List<List<Integer>> resolveMap = getPartialAggResolveMap(partialAgg);
    assert resolveMap.size() == partialAgg.getAggCallList().size();
    for (int i = 0; i < resolveMap.size(); i++) {
      AggregateCall a = partialAgg.getAggCallList().get(i);
      AggregateCall finalAggCall = AggregateCall
              .create(a.getAggregation(), a.isDistinct(), a.isApproximate(), resolveMap.get(i), a.filterArg,
                      groupCount, partialAgg, null, a.name);
      finalAggregateCalls.add(finalAggCall);
    }

    // Group set columns are already in the front in the output type of
    // PartialOnlyAggregate
    ImmutableBitSet groupSet = ImmutableBitSet.of(ImmutableIntList
        .range(0, groupCount));

    TvrAggregateTransformer aggTrans = new TvrAggregateTransformer(groupSet,
        ImmutableList.copyOf(finalAggregateCalls),
        ImmutableSet.copyOf(groupSet.asSet()), null);

    // add a project transformer to remove the last column that
    // represents "COUNT(1)" of value snapshot
    int outputFieldNum = finalAggregateCalls.stream().map(aggCall -> {
      RelDataType aggCallType = aggCall.getType();
      if (aggCallType.isStruct()) {
        return aggCallType.getFieldCount();
      } else {
        return 1;
      }
    }).reduce(Integer::sum).orElse(0) + groupCount;
    int indexOfCount = aggTrans.getIndexOfCountInOutputSchema();

    List<Integer> projectRefs =
        IntStream.range(0, outputFieldNum).filter(i -> i != indexOfCount)
            .boxed().collect(Collectors.toList());

    return aggTrans.addNewTransformer(
        new TvrProjectTransformer(ImmutableIntList.copyOf(projectRefs)));
  }

  // support original input is c
  // IF(multiplicity >= 0, c, -c)
  // Note: when input is a SetDelta, multiplicity must be 1 or -1
  private RexNode constructLinearTransform(RexBuilder builder, RexNode orgInput,
      RexNode multiplicityRef) {
    final RexNode geCheck = builder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, Arrays
        .asList(multiplicityRef, builder.makeBigintLiteral(BigDecimal.ZERO)));
    final RexNode negative =
        builder.makeCall(SqlStdOperatorTable.UNARY_MINUS, orgInput);
    return builder.makeCall(SqlStdOperatorTable.CASE,
        Arrays.asList(geCheck, orgInput, negative));
  }

  // support original input is c
  // IF(c is null, c, multiplicityRef)
  private RexNode constructLinearTransform4Count(RexBuilder builder,
      RexNode orgInput, RexNode multiplicityRef) {
    RexNode nullableMultiplicityRef = builder.makeCast(builder.getTypeFactory()
            .createTypeWithNullability(multiplicityRef.getType(), true),
        multiplicityRef);
    if (orgInput == null) {
      return nullableMultiplicityRef;
    }
    final RexNode isNullCheck = builder.makeCall(SqlStdOperatorTable.IS_NOT_NULL,
        Collections.singletonList(orgInput));
    return builder.makeCall(SqlStdOperatorTable.CASE, Arrays
        .asList(isNullCheck,
            builder.makeNullLiteral(nullableMultiplicityRef.getType()),
            multiplicityRef));
  }

  protected AggregateCall  linearProcess(
          AggregateCall aggregateCall, ProjectBuilder builder, RexInputRef multiplicityRef,
          RelNode input, int groupCount) {
    final RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    // change original SUM and COUNT to handle negative multiplicity
    // add a new linear transformed column needed by each aggregate call
    final int index = builder.getNumCols();
    RexNode rex;
    switch (aggregateCall.getAggregation().getName()) {
    case "SUM":
      if (!aggregateCall.getArgList().isEmpty()) {

        final RexInputRef inputRef =
            rexBuilder.makeInputRef(input, aggregateCall.getArgList().get(0));
        rex = constructLinearTransform(rexBuilder, inputRef, multiplicityRef);
      } else {
        throw new UnsupportedOperationException("TODO: aggregate call without arg list");
//        rex = constructLinearTransform(rexBuilder,
//            aggregateCall.getConstantsMap().get(0), multiplicityRef);
      }
      builder.add(rex, "generated" + index);
      return aggregateCall
          .copy(Collections.singletonList(index), aggregateCall.filterArg);
    case "COUNT":
      if (!aggregateCall.getArgList().isEmpty()) {
        final RexInputRef inputRef =
            rexBuilder.makeInputRef(input, aggregateCall.getArgList().get(0));
        rex = constructLinearTransform4Count(rexBuilder, inputRef, multiplicityRef);
      } else {
        rex = constructLinearTransform4Count(rexBuilder, null, multiplicityRef);
      }
      builder.add(rex, "generated" + index);
      return AggregateCall
          .create(SUM, aggregateCall.isDistinct(),
              Collections.singletonList(index), groupCount,
              builder.build().getRowType(),
              aggregateCall.getName() + "__sum");
    default:
      assert false;
    }
    return null;
  }
}

/**
 * This rule is part of the Progressive Computing Prototype. This rule enables
 * Logical Aggregate operator to output data in Delta form.
 */
class TvrLogicalAggregateRule extends TvrAggregateRuleBase {

  TvrLogicalAggregateRule() {
    super(operand(LogicalAggregate.class,
        // We assume all roll-up aggregates are already rewritten into
        // (expand + normal aggregate)
        a -> ! hasGroupingSets(a) &&
                a.getAggCallList().stream().allMatch(c -> SUPPORTED_AGG_FUNCS.contains(c.getAggregation().getName())),
            tvrEdgeSSMax(tvr()), operand(RelSubset.class,
            tvrEdgeSSMax(
                tvr(tvrEdge(TvrSetDelta.class, TvrSetDelta::isPositiveOnly,
                    logicalSubset()))), any())));
  }

  /**
   * [Set delta] - Aggregate - [Value delta]
   * <p>
   * Matches:
   * <p>
   *
   *  OdpsLogicalAggregate --SET_SNAPSHOT_MAX-- Tvr0
   *      |                                          (PositiveOnlySetDelta)
   *    input      -----SET_SNAPSHOT_MAX-- Tvr1 ---------------------------------input
   *
   * <p>
   * Converts to:
   * <p>
   *
   *                                            (PositiveOnlyValueDelta)
   *  OdpsLogicalAggregate --SET_SNAPSHOT_MAX-- Tvr0 --------------- OdpsLogicalAggregate
   *      |                                                                       /
   *      |                                     (PositiveOnlySetDelta)           /
   *    input      ------SET_SNAPSHOT_MAX-- Tvr1 -----------------------------input
   *
   * <p>
   */
  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch aggregateAny = getRoot(call);
    LogicalAggregate aggregate = aggregateAny.get();
    RelOfTvr input = aggregateAny.input(0).tvrSibling(TvrSetSemantics.class);
    RelNode inputNode = input.rel.get();
    TvrSemantics inputTrait = input.tvrSemantics;

    // Add the row count column for value delta
    List<AggregateCall> newAggregateCalls =
        Lists.newArrayList(aggregate.getAggCallList());
    RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();
    newAggregateCalls.add(TvrUtils.getRowCountAggCall(inputNode));

    transform(call, inputNode, newAggregateCalls, aggregate.getGroupSet(),
        aggregate.getGroupSets(), aggregate.indicator, inputTrait.fromVersion,
        inputTrait.toVersion, true, typeFactory);
  }
}

class TvrLinearAggregateSetDeltaRule extends TvrAggregateRuleBase {
  TvrLinearAggregateSetDeltaRule() {
    super(operand(LogicalAggregate.class,
        a -> !hasGroupingSets(a) && a.getAggCallList().stream()
            .allMatch(TvrAggregateRules::retractable), tvrEdgeSSMax(tvr()),
        operand(RelSubset.class, tvrEdgeSSMax(
            tvr(tvrEdge(TvrSetDelta.class, t -> !t.isPositiveOnly(),
                logicalSubset()))), any())));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch aggAny = getRoot(call);
    LogicalAggregate aggregate = aggAny.get();
    RelOfTvr inputRelOfTvr = aggAny.input(0).tvrSibling(TvrSetDelta.class);
    final TvrSetDelta inputDelta = (TvrSetDelta) inputRelOfTvr.tvrSemantics;
    RelNode input = inputRelOfTvr.rel.get();

    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();

    final int multiplicityIndex = inputDelta.getIndexOfMultiplicity(input);
    final RexInputRef
        multiplicityRef = rexBuilder.makeInputRef(input, multiplicityIndex);

    // make pass through projects for original input (except multiplicity)
    ProjectBuilder builder = ProjectBuilder.anchor(input).addAllBut(multiplicityIndex);

    List<AggregateCall> newAggregateCalls = new ArrayList<>(aggregate.getAggCallList());
    RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();
    newAggregateCalls.add(TvrUtils.getRowCountAggCall(input));

    int groupCount = aggregate.getGroupCount();
    newAggregateCalls = newAggregateCalls.stream().map(
        aggCall -> linearProcess(aggCall, builder,
            multiplicityRef, input, groupCount)).collect(Collectors.toList());

    // move the multiplicity column to the end
    builder.add(rexBuilder.makeInputRef(input, multiplicityIndex),
        TvrUtils.getMultiplicityName());

    RelNode p = builder.build();
    p = p.copy(input.getTraitSet(), p.getInputs());

    transform(call, p, newAggregateCalls, aggregate.getGroupSet(),
        aggregate.getGroupSets(), aggregate.indicator, inputDelta.fromVersion,
        inputDelta.toVersion, false, typeFactory);
  }
}

class TvrAggregateValueDeltaRule extends TvrAggregateRuleBase {

  private static final TransformerPredicate groupKeyPredicate =
      new TransformerPredicate() {
        @Override
        public boolean test(TvrAggregateTransformer transformer,
            ImmutableIntList indices) {
          if (indices.size() != 1) {
            return false;
          }
          return indices.stream().noneMatch(transformer::referToPartialResult);
        }

        @Override
        public boolean test(TvrProjectTransformer transformer,
            ImmutableIntList indices) {
          return indices.size() == 1;
        }

        @Override
        public boolean test(TvrSortTransformer transformer,
                            ImmutableIntList indices) {
          return false;
        }
      };

  private static final TransformerPredicate aggArgPredicate =
      new TransformerPredicate() {
        @Override
        public boolean test(TvrAggregateTransformer transformer,
            ImmutableIntList indices) {
          if (indices.size() != 1) {
            return false;
          }
          return indices.stream().allMatch(index -> {
            if (!transformer.referToPartialResult(index)) {
              return false;
            }

            AggregateCall call = transformer.getAggregateCalls()
                .get(index - transformer.getNumGroupKeys());
            SqlAggFunction func = call.getAggregation();
            return !call.isDistinct() && !call.isApproximate() &&
                TvrAggregateRules.CAN_OUTPUT_TO_NEXT_AGG_FUNC_TYPE.contains(func.getName().toUpperCase());
          });
        }

        @Override
        public boolean test(TvrProjectTransformer transformer,
            ImmutableIntList indices) {
          return indices.size() == 1;
        }

        @Override
        public boolean test(TvrSortTransformer transformer,
            ImmutableIntList indices) {
          return false;
        }
      };

  TvrAggregateValueDeltaRule() {
    super(operand(LogicalAggregate.class,
            a -> !hasGroupingSets(a),
        tvrEdgeSSMax(tvr()), operand(RelSubset.class,
            tvrEdgeSSMax(tvr(tvrEdge(TvrValueDelta.class, logicalSubset()))),
            any())));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    LogicalAggregate aggregate = root.get();
    TvrValueDelta inputTrait =
        (TvrValueDelta) root.input(0).tvrSibling().tvrSemantics;

    if (!inputTrait.isPositiveOnly() && aggregate.getAggCallList().stream()
        .anyMatch(TvrAggregateRules::retractable)) {
      return false;
    }

    if (!aggregate.getAggCallList().stream().allMatch(aggCall -> {
      SqlAggFunction aggr = aggCall.getAggregation();
      return !aggCall.isDistinct() && !aggCall.isApproximate()
//              && aggr.isBuiltin()
          && TvrAggregateRules.CAN_RECEIVE_PREVIOUS_AGG_RESULT_FUNC_TYPE
          .contains(aggr.getName().toUpperCase());
    })) {
      return false;
    }

    TvrSemanticsTransformer transformer = inputTrait.getTransformer();


    final boolean groupKeyCompatible = aggregate.getGroupSet().toList().stream()
        .allMatch(index -> transformer.isCompatible(groupKeyPredicate, ImmutableIntList.of(index)));

    if (!groupKeyCompatible) {
      return false;
    }

    return aggregate.getAggCallList().stream()
        .flatMap(aggCall -> aggCall.getArgList().stream())
        .allMatch(index -> transformer.isCompatible(aggArgPredicate, ImmutableIntList.of(index)));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    LogicalAggregate aggregate = root.get();
    RelOfTvr inputSibling = root.input(0).tvrSibling();
    RelNode input = inputSibling.rel.get();

    TvrValueDelta valueTrait = (TvrValueDelta) inputSibling.tvrSemantics;
    TvrSemanticsTransformer transformer = valueTrait.getTransformer();

    Map<Integer, ImmutableIntList> backwardMapping =
        transformer.backwardMapping();

    List<Integer> newGroupSet =
        aggregate.getGroupSet().asList().stream().map(i -> {
          ImmutableIntList newKeys = backwardMapping.get(i);
          assert newKeys.size() == 1;
          return newKeys.get(0);
        }).collect(Collectors.toList());
    ImmutableBitSet newGroupBitSet = ImmutableBitSet.of(newGroupSet);

    int indexOfRowCount = input.getRowType().getFieldCount() - 1;
    List<AggregateCall> newAggregateCalls =
        aggregate.getAggCallList().stream().map(aggCall -> {
          AggregateCall oldCall = aggCall;

          List<Integer> newArgList = aggCall.getArgList().stream().map(i -> {
            ImmutableIntList newArgs = backwardMapping.get(i);
            assert newArgs.size() == 1;
            return newArgs.get(0);
          }).collect(Collectors.toList());

          return AggregateCall
              .create(oldCall.getAggregation(), oldCall.isDistinct(),
                  newArgList, aggregate.getGroupCount(), oldCall.type,
                  oldCall.name);
        }).collect(Collectors.toList());

    // Add the row count column for value delta
    RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();
//    Integer indexOfRowCount =
//        Objects.requireNonNull(valueTrait.getIndexOfRowCountAggCall());
    newAggregateCalls.add(AggregateCall
        .create(SUM, false,
            Collections.singletonList(indexOfRowCount), newGroupSet.size(),
            input.getRowType(), "__row_count_sum__"));

    transform(call, input, newAggregateCalls, newGroupBitSet,
        ImmutableList.of(newGroupBitSet), aggregate.indicator,
        valueTrait.fromVersion, valueTrait.toVersion, valueTrait.isPositiveOnly(), typeFactory);
  }
}
