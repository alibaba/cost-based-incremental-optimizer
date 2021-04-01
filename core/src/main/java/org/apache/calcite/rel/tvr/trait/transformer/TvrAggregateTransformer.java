package org.apache.calcite.rel.tvr.trait.transformer;

import com.google.common.collect.*;
import com.google.gson.*;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.tvr.trait.transformer.predicate.TransformerPredicate;
import org.apache.calcite.rel.tvr.utils.ProjectBuilder;
import org.apache.calcite.rel.tvr.utils.TvrJsonUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.*;

/**
 * Sample implementation for aggregate transformer (for value semantics).
 */
public class TvrAggregateTransformer extends TvrSemanticsTransformer {

  /**
   * Bit set of grouping fields.
   * The indices referring to the input schema.
   */
  private final ImmutableBitSet groupSet;

  /**
   * Collection of calls to aggregate functions.
   */
  private final ImmutableList<AggregateCall> aggregateCalls;

  /**
   * The indices referring to the primary key columns.
   */
  private final ImmutableSet<Integer> primaryKeys;

  /**
   * index of the multiplicity column with type BIGINT,
   * it is required in positive/negative delta
   * where +1 represents insertion and -1 represents deletion
   * multiplicity is a part of group set
   */
  private final Integer multiplicityIndex;

  private final int numGroupKeys;

  public int getNumGroupKeys() {
    return numGroupKeys;
  }

  public TvrAggregateTransformer(ImmutableBitSet groupSet,
      ImmutableList<AggregateCall> aggregateCalls,
      ImmutableSet<Integer> primaryKeys,
      Integer multiplicityIndex) {
    this.groupSet = groupSet;
    this.aggregateCalls = aggregateCalls;
    this.primaryKeys = primaryKeys;
    this.multiplicityIndex = multiplicityIndex;

    this.numGroupKeys = groupSet.cardinality();

    for (int i = 0; i < numGroupKeys; i++) {
      ImmutableIntList groupKey = ImmutableIntList.of(groupSet.nth(i));
      forwardMapping.put(groupKey, i);
      backwardMapping.put(i, groupKey);
    }

    for (int i = 0; i < aggregateCalls.size(); i++) {
      ImmutableIntList args =
          ImmutableIntList.copyOf(aggregateCalls.get(i).getArgList());
      forwardMapping.put(args, i + numGroupKeys);
      backwardMapping.put(i + numGroupKeys, args);
    }
  }

  @Override
  public TvrSemanticsTransformer transform(
      Multimap<Integer, Integer> mapping) {
    // 1. update group set & primary keys
    List<Integer> newGroupSet =
        groupSet.asList().stream().filter(mapping::containsKey).map(i ->
            // the mapping must contain the original group column
            Objects.requireNonNull(getFirstMappingResult(mapping, i)))
            .collect(Collectors.toList());

    // if the key in the mapping is negative, it means this column is newly added by a project,
    // and it should be a part of the group set
    List<Integer> newGroupCols =
        mapping.entries().stream().filter(e -> e.getKey() < 0)
            .map(Map.Entry::getValue).collect(Collectors.toList());
    newGroupSet.addAll(newGroupCols);

    List<Integer> newPrimaryKeys = primaryKeys.asList().stream().map(i ->
        // the mapping must contain the original primary column
        Objects.requireNonNull(getFirstMappingResult(mapping, i))).
        collect(Collectors.toList());

    // 2. multiplicity
    Integer multiplicityIndex = this.multiplicityIndex == null ?
        null :
        getFirstMappingResult(mapping, this.multiplicityIndex);

    // 3. update aggCalls
    List<AggregateCall> newAggregateCalls = aggregateCalls.stream().filter(
        aggCall -> aggCall.getArgList().stream().allMatch(mapping::containsKey))
        .map(oldCall -> {
          List<Integer> newArgList = oldCall.getArgList().stream()
              .map(i -> mapping.get(i).iterator().next())
              .collect(Collectors.toList());

          return AggregateCall
                  .create(oldCall.getAggregation(), oldCall.isDistinct(), oldCall.isApproximate(),
                          newArgList, oldCall.filterArg, groupSet.size(), null, oldCall.type, oldCall.name);
        }).collect(Collectors.toList());

    return new TvrAggregateTransformer(ImmutableBitSet.of(newGroupSet),
        ImmutableList.copyOf(newAggregateCalls),
        ImmutableSet.copyOf(newPrimaryKeys),
        multiplicityIndex);
  }

  @Override
  public Set<Integer> getRequiredColumns() {
    Set<Integer> requiredColumns = new HashSet<>(primaryKeys);
    requiredColumns.add(getIndexOfCountInInputSchema());
    if (multiplicityIndex != null) {
      requiredColumns.add(multiplicityIndex);
    }
    return requiredColumns;
  }

  /**
   * Check if the given index in the SetSnapshot schema refers to
   * an aggregate column that is partially computed in this value semantics
   */
  @Override
  public boolean referToPartialResult(int index) {
    return index >= numGroupKeys && index < (numGroupKeys + aggregateCalls.size());
  }

  @Override
  public List<RelNode> apply(List<RelNode> inputs) {
    if (multiplicityIndex != null) {
      throw new UnsupportedOperationException(
          "positive-negative value delta does not have multiplicity column now.");
    }
    assert inputs.size() == 1;
    RelNode input = inputs.get(0);
    RelOptCluster cluster = input.getCluster();

    // 1. Aggregate.
    List<LogicalAggregate> finalAggregates = new ArrayList<>();
    List<Pair<List<Integer>, List<Integer>>> resolveMap =
        this.aggregateCalls.stream()
            .map(aggCall -> Pair.of((List<Integer>) null, aggCall.getArgList()))
            .collect(Collectors.toList());

    if (this.aggregateCalls.stream().anyMatch(AggregateCall::isDistinct)) {
      throw new UnsupportedOperationException("distinct not supported yet");
//      boolean twoPhaseAggregateMatches = AggregateRules
//          .twoPhaseAggregateMatches(cluster, this.numGroupKeys, this.aggregateCalls);
//      if (twoPhaseAggregateMatches) {
//        // [Progressive] Dedup -> (merge value delta) -> Complete
//        AbstractAggregate finalAgg = AbstractAggregate
//            .resolve(input, this.groupSet, Collections.emptyList(),
//                this.aggregateCalls, AbstractAggregate.Phase.FINAL,
//                AbstractAggregate.Method.TWO, resolveMap);
//        finalAggregates.add(finalAgg);
//      }
//
//      if (AggregateRules
//          .threePhaseAggregateMatches(cluster.getPlanner(), input,
//              this.groupSet, twoPhaseAggregateMatches)) {
//        // [Progressive] Dedup -> (merge value delta) -> Partial_1 -> Final
//        AbstractAggregate partialAgg = AbstractAggregate
//            .resolve(input, this.groupSet, Collections.emptyList(),
//                this.aggregateCalls, AbstractAggregate.Phase.PARTIAL,
//                AbstractAggregate.Method.THREE, resolveMap);
//        finalAggregates.add(
//            indirectAgg(partialAgg, AbstractAggregate.Phase.FINAL,
//                AbstractAggregate.Method.THREE));
//      }
    } else {
      finalAggregates.add(createFinalAgg(input));
    }

    // 2. Filter
    assert !finalAggregates.isEmpty();
    RexBuilder rexBuilder = cluster.getRexBuilder();

    // create condition "COUNT(1) > 0" for filter.
    // the row types of all the final aggregates are the same, so we use the first one to
    // create filter here
    int indexOfCount = getIndexOfCountInOutputSchema();
    RelDataType countType =
        finalAggregates.get(0).getRowType().getFieldList().get(indexOfCount).getType();
    RexInputRef countInputRef = new RexInputRef(indexOfCount, countType);

    RexLiteral zeroLiteral = rexBuilder.makeBigintLiteral(BigDecimal.ZERO);
    RexNode greaterThanZero = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
        ImmutableList.of(countInputRef, zeroLiteral));

    // create filter
    return finalAggregates.stream()
        .map(agg -> LogicalFilter.create(agg, greaterThanZero))
        .collect(Collectors.toList());

//    // 3. Project (for average only)
//    // for average aggregate calls, do the final merge
//    List<RelNode> projects = new ArrayList<>();
//    for (RelNode filter: filters) {
//      if (aggregateCalls.stream().noneMatch(c -> c.name.contains("_avg_Partial"))) {
//        // don't add a filter if there's not average
//        projects.add(filter);
//      } else {
//        ProjectBuilder projectBuilder = ProjectBuilder.anchor(filter);
//        int i = 0;
//        for (AggregateCall call: aggregateCalls) {
//          if (call.name.contains("_sum_avg_Partial")) {
//            // calculate sum/count and restore original avg name
//            RexNode div = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE,
//                    rexBuilder.makeInputRef(filter, i), rexBuilder.makeInputRef(filter, i + 1));
//            String avgName = call.name.substring(0, call.name.indexOf("_sum_avg_Partial"));
//            projectBuilder.add(div, avgName);
//            i++;
//          } else if (call.name.contains("_count_avg_Partial")) {
//            // covered in sum_avg_Partial
//          } else {
//            projectBuilder.add(i);
//            i++;
//          }
//        }
//      }
//    }

//    return projects;
  }


  private LogicalAggregate createFinalAgg(RelNode input) {
    // generate the partial aggregate calls from the complete agg calls
    int groupCount = this.numGroupKeys;

    List<AggregateCall> finalAggregateCalls = new ArrayList<>();
    aggregateCalls.forEach(aggCall -> {
      String aggFunc = aggCall.getAggregation().getName();
      if (aggFunc.equalsIgnoreCase("MIN") || aggFunc.equalsIgnoreCase("MAX")) {
        finalAggregateCalls.add(aggCall);
      } else if (aggFunc.equalsIgnoreCase("SUM") || aggFunc.equalsIgnoreCase("COUNT")) {
        // final aggregate for COUNT is SUM
        // if original type is not nullable, then use SUM0 to create non-nullable types
        if (aggCall.getType().isNullable()) {
          finalAggregateCalls.add(AggregateCall.create(SUM, aggCall.isDistinct(), aggCall.isApproximate(),
                  aggCall.getArgList(), aggCall.filterArg, groupCount, input, null, aggCall.name));
        } else {
          finalAggregateCalls.add(AggregateCall.create(SUM0, aggCall.isDistinct(), aggCall.isApproximate(),
                  aggCall.getArgList(), aggCall.filterArg, groupCount, input, null, aggCall.name));
        }
      } else {
        throw new UnsupportedOperationException("unsupported agg function for final aggregate " + aggCall.name);
      }
    });

    return LogicalAggregate.create(input, groupSet, null, finalAggregateCalls);
  }

  @Override
  public List<RelNode> consolidate(List<RelNode> inputs) {
    if (multiplicityIndex != null) {
      throw new UnsupportedOperationException(
          "positive-negative value delta does not have multiplicity column now.");
    }

    // [Progressive] Dedup -> (merge value delta) -> Complete
    // This pattern has only two phases, there is no chance to do the consolidation

    // [Progressive] Partial_1 -> (merge value delta) -> Final
    // This pattern has only two phases, there is no chance to do the consolidation

    // [Progressive] Partial_1 -> Partial_2 -> (merge value delta) -> Final
    // [Progressive] Partial_1 -> Partial_2 -> Partial_2 -> (merge value delta) -> Final
    // Re-apply the partial-2 to reduce the tuples

    // FIXME: aggregate transformer does not consolidate now.
    return inputs;
  }

  @Override
  public RelDataType deriveRowType(RelDataType inputRowType,
      RelDataTypeFactory factory) {
    RelDataTypeFactory.Builder builder = factory.builder();
    List<RelDataTypeField> inputs = inputRowType.getFieldList();
    for (int i : groupSet) {
      builder.add(inputs.get(i));
    }
    for (int i = 0; i < aggregateCalls.size(); i++) {
      AggregateCall call = aggregateCalls.get(i);
      String name;
      if (call.name != null) {
        name = call.name;
      } else {
        name = "$f" + (this.getNumGroupKeys() + i);
      }
      builder.add(name, call.type);
    }
    return builder.build();
  }

  @Override
  public TvrSemanticsTransformer addNewTransformer(
      TvrSemanticsTransformer newTransformer) {
    if (newTransformer instanceof TvrCompositeTransformer) {
      List<TvrSemanticsTransformer> transformers = Lists.newArrayList(this);
      transformers.addAll(
          ((TvrCompositeTransformer) newTransformer).getInnerTransformers());
      return new TvrCompositeTransformer(transformers);
    }
    return new TvrCompositeTransformer(
        Lists.newArrayList(this, newTransformer));
  }

  @Override
  public boolean isCompatible(TransformerPredicate predicate, ImmutableIntList indices) {
    return predicate.test(this, indices);
  }

  /**
   * Return the Count(1) AggregateCall added by TVR value semantics.
   * We follow the convention that it is always the last column.
   */
  public AggregateCall getCountAggregateCall() {
    return aggregateCalls.get(aggregateCalls.size() - 1);
  }

  // index of the count(1) column which is required for value delta/snapshot
  // this count(1) column indicates the number of tuples contributing to
  // this aggregate group; when count(1)=0, it indicates that this aggregate
  // group should not exist
  public int getIndexOfCountInOutputSchema() {
    return forwardMapping
        .get(ImmutableIntList.copyOf(getCountAggregateCall().getArgList()))
        .iterator().next();
  }

  public int getIndexOfCountInInputSchema() {
    return getCountAggregateCall().getArgList().get(0);
  }

  public ImmutableList<AggregateCall> getAggregateCalls() {
    return aggregateCalls;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TvrAggregateTransformer)) {
      return false;
    }
    TvrAggregateTransformer that = (TvrAggregateTransformer) o;
    return Objects.equals(groupSet, that.groupSet) && Objects
        .equals(aggregateCalls, that.aggregateCalls) && Objects
        .equals(primaryKeys, that.primaryKeys);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupSet, aggregateCalls, primaryKeys);
  }

  public static class TvrAggregateTransformerSerde
      implements JsonDeserializer<TvrAggregateTransformer>,
      JsonSerializer<TvrAggregateTransformer>,
      TvrJsonUtils.ColumnOrderAgnostic {

    protected RelDataType rowType;
    protected RelDataTypeFactory typeFactory;

    public TvrAggregateTransformerSerde(RelDataType rowType,
        RelDataTypeFactory typeFactory) {
      this.rowType = rowType;
      this.typeFactory = typeFactory;
    }

    @Override
    public TvrAggregateTransformer deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      ImmutableBitSet groupSet = ImmutableBitSet.of(c2i(
          context.deserialize(jsonObject.get("groupSet"), String[].class),
          rowType));
      ImmutableSet<Integer> primaryKeys = ImmutableSet.copyOf(c2i(context
              .deserialize(jsonObject.get("primaryKeys"), String[].class),
          rowType));

      List<AggregateCall> aggCalls = new ArrayList<>();
      jsonObject.getAsJsonArray("aggregateCalls").forEach(je -> aggCalls.add(
          context.deserialize(je.getAsJsonObject(), AggregateCall.class)));
      ImmutableList<AggregateCall> aggregateCalls =
          ImmutableList.copyOf(aggCalls);

      Integer multiplicity = null;
      if (jsonObject.get("multiplicity") != null) {
        multiplicity = rowType.getFieldNames()
            .indexOf(jsonObject.get("multiplicity").getAsString());
      }

      return new TvrAggregateTransformer(groupSet, aggregateCalls, primaryKeys,
          multiplicity);
    }

    @Override
    public JsonElement serialize(TvrAggregateTransformer src, Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject jsonObject = new JsonObject();

      jsonObject.add("groupSet",
          context.serialize(i2c(src.groupSet.asList(), rowType)));
      jsonObject.add("primaryKeys",
          context.serialize(i2c(src.primaryKeys.asList(), rowType)));
      if (src.multiplicityIndex != null) {
        jsonObject.add("multiplicity", context.serialize(
            rowType.getFieldList().get(src.multiplicityIndex).getName()));
      }
      jsonObject.add("aggregateCalls", context.serialize(src.aggregateCalls));
      return jsonObject;
    }
  }
}
