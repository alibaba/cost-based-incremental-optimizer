package org.apache.calcite.rel.tvr.trait;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.tvr.TvrSetSemantics;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.tvr.utils.ProjectBuilder;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;

public class TvrSetDelta extends TvrSetSemantics {
  private final boolean isPositiveOnly;

  public TvrSetDelta(TvrVersion fromVersion, TvrVersion toVersion,
      boolean isPositiveOnly) {
    super(fromVersion, toVersion);
    this.isPositiveOnly = isPositiveOnly;
  }

  @Override
  public RelDataType deriveRowType(RelDataType inputRowType,
      RelDataTypeFactory typeFactory) {
    if (isPositiveOnly) {
      return inputRowType;
    }

    Builder builder = new Builder(typeFactory);
    List<RelDataTypeField> fields = inputRowType.getFieldList();

    // skip the multiplicity column which always locates at the end
    fields.stream().limit(fields.size() - 1).forEach(builder::add);
    return builder.build();
  }

  // the index of the multiplicity column with type BIGINT,
  // it is always available in positive/negative delta
  // where +1 represents insertion and -1 represents deletion
  public int getIndexOfMultiplicity(RelNode rel) {
    assert !isPositiveOnly :
        "Positive-only set delta does not have multiplicity";
    return getIndexOfMultiplicity(rel.getRowType());
  }

  public int getIndexOfMultiplicity(RelDataType dataType) {
    assert !isPositiveOnly :
        "Positive-only set delta does not have multiplicity";
    return dataType.getFieldCount() - 1;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TvrSetDelta)) {
      return false;
    }

    if (isPositiveOnly != ((TvrSetDelta) obj).isPositiveOnly) {
      return false;
    }

    return super.equals(obj);
  }

  @Override
  public String toString() {
    return "SetDelta" + (isPositiveOnly ? "+" : "") + "@(" + fromVersion + ", "
        + toVersion + ")";
  }

  @Override
  public TvrSetDelta copy(TvrVersion from, TvrVersion to) {
    return new TvrSetDelta(from, to, isPositiveOnly);
  }

  public boolean isPositiveOnly() {
    return isPositiveOnly;
  }

  public static class TvrSetDeltaSerde
      implements JsonDeserializer<TvrSetDelta>, JsonSerializer<TvrSetDelta> {

    public static TvrSetDeltaSerde INSTANCE = new TvrSetDeltaSerde();

    @Override
    public TvrSetDelta deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      TvrVersion fromVersion =
          context.deserialize(jsonObject.get("fromVersion"), TvrVersion.class);
      TvrVersion toVersion =
          context.deserialize(jsonObject.get("toVersion"), TvrVersion.class);
      boolean isPositive = jsonObject.get("isPositiveOnly").getAsBoolean();
      return new TvrSetDelta(fromVersion, toVersion, isPositive);
    }

    @Override
    public JsonElement serialize(TvrSetDelta src, Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject jsonObject = new JsonObject();
      jsonObject.add("fromVersion", context.serialize(src.fromVersion, TvrVersion.class));
      jsonObject.add("toVersion", context.serialize(src.toVersion, TvrVersion.class));
      jsonObject.addProperty("isPositiveOnly", src.isPositiveOnly());
      return jsonObject;
    }
  }

  /**
   * Consolidate a SetDelta so that there's no entry with both +1 and -1
   * tuples.
   *
   *    (if has key) OdpsLogicalFilter (filter out multiplicity = 0)
   *    (if no key)  Duplicate with TableFunctionScan then project
   *                          |
   *                 OdpsLogicalAggregate (delta +-)
   *                          |
   *                       input (delta +-)
   */
  public static RelNode consolidate(RelNode input, RexBuilder rexBuilder,
      TvrSetDelta setDelta, boolean dropMultiplicityColumn) {
    RelOptCluster cluster = input.getCluster();
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    if (setDelta.isPositiveOnly()) {
      return input;
    }

    // Aggregate and sum the multiplicity
    int multiplicityIndex = setDelta.getIndexOfMultiplicity(input);

    // Aggregation group by columns: all columns except the last multiplicity column
    ImmutableBitSet groupByKeys = ImmutableBitSet.range(0, multiplicityIndex);

    AggregateCall aggregateCall = AggregateCall.create(
        // SUM aggregation function
        SqlStdOperatorTable.SUM, false, false,
        // SUM argument: the last column (multiplicity)
        singletonList(multiplicityIndex), -1, groupByKeys.size(),
        // SUM output data type: BIGINT (same as multiplicity data type)
        input, null, TvrUtils.getMultiplicityName());

    LogicalAggregate logicalAggregate = LogicalAggregate
        .create(input, false, groupByKeys, null, singletonList(aggregateCall));

    // - filter out multiplicity 0 (if has key)
    // - one to many duplicate (if no key)
    RelNode outputRelNode;
    if (TvrUtils.inputExistsKey(input)) {
      RexInputRef multiplicityRef =
          rexBuilder.makeInputRef(logicalAggregate, multiplicityIndex);

      RexLiteral zeroLiteral = rexBuilder.makeBigintLiteral(BigDecimal.ZERO);
      RexNode notZero = rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS,
          ImmutableList.of(multiplicityRef, zeroLiteral));
      LogicalFilter filter =
              LogicalFilter.create(logicalAggregate, notZero);

      if (dropMultiplicityColumn) {
        outputRelNode =
            ProjectBuilder.anchor(filter).addAllBut(multiplicityIndex).build();
      } else {
        outputRelNode = filter;
      }
    } else {
      outputRelNode =
          createOneToManyMapping(logicalAggregate, setDelta, cluster,
              typeFactory, rexBuilder, dropMultiplicityColumn);
    }
    return outputRelNode;
  }

  /**
   * Mapping a record to multiple records by adding DUPLICATE and projection.
   * In the previous subtraction of "row_count" in the two tables, its value is
   * still an aggregate result. Here we convert it into a single row of records.
   *
   * the plan is similar to
   * SELECT all_original_columns, if (__multiplicity__ > 0, 1, -1)
   * FROM agg_result
   * LATERAL VIEW duplicate(abs(__multiplicity__)) t as __dup_row_count__;
   *
   * For example:
   * After TableFunctionScan with DUPLICATE on ABS(__multiplicity__)
   * - column_1  __multiplicity__  -> column_1  __multiplicity__  __duplicate_row_count__
   * -    a           + 2                a           + 2                  + 2
   * -                                   a           + 2                  + 2
   * -    a           - 2                a           - 2                  + 2
   * -                                   a           - 2                  + 2
   *
   * After Projection:
   * - column_1  __multiplicity__  -> column_1  __multiplicity__
   * -    a           + 2                a           + 1
   * -                                   a           + 1
   * -    a           - 2                a           - 1
   * -                                   a           - 1
   *
   * @param inputNode is the input union node
   * @return project node
   */
  private static RelNode createOneToManyMapping(RelNode inputNode,
      TvrSetDelta setDelta, RelOptCluster cluster,
      RelDataTypeFactory typeFactory, RexBuilder rexBuilder,
      boolean dropMultiplicityColumn) {
    List<RelDataTypeField> originFields = inputNode.getRowType().getFieldList();

    final int multiplicityIndex = setDelta.getIndexOfMultiplicity(inputNode);
    RelDataType multiplicityType = originFields.get(multiplicityIndex).getType();
    assert !multiplicityType.isNullable();

    List<RexNode> projects = IntStream.range(0, originFields.size() - 1)
            .mapToObj(i -> rexBuilder.makeInputRef(inputNode, i)).collect(Collectors.toList());
    projects.add(rexBuilder.makeCall(SqlStdOperatorTable.ABS,
        rexBuilder.makeInputRef(inputNode, multiplicityIndex)));

    RelNode projectNode = LogicalProject
        .create(inputNode, projects, inputNode.getRowType().getFieldNames());

    String duplicateFieldName = "__dup_row_count__";
    RelDataType dupRexCallType = typeFactory
        .createStructType(singletonList(multiplicityType),
            singletonList(duplicateFieldName));

    if (true) {
      throw new UnsupportedOperationException("figure out how to create duplicate rex call");
    }

    // create a DUPLICATE function on __dup_row_count__
    RexCall dupRexCall = new RexCall(dupRexCallType,
  null, // TODO
//            new SqlFunction.create("", "DUPLICATE", true, ImmutableMap.of(), multiplicityType, null, cluster),

            singletonList(rexBuilder
        .makeInputRef(projectNode, multiplicityIndex)));

    // create a new column:__dup_row_count__
    Builder builder = new Builder(typeFactory);
    builder.addAll(projectNode.getRowType().getFieldList());
    builder.add(duplicateFieldName, multiplicityType);
    // return type of the duplicate UDTF adds one more column:
    RelDataType dupUdtfRowType = builder.build();

    RexCall wrappedDupRexCall = (RexCall) rexBuilder.makeCall(dupUdtfRowType,
        null, // TODO
        // new OdpsSqlFunction.WrapApplySqlFunction(singletonList(dupRexCall)),
        new ArrayList<>());

    Set<RelColumnMapping> columnMappings =
        IntStream.range(0, dupUdtfRowType.getFieldCount() - 1)
            .mapToObj(i -> new RelColumnMapping(i, 0, i, false))
            .collect(toSet());

    // use ODPS built-in Table Function "duplicate" to generate multiple
    // tuples based row_count
    RelNode dupUdtf = LogicalTableFunctionScan
        .create(cluster, singletonList(projectNode), wrappedDupRexCall,
            null, dupUdtfRowType, columnMappings);

    ProjectBuilder projectbuilder = ProjectBuilder.anchor(dupUdtf)
        // remove the original __multiplicity__ column
        // and the last __dup_row_count__ column
        .addAllBut(dupUdtfRowType.getFieldCount() - 2,
            dupUdtfRowType.getFieldCount() - 1);

    if (!dropMultiplicityColumn) {
      // add a projection on original multiplicity column
      // if (__multiplicity__ > 0, 1, -1)
      projectbuilder = projectbuilder.add(rexBuilder.makeCall(SqlStdOperatorTable.CASE,
          rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, rexBuilder
                  .makeInputRef(dupUdtf, dupUdtfRowType.getFieldCount() - 2),
              rexBuilder.makeBigintLiteral(BigDecimal.ZERO)),
          rexBuilder.makeBigintLiteral(BigDecimal.ONE),
          rexBuilder.makeBigintLiteral(BigDecimal.valueOf(-1L))),
          TvrUtils.getMultiplicityName());
    }
    RelNode logicalProject = projectbuilder.build();

    if (!dropMultiplicityColumn) {
      RelDataTypeField lastField = logicalProject.getRowType().getFieldList()
          .get(logicalProject.getRowType().getFieldList().size() - 1);
      assert lastField.getName().equals(TvrUtils.getMultiplicityName()) && lastField
          .getType().equals(TvrUtils.getMultiplicityType(typeFactory, false));
    }
    return logicalProject;
  }
}
