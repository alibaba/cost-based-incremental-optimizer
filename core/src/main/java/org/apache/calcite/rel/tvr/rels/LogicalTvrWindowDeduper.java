package org.apache.calcite.rel.tvr.rels;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.tvr.utils.ProjectBuilder;
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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class LogicalTvrWindowDeduper extends LogicalTvrDeduper {

  // create window
  public ImmutableBitSet partitionKeys;
  private final RelCollation collation;
  private final RelDataType windowColumnType;

  // create filter
  private final BigDecimal limit;

  // create project
  private final ImmutableIntList inputRefs;

  private LogicalTvrWindowDeduper(RelOptCluster cluster, RelTraitSet traits,
      RelNode input, ImmutableBitSet partitionKeys, RelDataType windowColumnType,
      RelCollation collation, BigDecimal limit, ImmutableIntList inputRefs) {
    super(cluster, traits, input);
    this.partitionKeys = partitionKeys;
    this.windowColumnType = windowColumnType;
    this.collation = collation;
    this.limit = limit;
    this.inputRefs = inputRefs;
  }

  public static LogicalTvrWindowDeduper create(RelNode input,
      ImmutableBitSet partitionKeys, RelDataType windowColumn,
      RelCollation collation, BigDecimal limit, ImmutableIntList inputRefs) {
    return new LogicalTvrWindowDeduper(input.getCluster(), input.getTraitSet(),
        input, partitionKeys, windowColumn, collation, limit, inputRefs);
  }

  @Override
  public RelNode apply() {
    RelDataTypeFactory typeFactory = input.getCluster().getTypeFactory();

    // 1. create window
    RelDataType newRowType =
        typeFactory.builder().addAll(input.getRowType().getFieldList())
            .add("__tvr__window", windowColumnType).build();
    LogicalWindow window = createWindow(input, newRowType);

    // 2. create filter
    LogicalFilter newFilter = createFilter(window);

    // 3. create project
    ProjectBuilder builder = ProjectBuilder.anchor(newFilter);
    builder.addAll(inputRefs);
    return builder.build();
  }

  private LogicalWindow createWindow(RelNode relNode, RelDataType rowType) {
    List<RexLiteral> constants = new ArrayList<>();
    int fieldCount = rowType.getFieldCount();
    RelDataTypeField windowColumn = rowType.getFieldList().get(fieldCount - 1);
    Window.RexWinAggCall windowCall =
        new Window.RexWinAggCall(SqlStdOperatorTable.ROW_NUMBER,
            windowColumn.getType(), ImmutableList.of(), 0, false);
    Window.Group group =
        new LogicalWindow.Group(partitionKeys, true, null, null, collation,
            Collections.singletonList(windowCall));
    return LogicalWindow
        .create(relNode.getTraitSet(), relNode, constants, rowType,
            Collections.singletonList(group));
  }

  private LogicalFilter createFilter(RelNode relNode) {
    RexBuilder rexBuilder = relNode.getCluster().getRexBuilder();
    RexInputRef countRef = rexBuilder
        .makeInputRef(relNode, relNode.getRowType().getFieldCount() - 1);
    RexLiteral literal = rexBuilder.makeBigintLiteral(limit);
    RexNode filterCondition = rexBuilder
        .makeCall(SqlStdOperatorTable.LESS_THAN, ImmutableList.of(countRef, literal));
    return LogicalFilter.create(relNode, filterCondition, null);
  }

  @Override
  protected RelDataType deriveRowType() {
    // window deduper will not modify the row type of the input
    return input.getRowType();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    if (inputs.size() != 1) {
      throw new IllegalArgumentException(
          "LogicalTvrWindowDeduper should have only one input, but receive "
              + inputs.size());
    }
    RelNode input = inputs.get(0);
    return new LogicalTvrWindowDeduper(input.getCluster(), input.getTraitSet(),
        input, partitionKeys, windowColumnType, collation, limit, inputRefs);
  }

  @Override
  public boolean valueEquals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LogicalTvrWindowDeduper deduper = (LogicalTvrWindowDeduper) o;
    return Objects.equals(partitionKeys, deduper.partitionKeys) && Objects
        .equals(collation, deduper.collation) && Objects
        .equals(windowColumnType, deduper.windowColumnType) && Objects
        .equals(limit, deduper.limit) && Objects
        .equals(inputRefs, deduper.inputRefs);
  }
}
