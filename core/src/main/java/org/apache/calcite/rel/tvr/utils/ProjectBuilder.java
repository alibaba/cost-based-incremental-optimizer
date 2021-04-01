package org.apache.calcite.rel.tvr.utils;

import com.google.common.primitives.Ints;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ProjectBuilder {

  private RelNode input;
  private RexBuilder rexBuilder;
  private RelDataTypeFactory typeFactory;
  private List<RelDataTypeField> fields;

  private ArrayList<RexNode> projects = new ArrayList<>();
  private ArrayList<String> fieldNames = new ArrayList<>();

  private Set<String> distinctFieldNames = new HashSet<>();

  private ProjectBuilder(RelNode relNode) {
    input = relNode;
    RelOptCluster cluster = input.getCluster();
    rexBuilder = cluster.getRexBuilder();
    typeFactory = cluster.getTypeFactory();
    fields = input.getRowType().getFieldList();
  }

  private void addDistinctFieldName(String fieldName) {
    String newFieldName = fieldName;

    int i = 0;
    while (distinctFieldNames.contains(newFieldName)) {
      newFieldName = fieldName + "_" + i++;
    }

    distinctFieldNames.add(newFieldName);
    fieldNames.add(newFieldName);
  }

  public static ProjectBuilder anchor(RelNode input) {
    return new ProjectBuilder(input);
  }

  public ProjectBuilder add(int i) {
    RelDataTypeField field = fields.get(i);
    projects.add(rexBuilder.makeInputRef(field.getType(), i));
    addDistinctFieldName(field.getName());
    return this;
  }

  public ProjectBuilder add(int i, RelDataType expected) {
    RelDataTypeField field = fields.get(i);
    RexNode rexNode = rexBuilder.makeInputRef(field.getType(), i);
    if (!field.getType().equals(expected)) {
      rexNode = rexBuilder.makeCast(expected, rexNode, true);
    }
    projects.add(rexNode);
    addDistinctFieldName(field.getName());
    return this;
  }

  public ProjectBuilder addAll(List<Integer> indices) {
    for (int i : indices) {
      add(i);
    }
    return this;
  }

  public ProjectBuilder addAll(int startInclusive, int endExclusive) {
    for (int i = startInclusive; i < endExclusive; ++i) {
      add(i);
    }
    return this;
  }

  public ProjectBuilder addAll(int startInclusive, int endExclusive, List<RelDataType> expected) {
    assert expected.size() == endExclusive - startInclusive;
    int i = startInclusive;
    for (RelDataType type : expected) {
      add(i++, type);
    }
    return this;
  }

  public ProjectBuilder addAll() {
    return addAll(0, fields.size());
  }

  public ProjectBuilder addAllBut(int... skip) {
    for (int i = 0; i < fields.size(); ++i) {
      if (!Ints.contains(skip, i)) {
        add(i);
      }
    }
    return this;
  }

  public ProjectBuilder add(RexNode rexNode, String fieldName) {
    projects.add(rexNode);
    addDistinctFieldName(fieldName);
    return this;
  }

  public ProjectBuilder addConstMultiplicity(BigDecimal value) {
    RexLiteral literal = rexBuilder.makeBigintLiteral(value);
    return add(literal, TvrUtils.getMultiplicityName());
  }

  public ProjectBuilder addMultiplicity(int i) {
    RelDataTypeField field = fields.get(i);
    RelDataType type = field.getType();
    RexNode rexNode = rexBuilder.makeInputRef(type, i);
    if (type.isNullable()) {
      rexNode = rexBuilder.makeNotNull(rexNode);
    }
    assert rexNode.getType().equals(TvrUtils.getMultiplicityType(typeFactory, false));
    projects.add(rexNode);
    addDistinctFieldName(field.getName());
    return this;
  }

  public int getNumCols() {
    return projects.size();
  }

  public LogicalProject build() {
    return LogicalProject.create(input, projects, fieldNames);
  }
}
