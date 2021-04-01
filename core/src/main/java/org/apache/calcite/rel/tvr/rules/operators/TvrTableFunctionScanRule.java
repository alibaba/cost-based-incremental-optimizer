//package org.apache.calcite.rel.tvr.rules.operators;
//
//import com.google.common.collect.ImmutableList;
//import org.apache.calcite.plan.RelOptCluster;
//import org.apache.calcite.plan.RelOptRuleCall;
//import org.apache.calcite.plan.RelOptTvrRule;
//import org.apache.calcite.plan.volcano.RelSubset;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.logical.LogicalProject;
//import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
//import org.apache.calcite.rel.metadata.RelColumnMapping;
//import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.rel.type.RelDataTypeFactory;
//import org.apache.calcite.rel.type.RelDataTypeField;
//import org.apache.calcite.rex.RexBuilder;
//import org.apache.calcite.rex.RexCall;
//import org.apache.calcite.rex.RexNode;
//import org.apache.calcite.sql.SqlOperator;
//
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//import java.util.stream.IntStream;
//
//public class TvrTableFunctionScanRule extends RelOptTvrRule {
//
//  public static TvrTableFunctionScanRule INSTANCE =
//      new TvrTableFunctionScanRule();
//
//  private TvrTableFunctionScanRule() {
//    super(operand(LogicalTableFunctionScan.class, tvrEdgeSSMax(tvr()),
//        operand(RelSubset.class,
//            tvrEdgeSSMax(tvr(tvrEdge(TvrSetDelta.class, logicalSubset()))),
//            any())));
//  }
//
//  /**
//   *- <p>
//   *- Matches:
//   *- <p>
//   *-
//   *-  TableFunctionScan  --SET_SNAPSHOT_MAX-- Tvr0
//   *-        |                        TvrSetSemantics
//   *-      input       --SET_SNAPSHOT_MAX-- Tvr1 -------------------   input
//   *-
//   *- <p>
//   *- Converts to:
//   *- <p>
//   *-
//   *-                                  TvrSetSemantics
//   *-  TableFunctionScan --SET_SNAPSHOT_MAX-- Tvr0 ----------------- newTableFunctionScan
//   *-        |                       TvrSetSemantics        |
//   *-      input       --SET_SNAPSHOT_MAX-- Tvr1 -----------------     input
//   *-
//   *- <p>
//   */
//
//  @Override
//  public void onMatch(RelOptRuleCall call) {
//    RelMatch tableFunctionScanAny = getRoot(call);
//    LogicalTableFunctionScan tableFunctionScan = tableFunctionScanAny.get();
//    RelOfTvr input = tableFunctionScanAny.input(0).tvrSibling();
//    TvrSetDelta inputTrait = (TvrSetDelta) input.tvrSemantics;
//
//    RelOptCluster cluster = tableFunctionScan.getCluster();
//
//    RelNode newNode;
//    RelDataType outputType;
//    Set<RelColumnMapping> mappings =
//        new HashSet<>(tableFunctionScan.getColumnMappings());
//    if (!inputTrait.isPositiveOnly()) {
//      RelDataTypeFactory typeFactory = cluster.getTypeFactory();
//      RelDataTypeFactory.Builder typeBuilder =
//          new RelDataTypeFactory.Builder(typeFactory);
//      List<RelDataTypeField> newFields = new ArrayList<>();
//
//      // add the fields of the input node and add multiplicity to RelColumnMapping
//      List<RelDataTypeField> fields =
//          tableFunctionScan.getRowType().getFieldList();
//      int mappingSize = mappings.size();
//      IntStream.range(0, mappingSize)
//          .forEach(i -> newFields.add(fields.get(i)));
//
//      int indexOfMultiplicity = inputTrait
//          .getIndexOfMultiplicity((RelNode) input.rel.get());
//      List<RelDataTypeField> inputFields =
//          input.rel.get().getRowType().getFieldList();
//      newFields.add(inputFields.get(indexOfMultiplicity));
//      RelColumnMapping multiplicity =
//          new RelColumnMapping(mappingSize, 0, indexOfMultiplicity, false);
//      mappings.add(multiplicity);
//
//      // add multiplicity to OdpsSqlFunction.WrapApplySqlFunction to rebuild the rex call
//      RexNode newRexCall =
//          rebuildRexCall((RexCall) tableFunctionScan.getCall(), newFields,
//              typeFactory, cluster);
//
//      // add the new fields created by tableFunctionScan to build the output fields
//      IntStream.range(mappingSize, fields.size())
//          .forEach(i -> newFields.add(fields.get(i)));
//      outputType = typeBuilder.addAll(newFields).build();
//
//      LogicalTableFunctionScan newTableFunctionScan =
//              LogicalTableFunctionScan
//              .create(cluster, ImmutableList.of(input.rel.get()), newRexCall,
//                  tableFunctionScan.getElementType(), outputType, mappings);
//
//      // add project to relocate multiplicity
//      RexBuilder rexBuilder = cluster.getRexBuilder();
//      List<RexNode> projects = new ArrayList<>();
//      List<String> names = new ArrayList<>();
//
//      IntStream.range(0, newFields.size()).filter(i -> i != mappingSize)
//          .forEach(i -> {
//            projects.add(rexBuilder.makeInputRef(newTableFunctionScan, i));
//            names.add(newFields.get(i).getName());
//          });
//      projects.add(rexBuilder.makeInputRef(newTableFunctionScan, mappingSize));
//      names.add(newFields.get(mappingSize).getName());
//
//      newNode = LogicalProject.create(newTableFunctionScan, projects, names);
//    } else {
//      outputType = tableFunctionScan.getRowType();
//      newNode = LogicalTableFunctionScan
//          .create(cluster, ImmutableList.of(input.rel.get()), tableFunctionScan.getCall(),
//              tableFunctionScan.getElementType(), outputType, mappings);
//    }
//
//    transformToRootTvr(call, newNode, inputTrait);
//  }
//
//  private RexNode rebuildRexCall(RexCall rexCall,
//      List<RelDataTypeField> inputFields, RelDataTypeFactory typeFactory,
//      RelOptCluster cluster) {
//    assert rexCall.op instanceof OdpsSqlFunction.WrapApplySqlFunction;
//
//    RelDataTypeFactory.Builder builder =
//        new RelDataTypeFactory.Builder(typeFactory);
//    builder.addAll(inputFields);
//    OdpsSqlFunction.WrapApplySqlFunction func =
//        (OdpsSqlFunction.WrapApplySqlFunction) rexCall.op;
//
//    Iterable<RexNode> calls = func.getFunctions();
//    calls.forEach(rexNode -> builder.addAll(rexNode.getType().getFieldList()));
//    SqlOperator wrapFunction = new OdpsSqlFunction.WrapApplySqlFunction(calls);
//
//    return cluster.getRexBuilder()
//        .makeCall(builder.build(), wrapFunction, ImmutableList.of());
//  }
//}
