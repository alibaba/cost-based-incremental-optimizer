package org.apache.calcite.rel.tvr.utils;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.MatType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.temp.PhysicalTableSink;
import org.apache.calcite.rel.temp.PhysicalTableSpool;

import java.util.List;
import java.util.Map;

import static org.apache.calcite.plan.volcano.MatType.MAT_DISK;
import static org.apache.calcite.plan.volcano.MatType.MAT_MEMORY;

public class TvrSinkUtils {

  public static RelNode buildMatSink(RelNode rel, MatType matType,
      RelTraitSet traitSet, String projectName, String tableName, double estimatedRowCount,
      Map<String, String> parameters) {
    assert matType == MAT_MEMORY || matType == MAT_DISK;

    return new PhysicalTableSpool(rel.getTraitSet(), rel, projectName, tableName);

//    // Tag the type of table: memory table or disk table
//    // By default, this table is regarded as a state table, and its bucket number can be adjusted dynamically.
//    // Eventually these flags will be updated to the values ​​
//    // that really meet the requirements (see ExecPlanManager.propagateTableFlags).
//
//    // if range query optimization is enabled, there is no table will be marked as a state table,
//    // because these intermediate states may be reused for a long time.
//    boolean isStateTable = !TvrUtils.progressiveRangeQueryOptEnabled(
//        OdpsRelContext.getInstance(rel.getCluster()));
//
//    Map<String, String> tableFlags = com.aliyun.odps.lot.cbo.utils.TvrTableUtils.createTableFlags(isStateTable,
//        matType == MAT_MEMORY, com.aliyun.odps.lot.cbo.utils.TvrTableUtils.isClusterTable(traitSet));
//    tableFlags.put(com.aliyun.odps.lot.cbo.utils.TvrTableUtils.TVR_ESTIMATED_ROW_COUNT, String.valueOf(estimatedRowCount));
//    tableFlags.put(com.aliyun.odps.lot.cbo.utils.TvrTableUtils.TVR_RELATED_DERIVED_TABLE, "true");
//    tableFlags.putAll(parameters);
//
//    return TvrSinkUtils.buildTableSink(rel, projectName, tableName, traitSet, tableFlags,
//        true);
  }

//  public static RelNode buildTableSink(RelNode matNode, String projectName,
//      String tableName, RelTraitSet traitSet, Map<String, String> parameters, boolean isPhysical) {
//    // create state table
//    OdpsRelContext ctx = OdpsRelContext.getInstance(matNode.getCluster());
//    OdpsRelOptTable newRelOptTable = (OdpsRelOptTable) com.aliyun.odps.lot.cbo.utils.TvrTableUtils
//        .createNewRelOptTable(ctx,
//            (OdpsCatalogReader) ctx.getOdpsRelBuilder()
//                .getRelOptSchema(), projectName, tableName,
//            traitSet, matNode.getRowType(), 0, parameters);
//
//    List<RelDataType> fieldTypes = matNode.getRowType().getFieldList().stream()
//        .map(RelDataTypeField::getType).collect(Collectors.toList());
//    List<RexNode> projects = IntStream.range(0, fieldTypes.size())
//        .mapToObj(i -> new RexInputRef(i, fieldTypes.get(i))).collect(Collectors.toList());
//
//    RelNode sink;
//    if (isPhysical) {
//      sink = OdpsPhysicalTableSink
//          .create(matNode, matNode.getTraitSet(), newRelOptTable, null, null,
//              true, projects);
//    } else {
//      sink = LogicalTableSink.create(matNode, newRelOptTable, null, null,
//          true, projects);
//    }
//    sink = sink.copy(traitSet, sink.getInputs());
//    return sink;
//  }
//
  public static PhysicalTableSink buildSinkForDownStream(RelNode input,
                                                         PhysicalTableSink relatedSink, MatType matType, RelTraitSet traitSet,
                                                         String projectName, String tableName, double estimatedRowCount) {

    throw new UnsupportedOperationException("buildSinkForDownStream not supported");

//    Map<String, String> parameters =
//        createTableFlags(relatedSink, matType, isClusterTable(traitSet));
//    parameters.put(com.aliyun.odps.lot.cbo.utils.TvrTableUtils.TVR_ESTIMATED_ROW_COUNT, String.valueOf(estimatedRowCount));
//    parameters.put(com.aliyun.odps.lot.cbo.utils.TvrTableUtils.TVR_RELATED_DERIVED_TABLE, "true");
//
//    Map<String, RexLiteral> staticPartitions = getStaticPartitions(relatedSink);
//    return buildPartitionedTableSink(input, projectName, tableName, traitSet,
//        staticPartitions, parameters);
  }
//
//  private static OdpsPhysicalTableSink buildPartitionedTableSink(RelNode input,
//      String projectName, String tableName, RelTraitSet traitSet, Map<String, RexLiteral> staticPartitions,
//      Map<String, String> parameters) {
//    if (staticPartitions == null || staticPartitions.isEmpty()) {
//      return (OdpsPhysicalTableSink) buildTableSink(input, projectName, tableName,
//          traitSet, parameters, true);
//    }
//
//    assert input != null;
//    RelOptCluster cluster = input.getCluster();
//    RelDataTypeFactory typeFactory = cluster.getTypeFactory();
//    List<RelDataTypeField> fields = input.getRowType().getFieldList();
//    List<RelDataType> fieldTypes =
//        fields.stream().map(RelDataTypeField::getType)
//            .collect(Collectors.toList());
//    List<String> colNames = fields.stream().map(RelDataTypeField::getName)
//        .collect(Collectors.toList());
//
//    // add partition cols
//    staticPartitions.forEach((k, v) -> {
//      fieldTypes.add(v.getType());
//      colNames.add(k);
//    });
//    RelDataType newRelDataType = typeFactory.createStructType(fieldTypes, colNames);
//
//    // create new table
//    OdpsRelContext ctx = OdpsRelContext.getInstance(cluster);
//    OdpsRelOptTable table = (OdpsRelOptTable) com.aliyun.odps.lot.cbo.utils.TvrTableUtils
//        .createNewRelOptTable(ctx,
//            (OdpsCatalogReader) ctx.getOdpsRelBuilder().getRelOptSchema(),
//            projectName, tableName, traitSet, newRelDataType,
//            staticPartitions.size(), parameters);
//
//    // create partitions
//    OdpsTable odpsTable = table.getTable();
//    OdpsPartition odpsPartition = OdpsPartition
//        .create(odpsTable.getProperties(), null, odpsTable.getStorageDescriptor());
//    OdpsRelOptPartition newOdpsPartition =
//        (OdpsRelOptPartition) OdpsRelOptPartition
//            .create(odpsPartition, staticPartitions, Collections.emptyMap(),
//                null, null, table.getCanonizer());
//
//    // create new sink
//    RexBuilder rexBuilder = cluster.getRexBuilder();
//    List<RexNode> projects = IntStream.range(0, fields.size())
//        .mapToObj(i -> rexBuilder.makeInputRef(fields.get(i).getType(), i))
//        .collect(Collectors.toList());
//    RelNode sink = OdpsPhysicalTableSink
//        .create(input, input.getTraitSet(), table,
//            newOdpsPartition, null, true, projects);
//    sink = sink.copy(traitSet, sink.getInputs());
//    return (OdpsPhysicalTableSink) sink;
//  }
//
//  private static Map<String, RexLiteral> getStaticPartitions(TableSink sink) {
//    RelOptPartition partition = sink.getPartition();
//    if (partition == null) {
//      return null;
//    }
//
//    return partition.getStaticPartitionSpec();
//  }
//
  public static void addHardLinks(
          PhysicalTableSink sink, List<PhysicalTableSink> linkedSinks) {
      throw new UnsupportedOperationException("addHardLink not supported");
//    List<com.aliyun.odps.lot.cbo.utils.TvrTableUtils.UnionSourceTableInfo> hardLinkInfos =
//        com.aliyun.odps.lot.cbo.utils.TvrTableUtils.createHardLinkInfos(linkedSinks);
//    Map<String, String> tableProperties =
//        ((OdpsRelOptTable) sink.getTable()).getTable().getProperties();
//    tableProperties.put(ODPS_TABLESINK_UNION, new Gson().toJson(hardLinkInfos));
  }

}
