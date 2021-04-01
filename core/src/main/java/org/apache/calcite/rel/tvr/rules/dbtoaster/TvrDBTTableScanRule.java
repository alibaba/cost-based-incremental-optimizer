package org.apache.calcite.rel.tvr.rules.dbtoaster;

import org.apache.calcite.plan.*;
import org.apache.calcite.plan.tvr.TvrSetSnapshot;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrMetaSetType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.tvr.rules.operators.TvrTableScanRule;
import org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil;
import org.apache.calcite.rel.tvr.trait.property.TvrUpdateOneTableProperty;
import org.apache.calcite.rel.tvr.utils.TimeInterval;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrTableUtils;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.tools.RelBuilder;

public class TvrDBTTableScanRule extends RelOptTvrRule
    implements InterTvrRule {

  public static TvrDBTTableScanRule INSTANCE = new TvrDBTTableScanRule();

  private TvrDBTTableScanRule() {
    super(operand(LogicalTableScan.class,
        r -> !TvrTableUtils.isDerivedTable(r)
            && TvrTableUtils.findTvrRelatedTableMapStr(r) == null,
        tvrEdgeSSMax(tvr()), none()));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalTableScan tableScan = getRoot(call).get();
    TvrMetaSet tvr = getRoot(call).tvr().get();
    TvrMetaSetType tvrType = tvr.getTvrType();
    RelOptCluster cluster = tableScan.getCluster();
    TvrContext ctx = TvrContext.getInstance(cluster);

    RelBuilder relBuilder = call.builder();
    RelOptTable relOptTable = tableScan.getTable();

    // Add property link for each changing tables
    for (String updateTable : ctx.allNonDimTablesSorted()) {
      int updateTableOrd = ctx.tableOrdinal(updateTable);
      TvrMetaSetType updateOneTableTvrType = TvrPropertyUtil
          .getDbtUpdateOneTableTvrType(updateTableOrd, tvrType);
      if (updateOneTableTvrType == TvrMetaSetType.DEFAULT) {
        continue;
      }

      RelOptRuleCall.TransformBuilder builder = call.transformBuilder();
      RelNode ts;
      if (TvrUtils.isDimTable(ctx, tableScan)) {
        ts = tableScan;
      } else {
        int ord = ctx.tableOrdinal(tableScan);
        TvrVersion[] snapshots = updateOneTableTvrType.getSnapshots();
        TvrVersion snapshot = snapshots[snapshots.length - 1];
        long scanTo = snapshot.get(ord);
        TimeInterval inv = TimeInterval.of(TvrVersion.MIN_TIME, scanTo);
//        if (ctx.getTvrDataArrivalInfos()
//            .containsKey(relOptTable.getTable().getName())) {
//          ts = TvrTableScanRule.createMockPartitionTableScan(relOptTable, inv,
//              new TvrSetSnapshot(updateOneTableTvrType.getSnapshots()[
//                  updateOneTableTvrType.getSnapshots().length - 1]), relBuilder,
//              cluster).left;
//        } else {
//          ts = TvrTableScanRule
//              .createTableScan(relOptTable, inv, relBuilder, cluster);
//        }
        ts = null;
      }
      builder.addPropertyLink(tvr, new TvrUpdateOneTableProperty(updateTableOrd,
              TvrUpdateOneTableProperty.PropertyType.DB_TOASTER), ts,
          updateOneTableTvrType);
      builder.transform();
    }
  }
}
