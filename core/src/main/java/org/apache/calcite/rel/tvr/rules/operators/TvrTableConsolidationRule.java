//package org.apache.calcite.rel.tvr.rules.operators;
//
//import org.apache.calcite.plan.RelOptRule;
//import org.apache.calcite.plan.RelOptRuleCall;
//import org.apache.calcite.plan.RelOptTable;
//import org.apache.calcite.rel.core.TableScan;
//import org.apache.calcite.rel.tvr.utils.TvrContext;
//import org.apache.calcite.rel.tvr.utils.TvrUtils;
//
///**
// * Apply the time interval of each partition table to build the real qualified partitions.
// * NOTE: this rule is just enabled in range query optimization.
// *
// * For example,
// *    Table A: qualified partitions = [20200901, 20200902, 20200903]
// *             time interval = [MIN, 1598976000000 (which is the timestamp of "20200902")]
// *    After Consolidation:
// *    Table A: qualified partitions = [20200901, 20200902]
// *             time interval = [MIN, MAX]
// */
//public class TvrTableConsolidationRule extends RelOptRule {
//  public static final TvrTableConsolidationRule INSTANCE = new TvrTableConsolidationRule();
//
//  private TvrTableConsolidationRule() {
//    super(operand(TableScan.class, any()), "TvrTableConsolidationRule");
//  }
//
//  @Override
//  public boolean matches(RelOptRuleCall call) {
//    TableScan scan = call.rel(0);
//
//    TvrContext ctx = TvrContext.getInstance(scan.getCluster());
//    if (!TvrUtils.progressiveRangeQueryOptEnabled(ctx)) {
//      // NOTE: this rule is enabled just in range query optimization.
//      return false;
//    }
//
//    RelOptTable odpsRelOptTable = (RelOptTable) scan.getTable();
//    return odpsRelOptTable.getTable().isPartitioned();
//  }
//
//  @Override
//  public void onMatch(RelOptRuleCall call) {
//    TableScan scan = call.rel(0);
//
//    OdpsRelOptTable odpsRelOptTable = (OdpsRelOptTable) scan.getTable();
//    odpsRelOptTable.consolidate();
//    call.transformTo(scan);
//  }
//}
