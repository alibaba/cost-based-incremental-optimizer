package org.apache.calcite.rel.tvr.rules.dbtoaster;

import org.apache.calcite.plan.InterTvrRule;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrMetaSetType;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil;
import org.apache.calcite.rel.tvr.trait.property.TvrUpdateOneTableProperty;
import org.apache.calcite.rel.tvr.utils.TvrContext;

public class TvrDBTValuesRule extends RelOptTvrRule
    implements InterTvrRule {

  public static TvrDBTValuesRule INSTANCE = new TvrDBTValuesRule();

  private TvrDBTValuesRule() {
    super(operand(Values.class, tvrEdgeSSMax(tvr()), none()));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Values values = getRoot(call).get();
    TvrMetaSet tvr = getRoot(call).tvr().get();
    TvrMetaSetType tvrType = tvr.getTvrType();

    RelOptCluster cluster = values.getCluster();
    TvrContext ctx = TvrContext.getInstance(cluster);

    // Add property link for each changing tables
    for (String updateTable : ctx.allNonDimTablesSorted()) {
      int tableOrd = ctx.tableOrdinal(updateTable);
      TvrMetaSetType updateOneTableTvrType =
          TvrPropertyUtil.getDbtUpdateOneTableTvrType(tableOrd, tvrType);
      if (updateOneTableTvrType == TvrMetaSetType.DEFAULT) {
        continue;
      }

      RelOptRuleCall.TransformBuilder builder = call.transformBuilder();
      builder.addPropertyLink(tvr, new TvrUpdateOneTableProperty(tableOrd,
              TvrUpdateOneTableProperty.PropertyType.DB_TOASTER), values,
          updateOneTableTvrType);
      builder.transform();
    }
  }
}
