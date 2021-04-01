package org.apache.calcite.rel.tvr.trait.property;

import com.google.common.base.Verify;
import com.google.gson.Gson;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.trait.TvrDbtUpdateOneMetaSetType;
import org.apache.calcite.rel.tvr.trait.TvrDefaultMetaSetType;
import org.apache.calcite.rel.tvr.trait.TvrUpdateOneMetaSetType;
import org.apache.calcite.rel.tvr.utils.TvrJsonUtils;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import java.util.Map;

public class TvrPropertyUtil {

  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  // Disable constructor
  private TvrPropertyUtil() {
  }

  public static Gson GSON = TvrJsonUtils.createTvrGson(null, null);

  public static String toString(TvrProperty property) {
    return GSON.toJson(property, TvrProperty.class);
  }

  public static TvrProperty fromString(String propertyStr) {
    return GSON.fromJson(propertyStr, TvrProperty.class);
  }

  public static TvrMetaSetType getDbtUpdateOneTableTvrType(int tableOrd,
      TvrMetaSetType tvrType) {
    if (tvrType instanceof TvrDefaultMetaSetType) {
      return TvrDbtUpdateOneMetaSetType
          .create((TvrDefaultMetaSetType) tvrType, tableOrd);
    } else if (tvrType instanceof TvrDbtUpdateOneMetaSetType) {
      if (((TvrDbtUpdateOneMetaSetType) tvrType).getTableOrd() == tableOrd) {
        return tvrType;
      } else {
        return TvrMetaSetType.DEFAULT;
      }
    } else if (tvrType == TvrMetaSetType.DEFAULT) {
      return tvrType;
    } else if (tvrType instanceof TvrUpdateOneMetaSetType) {
      return TvrDbtUpdateOneMetaSetType
          .create(((TvrUpdateOneMetaSetType) tvrType).getBaseType(), tableOrd);
    } else {
      throw new UnsupportedOperationException("unknown tvr type " + tvrType);
    }
  }

  /**
   * Produce a tvrType for only updating one table out of all tables.
   * The series with three tables and three versions for updating table ord 1:
   * 112 \
   * 122 /
   * 223 \
   * 233 /
   */
  public static TvrMetaSetType getUpdateOneTableTvrType(int tableOrd,
      TvrMetaSetType tvrType) {
    Verify.verify(tvrType instanceof TvrDefaultMetaSetType);
    int numTables = ((TvrDefaultMetaSetType) tvrType).getVersionDim();
    assert numTables > 0;

    // If only one table, degrade to default tvrType
    if (numTables == 1) {
      return tvrType;
    }

    // Same set of time points as the default tvrType
    return TvrUpdateOneMetaSetType
        .create((TvrDefaultMetaSetType) tvrType, tableOrd);
  }

  public static Map<TvrUpdateOneTableProperty, TvrMetaSet> updateOneTableProperties(
      TvrMetaSet tvr, TvrUpdateOneTableProperty.PropertyType type) {
    return tvr.getTvrPropertyLinks(p -> p instanceof TvrUpdateOneTableProperty
        && ((TvrUpdateOneTableProperty) p).getType().equals(type));
  }

  public static boolean checkExisitingOJVProperty(RelOptRuleCall call,
      TvrMetaSet rootTvr, RelNode relNode,
      TvrOuterJoinViewProperty newOjvProperty) {

    Map<TvrOuterJoinViewProperty, TvrMetaSet> existing =
        rootTvr.getTvrPropertyLinks(TvrOuterJoinViewProperty.class);
    VolcanoPlanner planner = (VolcanoPlanner)call.getPlanner();
    RelSet set = planner.getSet(relNode);
    if (existing.isEmpty()) {
      LOGGER.debug("Connect TvrOuterJoinViewProperty at set {} because of {}",
          set.getId(), relNode);
    } else {
      TvrOuterJoinViewProperty e = existing.keySet().iterator().next();
      if (e.equals(newOjvProperty)) {
        LOGGER.debug("TvrOuterJoinViewProperty matched at set {} because of {}",
            set.getId(), relNode);
      } else {
        LOGGER.error(
            "TvrOuterJoinViewProperty mismatch! \nexisting: {}\nnew:      {}",
            e.toFullString(), newOjvProperty.toFullString());
      }
    }
    return true;
  }
}
