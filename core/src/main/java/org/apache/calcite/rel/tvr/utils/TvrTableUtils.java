package org.apache.calcite.rel.tvr.utils;

import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.MatType;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.TableSink;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.temp.PhysicalTableSink;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.io.Serializable;
import java.util.List;

public class TvrTableUtils {

  // Indicate the estimated row count of the derived tvr table.
  // For example, a query may tell its downstream queries that it will
  // produce some intermediate states (which are recorded in the TvrTableRelation) in the future,
  // and it can estimate the row counts of these states to the downstream queries by this flag.
  public static final String TVR_ESTIMATED_ROW_COUNT=
          "progressive.tvr.estimated.row.count";

  // Indicate the plan that creates this table.
  public static final String PROGRESSIVE_TABLE_DIGEST =
          "optimizer.progressive.table.digest";

  // Indicate the life cycle of the tvr derived table.
  public static final String PROGRESSIVE_TABLE_LIFECYCLE =
          "optimizer.progressive.table.lifecycle";

  public static final ThreadLocal<Integer> LIFECYCLE = new ThreadLocal<>();
  private static final int DEFAULT_LIFECYCLE = 3;

  private static final ImmutableSet CLUSTER_TABLE_DISTRIBUTION_TYPES = ImmutableSet
          .of(RelDistribution.Type.HASH_DISTRIBUTED,
                  RelDistribution.Type.RANGE_DISTRIBUTED);

  public static String escapeTableName(String tableName) {
    return tableName.replaceAll("[^0-9a-zA-Z_]", "_").toLowerCase();
  }

  public static String getProjectName(TvrContext ctx) {
    return "_tvr_internal_mat";
  }

  public static Integer getLifecycle() {
    Integer lifecycle = LIFECYCLE.get();
    if (lifecycle == null) {
      lifecycle = DEFAULT_LIFECYCLE;
      LIFECYCLE.set(lifecycle);
    }
    return lifecycle;
  }

  public static String getStateTableName(TvrContext ctx, int id, MatType matType) {
    return getStateTableName(ctx, id, ((matType == null) ? "" : matType.toString().toLowerCase()));
  }

  public static String getStateTableName(TvrContext ctx, int id, String suffix) {
    return TvrTableUtils.escapeTableName(
            "progrstate_" + "_"
                    + id + "_" + suffix);
  }

  public static String getStatsTableName(TvrContext ctx, int id) {
    return TvrTableUtils.escapeTableName("_relset_id_" + id);
  }

  public static String replaceReferenceName(String name) {
    if (name.startsWith("$")) {
      name = name.replaceFirst("\\$", "undetermined_");
    }
    return name;
  }

  public static boolean isClusterTable(RelTraitSet traitSet) {
    return CLUSTER_TABLE_DISTRIBUTION_TYPES
            .contains(traitSet.getTrait(RelDistributionTraitDef.INSTANCE).getType())
            || !traitSet.getTrait(RelCollationTraitDef.INSTANCE)
            .getFieldCollations().isEmpty();
  }

  public static boolean tableMetaReusable(RelTraitSet dst, RelTraitSet src) {
    // TODO: for now, hard linking two clustered tables requires their
    //  distribution/collations to be EXACTLY the same
    RelDistribution dstDist = dst.getTrait(RelDistributionTraitDef.INSTANCE);
    RelDistribution srcDist = src.getTrait(RelDistributionTraitDef.INSTANCE);
    if (dstDist != null && srcDist != null) {
      if (! srcDist.equals(dstDist)) {
        return false;
      }
    }

    RelCollation dstColl = dst.getTrait(RelCollationTraitDef.INSTANCE);
    RelCollation srcColl = dst.getTrait(RelCollationTraitDef.INSTANCE);

    if (dstColl != null && srcColl != null) {
      if (!dstColl.getFieldCollations().isEmpty()) {
        return dstColl.equals(srcColl);
      }
    }

    return true;
  }

  public static class TvrTableRelation implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final String TVR_RELATED_TABLE_MAP = "progressive.tvr.related.table.map";
    public static final String CONFIG_INPUT_TVR_RELATED_TABLE_MAPS = "progressive.tvr.input.related.table.maps";
    public static final String CONFIG_OUTPUT_TVR_RELATED_TABLE_MAPS = "progressive.tvr.output.related.table.maps";
    public String projectName;
    public String tableName;
    public String tableRowType;
    public Integer partitionNum;
    public String relDistribution;
    public String relCollation;
    public Double estimatedRowCount;

    public TvrTableRelation(String projectName, String tableName, String tableRowType, Integer partitionNum, String relDistribution, String relCollation, Double estimatedRowCount) {
      this.projectName = projectName;
      this.tableName = tableName;
      this.estimatedRowCount = estimatedRowCount;
      this.relDistribution = relDistribution;
      this.relCollation = relCollation;
      this.tableRowType = tableRowType;
      this.partitionNum = partitionNum;
    }
  }

  public static class TvrTableRelationUtil {

    public static TvrTableRelation of(RelDataTypeFactory typeFactory,
                                      String projectName, String tableName, RelDataType tableRowType,
                                      Integer partitionNum, RelDistribution distribution,
                                      RelCollation collation, Double estimatedRowCount) {

      Gson gson = TvrJsonUtils.createTvrGson(tableRowType, typeFactory);
      String tableTypeStr = gson.toJson(tableRowType, RelDataType.class);

      String distributionStr;
      if (distribution instanceof RelDistribution) {
        distributionStr =
                gson.toJson(distribution, RelDistribution.class);
      } else if (distribution == RelDistributions.ANY) {
        distributionStr = "ANY";
      } else if (distribution == RelDistributions.SINGLETON) {
        distributionStr = "SINGLE";
      } else {
        throw new RuntimeException("cannot serialize " + distribution);
      }

      String collationStr;
      if (collation instanceof RelCollationImpl) {
        collationStr = gson.toJson(collation, RelCollationImpl.class);
      } else if (collation == RelCollations.EMPTY) {
        collationStr = "EMPTY";
      } else {
        throw new RuntimeException("cannot serialize " + collation);
      }

      return new TvrTableRelation(projectName, tableName, tableTypeStr,
              partitionNum, distributionStr, collationStr, estimatedRowCount);
    }

    public static TvrTableRelation of(TableSink tableSink) {
      return null;
    }

    public static RelDistribution getRelDistribution(RelDataTypeFactory typeFactory,
                                                     RelDataType rowType, String relDistribution) {
      if (relDistribution == null) {
        return null;
      }
      switch (relDistribution) {
        case "ANY":
          return RelDistributions.ANY;
        case "SINGLE":
          return RelDistributions.SINGLETON;
      }
      Gson gson = TvrJsonUtils.createTvrGson(rowType, typeFactory);
      return gson.fromJson(relDistribution, RelDistribution.class);
    }

    public static RelCollation getRelCollation(RelDataTypeFactory typeFactory,
                                               RelDataType rowType, String relCollation) {
      if (relCollation == null) {
        return null;
      }
      if (relCollation.equals("EMPTY")) {
        return RelCollations.EMPTY;
      }
      Gson gson = TvrJsonUtils.createTvrGson(rowType, typeFactory);
      return gson.fromJson(relCollation, RelCollationImpl.class);
    }

  }

  // TODO[TVR]: this is a hack assuming that all progressive iterations happen in the same day
  private static double getRatioOfDay(TimeInterval inv) {
    long startHour = (inv.from == Long.MIN_VALUE) ? 0 : inv.from % 86400000L;
    long endHour = (inv.to == Long.MAX_VALUE) ? 86400000L : inv.to % 86400000L;
    return ((double) (endHour - startHour)) / 86400000L;
  }


  public static String findTvrRelatedTableMapStr(
          LogicalTableScan tableScan) {
    return null;
  }

  public static void markAsDerivedTable(RelOptTable table) {
    throw new UnsupportedOperationException("do not support mark derived table");
  }

  public static boolean isDerivedTable(TableScan tableScan) {
    return isDerivedTable(tableScan.getTable());
  }

  public static boolean isDerivedTable(RelOptTable table) {
    return false;
  }

  public static boolean isMemoryTable(PhysicalTableSink sink) {
    return false;
  }

  public static class UnionSourceTableInfo {
    private final String projectName;
    private final String tableName;
    private final List<String> partitionSpecs;

    public UnionSourceTableInfo(
            String projectName,
            String tableName,
            List<String> partitionSpecs) {

      this.projectName = projectName;
      this.tableName = tableName;
      this.partitionSpecs = partitionSpecs;
    }

    public String getProjectName() {
      return projectName;
    }

    public String getTableName() {
      return tableName;
    }

    public List<String> getPartitionSpecs() {
      return partitionSpecs;
    }
  }

  public static List<UnionSourceTableInfo> createHardLinkInfos(
          List<PhysicalTableSink> sinks) {
    throw new UnsupportedOperationException("hard link is not supported");
  }

  public static String getNormalizedTableName(RelNode rel) {
    return rel.getTable().getQualifiedName().toString();
  }

  public static void saveTableVersion(
  ) {
    throw new UnsupportedOperationException("save table version not supported");
  }

  public static Double getEstimatedRowCount(RelOptTable table) {
    return null;
  }
}
