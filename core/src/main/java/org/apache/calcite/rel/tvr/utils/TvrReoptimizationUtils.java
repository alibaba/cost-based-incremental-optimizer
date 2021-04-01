//package org.apache.calcite.rel.tvr.utils;
//
//import com.aliyun.odps.PartitionSpec;
//import com.aliyun.odps.compiler.meta.Partition;
//import com.aliyun.odps.compiler.meta.Table;
//import com.aliyun.odps.lot.cbo.OdpsRelContext;
//import com.aliyun.odps.lot.cbo.exec.ClusteredByDesc;
//import com.aliyun.odps.lot.cbo.expr.JaninoExecutor;
//import com.aliyun.odps.lot.cbo.expr.OdpsExecutorImpl;
//import com.aliyun.odps.lot.cbo.monitor.MetricsMonitor;
//import com.aliyun.odps.lot.cbo.profiling.PlanMetrics;
//import com.aliyun.odps.lot.cbo.profiling.PlanProfiling;
//import com.aliyun.odps.lot.cbo.rel.OdpsRelOptPartition;
//import com.aliyun.odps.lot.cbo.rel.OdpsRexCall;
//import com.aliyun.odps.lot.cbo.rel.RelOptPartition;
//import com.aliyun.odps.lot.cbo.rel.odps.OdpsRelOptUtils;
//import com.aliyun.odps.lot.cbo.rex.OdpsSimplifiedRexBuilder;
//import com.aliyun.odps.lot.cbo.type.OdpsRelOptTable;
//import com.aliyun.odps.lot.cbo.type.OdpsTable;
//import com.aliyun.odps.lot.cbo.type.ThreadCounter;
//import com.aliyun.odps.lot.cbo.type.TypeUtil;
//import com.aliyun.odps.optimizer.Constants;
//import com.google.common.collect.ImmutableList;
//import com.google.common.collect.ImmutableMap;
//import com.google.common.collect.Lists;
//import com.google.common.collect.Maps;
//import org.apache.calcite.linq4j.Ord;
//import org.apache.calcite.plan.RelOptCluster;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.rel.type.RelDataTypeField;
//import org.apache.calcite.rex.*;
//import org.apache.calcite.sql.type.SqlTypeName;
//import org.apache.calcite.util.ImmutableBitSet;
//import org.apache.calcite.util.Pair;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//
//import java.lang.reflect.InvocationTargetException;
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//
//public class TvrReoptimizationUtils {
//  private static final Log LOG = LogFactory.getLog(TvrReoptimizationUtils.class);
//
//  public static void updateOdpsRelOptTable(
//      OdpsRelOptTable odpsRelOptTable, OdpsRelContext ctx,
//      RelOptCluster cluster) {
//
//    if (!com.aliyun.odps.lot.cbo.utils.TvrUtils.progressiveMetaAvailable(ctx) || !com.aliyun.odps.lot.cbo.utils.TvrUtils.progressiveReoptimizeEnabled(ctx)) {
//      return;
//    }
//
//    OdpsTable odpsTable = odpsRelOptTable.getTable();
//    if (com.aliyun.odps.lot.cbo.utils.TvrUtils.isDimTable(ctx, odpsTable.getNormalizedName())) {
//      return;
//    }
//
//    Table newTable = com.aliyun.odps.lot.cbo.utils.TvrTableUtils.getTableFromMeta(odpsTable.getProject(), odpsTable.getName());
//    Table oldTable = odpsTable.getTableInternal();
//    if (newTable == null || newTable == oldTable) {
//      return;
//    }
//
//    Map<String, String> parameters = newTable.getParameters();
//    oldTable.getParameters().forEach((k, v) -> {
//      // keep the original parameters
//      if (!parameters.containsKey(k)) {
//        parameters.put(k, v);
//      }
//    });
//
//    OdpsTable newOdpsTable = odpsTable.copy(newTable);
//    List<RexNode> partitionPreds = odpsRelOptTable.getPartitionPreds();
//    if (newOdpsTable.isPartitioned()) {
//      List<Partition> parts;
//      if (partitionPreds != null && !partitionPreds.isEmpty()) {
//        List<Partition> partitions = new ArrayList<>();
//        for (RexNode rexNode : partitionPreds) {
//          partitions.addAll(getPartitionsByRange(cluster, rexNode, newOdpsTable,
//              odpsRelOptTable.getProjects(), odpsRelOptTable.getRowType()));
//        }
//
//        for (RexNode rexNode : partitionPreds) {
//          Pair<Boolean, List<Partition>> result =
//              getQualifiedPartitions((RexCall) rexNode, newOdpsTable,
//                  IntStream.range(0, newOdpsTable.getFieldNames().size())
//                      .boxed().collect(Collectors.toList()),
//                  newOdpsTable.getNormalizedName(), partitions,
//                  cluster.getRexBuilder(), cluster);
//          if (!result.getKey()) {
//            throw new RuntimeException("PPR calculation failed.");
//          }
//          partitions.clear();
//          partitions.addAll(result.right);
//        }
//        parts = partitions;
//      } else {
//        parts = newOdpsTable.getPartitions();
//      }
//
//      // Batch load the partitions
//      newOdpsTable.getPartitions(parts.stream().map(Partition::getPartitionSpec)
//          .collect(Collectors.toList()));
//
//      List<RelOptPartition> newParts = Lists.newArrayList();
//      parts.forEach(p -> newParts.add(
//          convertRelOptPartition(p, newOdpsTable, odpsRelOptTable.getCanonizer())));
//
//      odpsRelOptTable.updateTableAndPartitions(newOdpsTable, newParts);
//    } else {
//      odpsRelOptTable.updateTableAndPartitions(newOdpsTable, odpsRelOptTable.getQualifiedPartitions());
//    }
//  }
//
//  private static Pair<Boolean, List<Partition>> getQualifiedPartitions(
//      RexCall call,
//      OdpsTable odpsTable,
//      List<Integer> projects,
//      String tableName,
//      List<Partition> partitionList,
//      RexBuilder rexBuilder,
//      RelOptCluster cluster) {
//
//    if (partitionList == null || partitionList.size() <= 0) {
//      return Pair.of(true, partitionList);
//    }
//    MetricsMonitor monitor = OdpsRelContext.getInstance(cluster)
//        .getMonitor();
//    List<Partition> qualifiedPartitions = Lists.newArrayList();
//    try (PlanProfiling ignore =
//        PlanProfiling.latencyAndCount(PlanMetrics.PLAN_PARTITION_PRUNING_LATENCY,
//            PlanMetrics.PLAN_PARTITION_PRUNING_COUNT, LOG, monitor)) {
//      long startTimeMillis = System.currentTimeMillis();
//
//      boolean enableJaninoExecutor = OdpsRelContext.getInstance(cluster).getConfig()
//          .getBool(Constants.ENABLE_JANINO_EXECUTOR, false);
//
//      int index = 0, lastIndex = 0;
//      if (enableJaninoExecutor && partitionList.size() > 100) {
//        // Let code gen executor do the work if applicable.
//        List<RexInputRef> neededFields = new ArrayList<>();
//        JaninoExecutor je = JaninoExecutor.of(call, neededFields);
//        if (je != null) {
//          try {
//            LOG.info(String.format("Janino partition prune start, table %s, partition count %d",
//                tableName, partitionList.size()));
//            long lastTime = System.currentTimeMillis();
//            for (Partition p : partitionList) {
//              ++index;
//              if (index % 100 == 0) {
//                long now = System.currentTimeMillis();
//                if (now - lastTime > 1000 * 2) {
//                  LOG.info(String.format("Janino partition prune is running, table %s, filter %d in %d ms",
//                      tableName, index - lastIndex, now - lastTime));
//                  lastTime = now;
//                  lastIndex = index;
//                }
//              }
//              boolean r = (boolean) je.evaluate(resolve(p, neededFields, odpsTable, projects));
//              if (r) {
//                qualifiedPartitions.add(p);
//              }
//            }
//            long latency = System.currentTimeMillis() - startTimeMillis;
//            LOG.info(String.format("Janino partition prune OK, table %s, input %d, output %d, use %d ms",
//                tableName, partitionList.size(), qualifiedPartitions.size(), latency));
//
//            ThreadCounter.add(ThreadCounter.PATITION_PRUNE, latency);
//            OdpsTimer timer = OdpsRelContext.getInstance(cluster).getTimer();
//            if (timer != null) {
//              timer.incrementExcludedMillis(latency);
//            }
//            return Pair.of(true, qualifiedPartitions);
//          } catch (InvocationTargetException e) {
//            LOG.info(String.format("Janino partition fail, table %s, filter %d, err : %s",
//                tableName, index, e.getMessage()));
//          }
//        }
//      }
//
//      index = 0;
//      lastIndex = 0;
//
//      LOG.info(String.format("Partition prune start, table %s, partition count %d", tableName,
//          partitionList.size()));
//      // If code gen failed, evaluate by executor.
//      RexExecutor executor = cluster.getPlanner().getExecutor();
//
//      rexBuilder = new OdpsSimplifiedRexBuilder(rexBuilder.getTypeFactory());
//      PartitionGroupGenerator generator =
//          new PartitionGroupGenerator(
//              loadPartitionRef(call, odpsTable, projects), partitionList);
//
//      RexCallReplacer callReplacer = new RexCallReplacer();
//      call = (RexCall) call.accept(callReplacer);
//      long lastTime = System.currentTimeMillis();
//
//      while (generator.hasNext()) {
//        ++index;
//        if (index % 100 == 0) {
//          long now = System.currentTimeMillis();
//          if (now - lastTime > 1000 * 2) {
//            LOG.info(String.format("partition prune is running, table %s, filter %d in %d ms",
//                tableName, index - lastIndex, now - lastTime));
//            lastTime = now;
//            lastIndex = index;
//          }
//        }
//        PartitionGroup group = generator.next();
//
//        List<RexNode> result = Lists.newArrayList();
//        ((OdpsExecutorImpl) executor).reduce(rexBuilder, ImmutableList.of(call), group.predicate, result);
//        if (!RexUtil.isLiteral(result.get(0), false)) {
//          LOG.info("Reduce failed. rexNode: " + String.valueOf(call) + ", result: " + String.valueOf(result.get(0)));
//          return Pair.of(false, null);
//        }
//
//        if (isQualifiedConst(result.get(0))) {
//          if (LOG.isTraceEnabled()) {
//            LOG.trace("Get qualified partitions: " + group.partitions);
//          }
//          qualifiedPartitions.addAll(group.partitions);
//        }
//      }
//
//      long latency = System.currentTimeMillis() - startTimeMillis;
//      LOG.info(String.format("Partition prune OK, table %s, input %d, output %d, use %d ms",
//          tableName, partitionList.size(), qualifiedPartitions.size(), latency));
//      ThreadCounter.add(ThreadCounter.PATITION_PRUNE, latency);
//      OdpsTimer timer = OdpsRelContext.getInstance(cluster).getTimer();
//      if (timer != null) {
//        timer.incrementExcludedMillis(latency);
//      }
//      return Pair.of(true, qualifiedPartitions);
//    }
//  }
//
//  /**
//   * Group the partitions which have the same predicate.
//   */
//  static class PartitionGroupGenerator implements
//      Iterator<PartitionGroup> {
//
//    private final ImmutableMap<String, RexInputRef> partitionRefMap;
//    private final List<Partition> partitions;
//    private int currentIndex = 0;
//    private Map<Integer, Object> parameters;
//
//    PartitionGroupGenerator(ImmutableMap<String, RexInputRef> partitionRefMap,
//        List<Partition> partitions) {
//      this.partitionRefMap = partitionRefMap;
//      this.partitions = partitions;
//      this.parameters = Maps.newHashMapWithExpectedSize(partitionRefMap.size());
//    }
//
//    @Override public boolean hasNext() {
//      return currentIndex < partitions.size() && currentIndex >= 0;
//    }
//
//    @Override public PartitionGroup next() {
//      if (!hasNext()) {
//        return null;
//      }
//      updatePredicate();
//      ImmutableList.Builder<Partition> builder = ImmutableList.builder();
//      builder.add(getPartition());
//      nextPartition();
//      return new PartitionGroup(parameters, builder.build());
//    }
//
//    private void nextPartition() {
//      currentIndex += 1;
//    }
//
//    private Partition getPartition() {
//      return partitions.get(currentIndex);
//    }
//
//    private void updatePredicate() {
//      PartitionSpec spec = getPartition().getPartitionSpec();
//      for (String key : spec.keys()) {
//        if (partitionRefMap.containsKey(key)) {
//          RexInputRef inputRef = partitionRefMap.get(key);
//          parameters.put(
//              inputRef.getIndex(),
//              OdpsRexUtil.normalizePartValue(spec.get(key),
//                  inputRef.getType()));
//        }
//      }
//    }
//  }
//
//  private static ImmutableMap<String, RexInputRef> loadPartitionRef(RexCall call,
//      OdpsTable odpsTable, List<Integer> projects) {
//    ImmutableMap.Builder<String, RexInputRef> builder = ImmutableMap.builder();
//    for (RexInputRef partInput : OdpsRelOptUtils.findUniqueInputRef(call)) {
//      builder.put(
//          odpsTable.getFieldName(projects.get(partInput.getIndex())),
//          partInput);
//    }
//    return builder.build();
//  }
//
//  static class PartitionGroup {
//    public Map<Integer, Object> predicate;
//    public ImmutableList<Partition> partitions;
//
//    PartitionGroup(Map<Integer, Object> predicate,
//        ImmutableList<Partition> partitions) {
//      this.predicate = predicate;
//      this.partitions = partitions;
//    }
//  }
//
//  private static class RexCallReplacer extends RexShuttle {
//    @Override
//    public RexNode visitCall(RexCall call) {
//      call = (RexCall) super.visitCall(call);
//      if (call instanceof OdpsRexCall) {
//        return new OdpsExecutorImpl.ExecuteRexCall((OdpsRexCall) call);
//      } else {
//        // it is CAST added by the calcite
//        return call;
//      }
//    }
//  }
//
//
//  private static Object[] resolve(
//      Partition p,
//      List<RexInputRef> neededFields,
//      OdpsTable odpsTable,
//      List<Integer> projects) {
//    Object[] r = new Object[neededFields.size()];
//    for (int i = 0; i < neededFields.size(); i++) {
//      String fieldName = odpsTable
//          .getFieldName(projects.get(neededFields.get(i).getIndex()));
//      r[i] = p.getPartitionSpec().get(fieldName);
//    }
//    return r;
//  }
//
//
//  /**
//   * the partition could be abandoned in two cases:
//   * 1. constant result with null value (such as, pt = null)
//   * 2. constant result with 'false' value (such as, pt == '1' and pt == '2')
//   *
//   * @param expr
//   * @return
//   */
//  private static boolean isQualifiedConst(RexNode expr) {
//    if (expr == null) {
//      throw new UnsupportedOperationException("Unexpected literal: null");
//    } else if (expr.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
//      throw new UnsupportedOperationException("Unexpected literal type: " + expr.getType());
//    }
//
//    return !(RexLiteral.isNullLiteral(expr) || expr.isAlwaysFalse());
//  }
//
//  final static private Pair<String, String> EMPTY = Pair.of("~", "!");
//  private static List<Partition> getPartitionsByRange(RelOptCluster relOptCluster,
//      RexNode rexNode,
//      OdpsTable odpsTable,
//      List<Integer> projects,
//      RelDataType relDataType) {
//    Pair<String, String> minMax = getRangeByPredicate(relOptCluster, rexNode, odpsTable, projects, relDataType);
//    if (minMax == null) {
//      return odpsTable.getPartitions();
//    } else if (minMax.equals(EMPTY)) {
//      return ImmutableList.of();
//    }
//    return odpsTable.getPartitionsByRange(minMax.getKey(), minMax.getValue());
//  }
//
//  /**
//   * Get min and max values by analyze predicates about partition key
//   * @return
//   */
//  private static Pair<String, String> getRangeByPredicate(
//      RelOptCluster cluster,
//      RexNode rexNode,
//      OdpsTable odpsTable,
//      List<Integer> projects,
//      RelDataType relDataType) {
//    final int partStartIdx =
//        odpsTable.numColumns() -
//            odpsTable.numPartitionColumns();
//    //partition keys must be increasing by 1
//    List<Integer> partitionKeys = new ArrayList<>();
//    List<RelDataTypeField> dataTypeFields= relDataType.getFieldList();
//    int numOfNonPartitionColumn = 0;
//    //KeyInfo only be used for ordering, the key no is not used
//    List<ClusteredByDesc.KeyInfo> keyInfos = new ArrayList<>();
//
//    // Traverse the original column index in ascending order to get the
//    // predicates of prefix partition columns. NOTE that, 1)
//    // there is a strong convention that the original indexes of partition
//    // columns
//    // are the biggest ones in the table. 2) getRangeByPredicate only support
//    // the predicates of prefix partition columns and the predicates must in
//    // the ascending order of the partition column indexes.
//    int partitionKeyNo = partStartIdx;
//    for (int i :  ImmutableBitSet.of(projects)) {
//      if (i == partitionKeyNo) {
//        //Type is not String, then it don't support search using (min,max)
//        int indexInRowType = projects.indexOf(i);
//        if (!TypeUtil.isStringType(dataTypeFields.get(indexInRowType).getType())) {
//          break;
//        }
//        partitionKeys.add(indexInRowType);
//        keyInfos.add(new ClusteredByDesc.KeyInfo(indexInRowType, true));
//        ++partitionKeyNo;
//      } else {
//        if (i > partStartIdx) {
//          // its a partition column which is not successive with the traversed ones.
//          break;
//        }
//        ++numOfNonPartitionColumn;
//      }
//    }
//
//    if (partitionKeys.isEmpty()) {
//      return null;
//    }
//
//    // Add left partition keys
//    for (int j = partitionKeys.size(); j < odpsTable.numPartitionColumns(); ++j) {
//      keyInfos.add(new ClusteredByDesc.KeyInfo(-1, true));
//    }
//
//    List<ClusteredByDesc.KeyRange> keyRanges = OdpsRelOptUtils.convertPredicateToRanges(
//        cluster.getRexBuilder(), cluster.getPlanner().getExecutor(),
//        OdpsRelContext.getInstance(cluster).getConfig().getRexNodeComponentLimit(),
//        rexNode, keyInfos, 1);
//    //Not support 'or' like predicates (pt='20170617' or pt='20170618')
//    if (keyRanges == null || keyRanges.size() > 1) {
//      return null;
//    }
//
//    if (keyRanges.isEmpty()) {
//      return EMPTY;
//    }
//
//    return convertKeyRangeToMetaRange(keyRanges.get(0), relDataType, numOfNonPartitionColumn);
//  }
//
//  /**
//   * Convert KeyRange to Range for meta
//   * ! denotes min in OTS meta, but (space) is less than ! in ascii
//   * ~ denotes max
//   * (pt < 1) -> ('', 1), min is ''
//   * (pt <= 1) -> ('', 1~)
//   * (pt > 1) -> (1!, ~) -> (1(space), ~) -> (1, ~) like pt>=1
//   * (pt >= 1) -> (1, ~)
//   * @param keyRanges
//   * @return
//   */
//  private static Pair<String, String> convertKeyRangeToMetaRange(
//      ClusteredByDesc.KeyRange keyRanges,
//      RelDataType relDataType,
//      int numOfNonPartitionColumn) {
//    StringBuilder min = new StringBuilder();
//    StringBuilder max = new StringBuilder();
//
//    //convert min value
//    for (Ord<Object> ord: Ord.zip(keyRanges.getLower())) {
//      Object object = ord.e;
//
//      //pt = null
//      if (object == null) {
//        return null;
//      }
//
//      if (object.equals(ClusteredByDesc.GLOBAL_MIN)) {
//        //MIN denotes equal like pt >= '1', don't need !
//        if (ord.getKey() == 0) {
//          min.append("");
//        }
//        break;
//      } else if (object.equals(ClusteredByDesc.GLOBAL_MAX)) {
//        //MAX denotes equal like pt > '1'
//        // if pt > '1' then get partitions contains '1'
//        assert ord.getKey() > 0;
//        //          min.append(" ");
//        break;
//      } else {
//        RelDataTypeField relDataTypeField =
//            relDataType.getFieldList().get(numOfNonPartitionColumn + ord.i);
//        String ptName = relDataTypeField.getName();
//        if (TypeUtil.isStringType(relDataTypeField.getType())) {
//          if (ord.getKey() > 0) {
//            min.append("/");
//          }
//          min.append(ptName).append("=").append(object);
//        } else {
//          throw new RuntimeException("The type of partition column("
//              + ptName + ") is not string type");
//        }
//      }
//    }
//
//    boolean needAddMaxTilde = true;
//
//    //convert max value
//    for (Ord<Object> ord: Ord.zip(keyRanges.getUpper())) {
//      Object object = ord.e;
//      if (object == null) {
//        return null;
//      }
//      if (object.equals(ClusteredByDesc.GLOBAL_MAX)) {
//        //MAX denotes equal like pt <= '1'
//        //          max.append("~");
//        break;
//      } else if (object.equals(ClusteredByDesc.GLOBAL_MIN)) {
//        //MIN denotes equal like pt < '1'
//        if (ord.getKey() == 0) {
//          max.append("~");
//        }
//        needAddMaxTilde = false;
//        break;
//      } else {
//        RelDataTypeField relDataTypeField =
//            relDataType.getFieldList().get(numOfNonPartitionColumn + ord.i);
//        String ptName = relDataTypeField.getName();
//        if (TypeUtil.isStringType(relDataTypeField.getType())) {
//          if (ord.getKey() > 0) {
//            max.append("/");
//          }
//          max.append(ptName).append("=").append(object);
//        } else {
//          throw new RuntimeException("The type of partition column("
//              + ptName + ") is not string type");
//        }
//      }
//    }
//
//    if (needAddMaxTilde) {
//      max.append("~");
//    }
//
//    return Pair.of(min.toString(), max.toString());
//  }
//
//  public static RelOptPartition convertRelOptPartition(Partition p,
//      OdpsTable table, Canonizer<Integer> canonizer) {
//    return OdpsRelOptPartition
//        .create(table.getPartitionEntity(p), null,
//            null, null, null, canonizer);
//  }
//
//}
