package org.apache.calcite.plan.volcano;


import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableUnion;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.temp.PhysicalTableSink;
import org.apache.calcite.rel.tvr.rels.EnumerablePhysicalVirtualRoot;
import org.apache.calcite.rel.tvr.rels.TvrExecPlansOp;
import org.apache.calcite.rel.tvr.utils.TvrSinkUtils;
import org.apache.calcite.util.IdentityLinkedHashMap;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExecPlanManager {
  // assume all the elements in these collections are in order of execution time.
  private final ImmutableList<Set<RelNode>> plans;
  // <target sink, src sinks>
  // The value are the list of the original linked tables.
  // For example, table A and table B are linked to table C,
  //              table C and table D are linked to table E.
  //              For table E, it is equivalent to linking table A, B and D to E.
  private final Map<PhysicalTableSink, List<PhysicalTableSink>> links;

  // indicate whether the re-optimization is enabled.
  private final boolean reoptimize;
  // minimum execution time, the execution time of all the plans can not be less than this value.
  private final int minTime;
  // maximum execution time, the execution time of all the plans can not be greater than this value.
  private final int lastTime;

  // indicate whether to disable hardlink plans.
  // Multi-insert queries may violate an assumption that each table can only
  // be linked three times at most in Pangu file system.
  // For example:
  //      Sink A   B     C   D
  //            \   \    /  /    =>  link E to A,B,C,D
  //                  E
  private final boolean isMultiInsert;

  private final RelOptCluster cluster;

  ExecPlanManager(int minTime, int lastTime, boolean reoptimize,
                  boolean isMultiInsert, RelOptCluster cluster) {
    this.minTime = minTime;
    this.lastTime = lastTime;
    this.reoptimize = reoptimize;
    this.cluster = cluster;
    this.isMultiInsert = isMultiInsert;

    int arrSize = lastTime + 1 - minTime;
    this.plans = ImmutableList.copyOf(Stream.generate(
        () -> Collections.<RelNode>newSetFromMap(new IdentityLinkedHashMap<>()))
        .limit(arrSize).iterator());
    this.links = new IdentityLinkedHashMap<>();
  }

  void addPlan(int time, RelNode plan) {
    if (time < minTime) {
      return;
    }
    plans.get(time - minTime).add(plan);
  }

  void addHardLinkPlan(int time, PhysicalTableSink targetSink, Set<PhysicalTableSink> srcSinks) {
    List<PhysicalTableSink> linkedSinks =
        srcSinks.stream().flatMap(sink -> getLinkedSrcSinks(sink).stream())
            .collect(Collectors.toList());
    links.put(targetSink, linkedSinks);
    if (isMultiInsert) {
      // if the target sink has the same execution time with a linked sink,
      // it is better to use the plan of the linked sink directly rather than reusing the table scan.
      // NOTE: the execution time of a source sink must be less than or equal to the target sink.
      Set<RelNode> currentPlans = plans.get(Math.max(time - minTime, 0));
      List<RelNode> inputs = linkedSinks.stream()
          .map(r -> currentPlans.contains(r) ? r.getInput() : r)
          .collect(Collectors.toList());
      RelNode input = inputs.size() == 1 ?
          inputs.get(0) :
          new EnumerableUnion(inputs.get(0).getCluster(), inputs.get(0).getTraitSet(), inputs, true);
      targetSink = (PhysicalTableSink) targetSink
          .copy(targetSink.getTraitSet(), ImmutableList.of(input));
    } else {
      TvrSinkUtils.addHardLinks(targetSink, linkedSinks);
    }
    addPlan(time, targetSink);
  }

  void addSoftLinkPlan(PhysicalTableSink targetSink, Set<PhysicalTableSink> srcSinks) {
    links.put(targetSink, srcSinks.stream().flatMap(sink -> getLinkedSrcSinks(sink).stream())
        .collect(Collectors.toList()));
  }

  public RelNode buildCheapestPlan() {
    int endIndex = reoptimize ? 0 : plans.size() - 1;
    // propagate table flags
    propagateTableFlags();

    List<RelNode> eachTimeExecPlans = new ArrayList<>();
    for (int i = 0; i <= endIndex; i++) {
      // merge consecutive non-hardlink nodes together & remove virtual adhocsink
      RelNode plan = splitPlans(i, i + minTime);
      if (plan != null) {
        assert plan instanceof TvrExecPlansOp;
        eachTimeExecPlans.add(plan);
      }
    }

    if (eachTimeExecPlans.isEmpty()) {
      return TvrExecPlansOp.create(cluster, Collections.emptyList(), lastTime);
    }

    return TvrExecPlansOp.create(cluster, eachTimeExecPlans, lastTime);
  }

  private RelNode splitPlans(int index, int execTime) {
    Set<RelNode> currentPlans = plans.get(index);
    if (currentPlans.isEmpty()) {
      return null;
    }

    List<RelNode> fuxiPlans = new ArrayList<>();
    List<RelNode> hardLinkPlans = new ArrayList<>();

    for (RelNode plan : currentPlans) {
      //noinspection SuspiciousMethodCalls
      if (links.containsKey(plan)) {
        hardLinkPlans.add(plan);
      } else {
        fuxiPlans.add(plan);
      }
    }

    RelNode fuxiJob = null;
    if (!fuxiPlans.isEmpty()) {
      PlanMergeHelper mergeHelper = new PlanMergeHelper();
      fuxiPlans.forEach(mergeHelper::go);

      fuxiJob = EnumerablePhysicalVirtualRoot.create(
          fuxiPlans.stream().filter(plan -> !mergeHelper.isReused(plan))
              .collect(Collectors.toList()));
    }

    if (fuxiJob == null && hardLinkPlans.isEmpty()) {
      return null;
    }

    return TvrExecPlansOp.create(cluster, Stream.concat(Stream.of(fuxiJob), hardLinkPlans.stream())
        .filter(Objects::nonNull)
        .map(rel -> TvrExecPlansOp.create(cluster, ImmutableList.of(rel), execTime))
        .collect(Collectors.toList()), execTime);
  }

  class PlanMergeHelper extends RelVisitor {
    // <Input of the Sink -> Sink>
    Map<RelNode, PhysicalTableSink> sinkMap = new HashMap<>();

    // the sinks that have been reused to replace the inputs of nodes
    Set<PhysicalTableSink> reused = new HashSet<>();

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
      if (parent instanceof PhysicalTableSink) {
        sinkMap.put(node, (PhysicalTableSink) parent);
      } else if (sinkMap.containsKey(node)) {
        // parent is not PhysicalTableSink
        PhysicalTableSink sink = sinkMap.get(node);
        parent.replaceInput(ordinal, sink);
        reused.add(sink);
      }

      super.visit(node, ordinal, parent);
    }

    public boolean isReused(RelNode node) {
      return reused.contains(node);
    }
  }

  /**
   * 'ODPS_PROGRESSIVE_STATE_TABLE', 'ODPS_PROGRESSIVE_USE_CACHE' and
   * 'ODPS_PROGRESSIVE_VIRTUAL_SHARD_NUMBER' need to be consistent
   * in the source tables and target table in a hardlink plan.
   */
  private void propagateTableFlags() {
      // do nothing
//    ImmutableList.copyOf(links.entrySet()).reverse().forEach(entry -> {
//      OdpsPhysicalTableSink destSink = entry.getKey();
//      OdpsTable destTable = ((OdpsRelOptTable) destSink.getTable()).getTable();
//
//      boolean isStateTable;
//      boolean adjustShardNumber;
//      boolean isMemoryTable;
//      if (destTable.isCreateNew()) {
//        Map<String, String> properties = destTable.getProperties();
//        isStateTable = properties.get(TvrUtils.ODPS_PROGRESSIVE_STATE_TABLE) != null;
//        isMemoryTable = properties.get(TvrUtils.ODPS_PROGRESSIVE_USE_CACHE) != null;
//        // if the dest table allows driver to adjust its bucket number,
//        // or if this table is not a cluster table,
//        // the bucket number of its linked src tables can also be adjusted.
//        adjustShardNumber = properties.get(TvrTableUtils.ODPS_PROGRESSIVE_VIRTUAL_SHARD_NUMBER) != null
//            || !TvrTableUtils.isClusterTable(destSink.getTraitSet());
//      } else {
//        // this table is the user's table
//        isStateTable = false;
//        isMemoryTable = destTable.isMemory();
//        // if user's table is not a cluster table, the bucket number of its linked src tables
//        // can be adjusted. otherwise, the bucket number of linked tables should
//        // be consistent with the user's table.
//        adjustShardNumber = !TvrTableUtils.isClusterTable(destSink.getTraitSet());
//      }
//
//      entry.getValue().forEach(srcSink -> {
//        Map<String, String> srcProperties =
//            ((OdpsRelOptTable) srcSink.getTable()).getTable().getProperties();
//        if (isStateTable) {
//          srcProperties.put(TvrUtils.ODPS_PROGRESSIVE_STATE_TABLE, "true");
//        } else {
//          srcProperties.remove(TvrUtils.ODPS_PROGRESSIVE_STATE_TABLE);
//        }
//        if (adjustShardNumber && TvrTableUtils.isClusterTable(srcSink.getTraitSet())) {
//          srcProperties.put(TvrTableUtils.ODPS_PROGRESSIVE_VIRTUAL_SHARD_NUMBER, "true");
//        } else if (TvrTableUtils.isClusterTable(destSink.getTraitSet())) {
//          srcProperties.remove(TvrTableUtils.ODPS_PROGRESSIVE_VIRTUAL_SHARD_NUMBER);
//        }
//
//        if (isMemoryTable && srcProperties.get(TvrUtils.ODPS_PROGRESSIVE_USE_CACHE) == null) {
//          throw new IllegalArgumentException(
//              "The storage medium of the target table and linked table are inconsistent");
//        }
//      });
//    });
  }

  List<PhysicalTableSink> getLinkedSrcSinks(PhysicalTableSink sink) {
    List<PhysicalTableSink> srcSinks = links.get(sink);
    return srcSinks == null ? ImmutableList.of(sink) : srcSinks;
  }
}
