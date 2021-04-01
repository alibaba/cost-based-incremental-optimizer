package org.apache.calcite.plan.volcano;

import com.google.common.collect.*;
import com.google.gson.Gson;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.enumerable.EnumerableUnion;
import org.apache.calcite.adapter.enumerable.EnumerableValues;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.TvrNode.TvrGraphConstructor;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AdhocSink;
import org.apache.calcite.rel.core.TableSink;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.temp.PhysicalTableSink;
import org.apache.calcite.rel.tvr.TvrVolcanoPlanner;
import org.apache.calcite.rel.tvr.rels.TvrVirtualSpool;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrTableInfoForMeta;
import org.apache.calcite.rel.tvr.utils.TvrTableUtils;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.DecimalFormat;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.calcite.plan.volcano.MatType.MAT_DISK;
import static org.apache.calcite.plan.volcano.MatType.MAT_MEMORY;
import static org.apache.calcite.plan.volcano.ReuseUtils.*;
import static org.apache.calcite.rel.tvr.utils.TvrSinkUtils.buildMatSink;
import static org.apache.calcite.rel.tvr.utils.TvrTableUtils.isMemoryTable;
import static org.apache.calcite.rel.tvr.utils.TvrUtils.isVirtualTableSink;
import static org.apache.calcite.rel.tvr.utils.TvrUtils.splittable;

public class MqcOptimizer implements MultiQueryCostOptimizer {
  private static final Log LOG = LogFactory.getLog(MqcOptimizer.class);

  private final RelSubset root;
  protected final TvrContext ctx;
  private final TvrVolcanoPlanner planner;
  private final RelOptCostFactory costFactory;
  private final RelMetadataQuery mq;
  private final int lastTime;
  private final int minTime;
  private final TRelOptCost[] zeroCosts;

  private final TxnMap<RelSubset, ReuseImpl> directlyMaterialized;
  private final TxnMap<RelSubset, RelSubset> indirectlyMaterialized;

  // subsets whose costs have changed because of materialization
  protected TxnMap<RelSubset, OptImpl> memo;
  private final BestPlanDepChecker depChecker;
  private final SubsetTopologyPropagator propagator;

  private final ExecPlanManager execPlanManager;
//  private final PlanDigestVisitor planDigestVisitor;

  // for range query optimization
//  private final boolean isRangeQuery;
//  private final TvrPanguDigestManager panguDigestManager;

  // Force some materialized subsets to build table sinks.
  // In general, mqc does not choose to build table sink for the last delta,
  // because no node will reuse this state in this query.
  // But in some cases (range query optimization or triggering downstream queries),
  // the last delta needs to be saved.
  // Therefore, we need to force these missing nodes to build sink.
  private final boolean forceBuildSink;

  public MqcOptimizer(TvrContext ctx, TvrVolcanoPlanner planner, RelSubset root,
                      Map<RelSubset, Integer> execTime, boolean forceBuildSink) {
    this.root = root;
    this.ctx = ctx;
    this.planner = planner;
    this.costFactory = planner.getCostFactory();
    this.lastTime = Collections.max(execTime.values());
    this.minTime = ctx.getCurrentOptimizationRound();

//    boolean isMultiInsert = findActualSinks(collectSinks(root)).stream()
//        .filter(r -> r instanceof OdpsPhysicalTableSink).distinct().count() > 1;
    boolean isMultiInsert = false;
    this.execPlanManager = new ExecPlanManager(minTime, lastTime,
        TvrUtils.progressiveReoptimizeEnabled(ctx), isMultiInsert, root.getCluster());

    this.forceBuildSink = forceBuildSink;
//    isRangeQuery = TvrUtils.progressiveRangeQueryOptEnabled(ctx);
//    panguDigestManager = isRangeQuery ? TvrPanguDigestManager.getInstance() : null;

    directlyMaterialized = new TxnMap<>(Maps::newIdentityHashMap);
    indirectlyMaterialized = new TxnMap<>(Maps::newIdentityHashMap);

    memo = new TxnMap<>(Maps::newIdentityHashMap);
    depChecker = new BestPlanDepChecker();
//    planDigestVisitor = new PlanDigestVisitor();

    SubsetTopologyGraph subsetTopoGraph = new SubsetTopologyGraph();
    subsetTopoGraph.expandGraphFromSubset(root);
    assert subsetTopoGraph.membership.keySet().equals(execTime.keySet());

    propagator = new SubsetTopologyPropagator(subsetTopoGraph, dirtySubsets -> {
      RelSubset subset = dirtySubsets.iterator().next();
      int count = 0;
      // Find and return the dirtySubset whose best plan does not contain any
      // other dirty subsets in this clique
      while (true) {
        RelSubset subsetFinal = subset;
        subset = depChecker
            .visit(subset, false, UseType.TO_OP, memo.get(subset).time())
            .stream().filter(s -> s != subsetFinal && dirtySubsets.contains(s))
            .findFirst().orElse(null);
        if (subset == null) {
          return subsetFinal;
        }
        if (count++ > dirtySubsets.size()) {
          throw new RuntimeException(
              "DepChecker best plan loop detected: " + dirtySubsets);
        }
      }
    });

    // for performance, prepare a bunch of zeroCosts to be reused
    this.zeroCosts = new TRelOptCost[this.lastTime + 1];
    for (int i = 0; i < this.zeroCosts.length; ++i) {
      this.zeroCosts[i] = makeCost(this.costFactory.makeZeroCost(), i);
    }

    if (false) {
      // Do not use cost in volcano memo for now and always recompute costs
      // ourselves because volcano's cost has consistency issue sometimes.
      this.mq = root.getCluster().getMetadataQuery();
      initMemoWithExistingCosts(execTime);
    } else {
      // Since cluster.getMetadataQuery() already have a fully loaded cache
      // with stale results, let's create a new one to re-compute everything
      this.mq = RelMetadataQuery.instance();

      // ingest manually defined row count for testing purposes
      Map<Integer, Double> fakeRowCount = ProgressiveExperimentUtil.loadFakeRowCount(ctx);
      if (fakeRowCount != null) {
        this.mq.setFakeRowCount(fakeRowCount);
        // Make sure the memo of our run and the fake row count indeed match
        Set<Integer> mySetIds = ProgressiveExperimentUtil.getRelSetsForRowCount(
            ProgressiveExperimentUtil.getRowCountSubsets(planner, execTime.keySet()));
        assert mySetIds.equals(fakeRowCount.keySet()) :
            "setId doesn't much, my set ids:\n" + mySetIds
                + "\nSet ids from row count config:\n" + fakeRowCount.keySet();
      }

      // recompute the initial costs in our temporally expanded memo
      // with possibly modified costs (fakeRowCount or ctx.getJoinHugeCost())
      initMemoWithRecomputedCosts(execTime);
    }

    // TODO: remove this
    // Collect info from the (adjusted) initial memo
    ctx.mqcMemoCostString = memo.entrySet().stream().map(entry -> {
      OptImpl record = entry.getValue();
      RelOptCost cost = record.bestCost.floating;
      if (record.best == null || cost.isInfinite()) {
        return Pair.of(entry.getKey(), "");
      }
      return Pair.of(entry.getKey(),
          new DecimalFormat("#.###E0").format(mq.getRowCount(record.best))
              + " - " + new DecimalFormat("#.######E0")
              .format(TRelOptCost.getCost(cost)));
    }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  public MqcOptimizer reset(RelSubset root, Map<RelSubset, Integer> execTime) {
    MqcOptimizer newMqc = new MqcOptimizer(ctx, planner, root, execTime, forceBuildSink);
    // the new mqc needs to mark the previously executed subset as materialized
    int minTime = ctx.getCurrentOptimizationRound();
    directlyMaterialized.entrySet().stream()
        .filter(e -> execTime.containsKey(e.getKey()))
        .filter(e -> execTime.get(e.getKey()) < minTime)
        .forEach(e -> e.getValue().matTypes.forEach(matType -> {
          boolean suc =
              newMqc.tryToMat(ImmutableSet.of(e.getKey()), matType, true);
          assert suc;
          newMqc.commit();
        }));
    return newMqc;
  }

  private void initMemoWithExistingCosts(Map<RelSubset, Integer> execTime) {
    execTime.forEach((subset, t) -> {
      assert subset.best != null && !subset.bestCost.isInfinite();
      TRelOptCost cost = makeCost(subset.bestCost, t == -1 ? 0 : t);
      memo.put(subset, OptImpl.of(cost, subset.best, ImmutableBitSet.of()));
    });
    memo.commit();
  }

  private void initMemoWithRecomputedCosts(Map<RelSubset, Integer> execTime) {
    SetMultimap<RelNode, RelNode> dep = Multimaps
        .newSetMultimap(Maps.newIdentityHashMap(), Sets::newIdentityHashSet);
    TopoOrderQueue<RelNode> topoHeap =
        new TopoOrderQueue<>(Comparator.comparingInt(r -> dep.get(r).size()));

    execTime.forEach((subset, t) -> {
      assert subset.best != null && !subset.bestCost.isInfinite();
      TRelOptCost cost = makeCost(costFactory.makeInfiniteCost(), t == -1 ? 0 : t);
      memo.put(subset, OptImpl.of(cost, null, ImmutableBitSet.of()));

      for (RelNode rel : subset.getRels()) {
        if (rel.getConvention() != Convention.NONE) {
          List<RelNode> inputs = rel.getInputs();
          if (inputs.isEmpty()) {
            topoHeap.offer(rel);
            continue;
          }
          for (RelNode input : inputs) {
            dep.putAll(rel, ((RelSubset) input).getRels());
          }
        }
      }
    });

    while (!topoHeap.isEmpty()) {
      RelNode rel = topoHeap.poll();
      for (RelNode parent : planner.getSubset(rel).getParentRels()) {
        if (dep.remove(parent, rel)) {
          topoHeap.heapify(parent);
        }
      }
      RelOptCost cost = computeCost(rel);
      RelSubset subset = planner.getSubset(rel);
      subset.getSatisfyingSubsets().forEach(s -> {
        OptImpl optImpl = memo.get(s);
        if (optImpl != null && (cost.isLt(optImpl.bestCost.floating)
            || optImpl.best == null)) {
          optImpl.bestCost.floating = cost;
          memo.put(s, OptImpl.of(optImpl.bestCost, rel, ImmutableBitSet.of()));
          for (RelNode parent : s.getParentRels()) {
            if (parent.getInputs().stream()
                .anyMatch(input -> input != s && !memo.containsKey(input))
                || !parent.getInputs().contains(s)) {
              continue;
            }
            topoHeap.offer(parent);
          }
        }
      });
    }
    memo.commit();
  }

  private RelOptCost computeCost(RelNode rel) {
    RelOptCost cost = mq.getNonCumulativeCost(rel);
    for (RelNode input : rel.getInputs()) {
      OptImpl optImpl = memo.get(input);
      if (optImpl == null) {
        return costFactory.makeInfiniteCost();
      }
      cost = cost.plus(optImpl.bestCost.floating);
      if (cost.isInfinite()) {
        return costFactory.makeInfiniteCost();
      }
    }
    return cost;
  }

  /**
   * If strict, try materialize only the exact given subsets, otherwise return
   * false. If not strict, materialize the realSubset instead if necessary.
   */
  @Override
  public boolean tryToMat(Set<RelSubset> subsets, MatType matType,
      boolean strict) {
    propagator.reset(false);

    for (RelSubset subset : subsets) {
      while (subset != null) {
        subset = tryToMatInternal(subset, matType);
        if (strict && subset != null) {
          rollback();
          return false;
        }
      }
    }
    // propagate the effect of the materialization
    while (propagator.hasNext()) {
      RelSubset s = propagator.next();
      for (RelNode p : s.getParentRels()) {
        propagateUpdates(p, s, false);
      }
    }
    return true;
  }

  /**
   * Try materialize a subset. Return null if success, otherwise return the
   * new subset to try again with.
   */
  private RelSubset tryToMatInternal(RelSubset subset, MatType matType) {
    RelSubset cause = indirectlyMaterialized.get(subset);
    if (cause != null) {
      return cause;
    }
    ReuseImpl reuseImpl = directlyMaterialized.get(subset);
    if (reuseImpl != null && reuseImpl.matTypes.contains(matType)) {
      return null;
    }

    OptImpl optImpl = memo.get(subset);
    TRelOptCost reuseCost =
        makeCost(makeReadIoCost(subset, matType), optImpl.time());
    if (reuseImpl == null) {
      reuseImpl = new ReuseImpl(EnumSet.of(matType), reuseCost);
    } else {
      EnumSet<MatType> newMatTypes = EnumSet.copyOf(reuseImpl.matTypes);
      newMatTypes.add(matType);
      TRelOptCost newReuseCost = reuseImpl.reuseCost.isLt(reuseCost) ?
          reuseImpl.reuseCost :
          reuseCost;
      reuseImpl = new ReuseImpl(newMatTypes, newReuseCost);
    }

    optImpl = OptImpl.of(optImpl.bestCost
            .pointPlus(makeWriteIoCost(subset, EnumSet.of(matType))), optImpl.best,
        optImpl.matOption);
    for (RelNode rel : subset.getRels()) {
      if (splittable(rel)) {
        OptImpl newOptImpl = recomputeCost(subset, optImpl, reuseImpl, rel, false);
        if (newOptImpl != null) {
          optImpl = newOptImpl;
        }
      }
    }
    RelSubset realSubset = planner.getSubset(optImpl.best);
    if (realSubset != subset && !subset.getTraitSet()
        .satisfies(realSubset.getTraitSet())) {
      // No need to rollback since we haven't made any changes yet
      return realSubset;
    }

    memo.put(subset, optImpl);
    directlyMaterialized.put(subset, reuseImpl);
    depChecker.updateDep(subset, optImpl);

    // find all NEWLY-indirectly-materialized subsets
    Set<RelSubset> newIndirect =
        subset.getSatisfyingSubsets().collect(Collectors.toSet());
    newIndirect.removeAll(directlyMaterialized.keySet());
    Iterator<RelSubset> iter = newIndirect.iterator();
    while (iter.hasNext()) {
      RelSubset s = iter.next();
      if (!memo.containsKey(s)) {
        iter.remove();
        continue;
      }
      ReuseImpl oldReuseImpl = getReuseImpl(s);
      if (oldReuseImpl == null || reuseImpl.reuseCost.isLt(oldReuseImpl.reuseCost) && reuseImpl.matTypes.containsAll(oldReuseImpl.matTypes)) {
        // if it was newly indirectly materialized or has a better way to be indirectly materialized
        indirectlyMaterialized.put(s, subset);
      } else {
        iter.remove();
      }
    }
    // Mark the subsets that needs cost propagation
    propagator.addDirtySubset(subset);
    return null;
  }

  private void propagateUpdates(RelNode p, RelSubset trigger, boolean costIncrease) {
    if (TvrUtils.isOdpsLogicalOperator(p) || !TvrUtils.notConverter(p) || p
        .getInputs().stream().anyMatch(input -> !memo.containsKey(input))) {
      return;
    }

    // If trigger is directly materialized, we need to propagate its
    // indirectly materialized subset's parent rels as well, since trigger
    // is in their dependencies too
    if (trigger != null) {
      if (p.getInputs().stream().noneMatch(input -> input == trigger
          || indirectlyMaterialized.get(input) == trigger)) {
        return;
      }
    }

    Set<RelSubset> as =
        planner.getSubset(p).getSatisfyingSubsets().collect(Collectors.toSet());
    for (RelSubset s : as) {
      OptImpl optImpl = memo.get(s);
      if (optImpl == null) {
        continue;
      }
      optImpl = recomputeCost(s, optImpl, directlyMaterialized.get(s), p, costIncrease);

      if (optImpl != null) {
        if (costIncrease) {
          // p is the old best and its cost increased or dependency changed
          // Iterate through all relNodes and find the new best for subset s
          for (RelNode rel : s.getRels()) {
            if (TvrUtils.isOdpsLogicalOperator(rel) || !TvrUtils.notConverter(rel)
                || rel == p) {
              continue;
            }
            OptImpl newOptImpl =
                recomputeCost(s, optImpl, directlyMaterialized.get(s), rel, false);
            if (newOptImpl != null) {
              optImpl = newOptImpl;
            }
          }
        }
        memo.put(s, optImpl);
        depChecker.updateDep(s, optImpl);
        propagator.addDirtySubset(s);
      }
    }
  }

  private OptImpl recomputeCost(RelSubset target, OptImpl optImpl,
      ReuseImpl reuseImpl, RelNode rel, boolean allowCostIncrease) {
    EnumSet<MatType> matTypes;
    if (reuseImpl != null) {
      matTypes = reuseImpl.matTypes;
    } else if (rel instanceof TableSink) {
      matTypes = EnumSet.of(MAT_DISK);
    } else {
      matTypes = EnumSet.noneOf(MatType.class);
    }
    return recomputeCost(target, optImpl, rel, matTypes, allowCostIncrease);
  }

  /**
   * Checks the `target` RelSubset whether one of its relNodes
   *  `rel` can be updated as the best plan by recomputing the cost of `rel`.
   * Returns a Record if the best plan and cost needs to be updated, null otherwise.
   *
   * In order to recompute the cost, for each input subset of `rel`, checks:
   *  1) if the input subset is materialized
   *  2) if the input subset forms a cycle (input subset depends on `target` subset)
   *
   * If this subset will be materialized, the following situations may occur:
   * case:
   *       Sink                          Sink
   *        |                             |
   *      Union (MAT_DISK/MEMORY)  =>   Union (hard link & no write cost)
   *       / \                           /     \
   *      A  B            A (MAT_DISK/MEMORY)   B (MAT_DISK/MEMORY)
   */
  private OptImpl recomputeCost(RelSubset target, OptImpl optImpl, RelNode rel,
      Set<MatType> matTypes, boolean allowCostIncrease) {
    assert !(rel instanceof RelSubset);

    int optTime = optImpl.time();
    TRelOptCost cost = makeCost(costFactory.makeZeroCost(), optTime);
    ImmutableBitSet.Builder matOption = ImmutableBitSet.builder();
    Set<RelSubset> dep;
    boolean splittable = splittable(rel);

    List<RelNode> inputs = rel.getInputs();
    List<Set<RelSubset>> newInputDeps = new ArrayList<>();
    for (int i = 0; i < inputs.size(); ++i) {
      RelSubset subset = (RelSubset) inputs.get(i);
      OptImpl bestInput = memo.get(subset);
      if (bestInput == null) {
        return null;
      }
      int inputTime = bestInput.time();
      ReuseImpl iReuseImpl = getReuseImpl(subset);
      EnumSet<MatType> iMatTypes = iReuseImpl != null ?
          iReuseImpl.matTypes :
          EnumSet.noneOf(MatType.class);
      TRelOptCost bestCost;
      // input subset is materialized and does not form a cycle
      // use reuse materialization cost as bestCost
      if (iReuseImpl != null && !(dep =
          depChecker.visit(subset, true, UseType.TO_OP, inputTime))
          .contains(target)) {
        // indicate the `i-th` input is materialized in the corresponding bit
        matOption.set(i);
        bestCost = zeroCosts[inputTime];
      } else if (!(dep =
          depChecker.visit(subset, false, UseType.TO_OP, optTime))
          .contains(target)) {
        // input subset is not materialized and does not form a cycle
        bestCost = bestInput.bestCost;
      } else {
        // target is in the dependency of rel
        return null;
      }
      newInputDeps.add(dep);

      if (!matTypes.isEmpty() && splittable) {
        bestCost = bestCost.pointPlus(
            makeWriteIoCost(subset, Sets.difference(matTypes, iMatTypes)));
        // for re-optimization,
        // if this input rel node is not materialized in the past,
        // it can only be executed when the time is greater than or equal to
        // minTime.
        cost = cost.plusInPlace(
            settleCost(bestCost, Math.max(inputTime, minTime)));
      } else {
        cost = cost.plusInPlace(bestCost);
      }
    }

    if (!matTypes.isEmpty() && !splittable) {
      cost = cost.pointPlusInPlace(makeWriteIoCost(target, matTypes));
    }

    RelOptCost localCost;
    if (rel instanceof TvrVirtualSpool && !matOption.isEmpty()) {
      // minus write cost
      localCost = mq.getNonCumulativeCost(rel).minus(makeWriteDiskIoCost(target).multiplyBy(2));
    } else if (rel instanceof TableSink) {
      localCost = costFactory.makeZeroCost();
    } else {
      localCost = mq.getNonCumulativeCost(rel);
    }
    cost = cost.pointPlusInPlace(localCost);

    boolean update = false;
    if (cost.isLt(optImpl.bestCost)) {
      update = true;
    } else if (rel == optImpl.best) {
      if (allowCostIncrease && optImpl.bestCost.isLt(cost)) {
        update = true;
      } else {
        Set<RelSubset> cachedDep =
            depChecker.visit(target, false, UseType.TO_OP, optTime);
        Set<RelSubset> newDep = depChecker.visit(target, optImpl, newInputDeps);
        if (!cachedDep.equals(newDep)) {
          update = true;
        }
      }
    }
    if (update) {
      return OptImpl.of(cost, rel, matOption.build());
    }
    return null;
  }

  @Override
  public void relCostIncreased(Set<RelNode> rels) {
    propagator.reset(false);
    for (RelNode p : rels) {
      propagateUpdates(p, null, true);
    }
    while (propagator.hasNext()) {
      RelSubset s = propagator.next();
      for (RelNode p : s.getParentRels()) {
        propagateUpdates(p, s, true);
      }
    }
  }

  @Override
  public RelNode getBest(RelSubset subset) {
    return memo.get(subset).best;
  }

  /**
   * Make the cost for reading a materialized node.
   * Copied from OdpsPhysicalTableScan.computeSelfCost.
   */
  private RelOptCost makeReadIoCost(RelSubset subset, MatType matType) {
    double factor;
    switch (matType) {
    case MAT_MEMORY:
      factor = 0;
      break;
    case MAT_DISK:
      factor = 1;
      break;
    default:
      throw new UnsupportedOperationException("Unknown materialization type");
    }

    double dRows = mq.getRowCount(subset);
    // Assume 100% locality.
    // Assume that writing to memory is 100X faster than to disk
    Double averageRowSize = mq.getAverageRowSize(subset);
    double dSize = dRows * mq.getAverageRowSize(subset) * factor;
    RelOptCostFactory costFactory = planner.getCostFactory();
    RelOptCost cost = costFactory.makeCost(dRows, 0, dSize);

    boolean isDeliveredTrait =
            subset.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE) != null &&
            subset.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE) != null && (
        subset.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE).getType()
            != RelDistribution.Type.ANY ||
            subset.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE)
                .getFieldCollations().size() > 0);

    if (isDeliveredTrait) {
      RelOptCost cost1 = cost.multiplyBy(1.1);
      RelOptCost cost2 = cost.plus(planner.getCostFactory().makeCost(0, 0, 10));
      if (cost1.isLe(cost2)) {
        cost = cost1;
      } else {
        cost = cost2;
      }
    }
    return cost;
  }

  private RelOptCost makeWriteIoCost(RelSubset subset, Set<MatType> matTypes) {
    RelOptCost cost = costFactory.makeZeroCost();
    if (matTypes.contains(MAT_MEMORY)) {
      cost = cost.plus(makeWriteMemoryIoCost(subset));
    }

    if (matTypes.contains(MAT_DISK)) {
      cost = cost.plus(makeWriteDiskIoCost(subset));
    }

    return cost;
  }

  /**
   * Make the cost for writing a materialized node.
   */
  private RelOptCost makeWriteDiskIoCost(RelSubset subset) {
    double dRows = mq.getRowCount(subset);
    double dCpu = 0;
    double dSize = dRows * mq.getAverageRowSize(subset);
    RelOptCostFactory costFactory = planner.getCostFactory();
    return costFactory.makeCost(dRows, dCpu, dSize);
  }

  private RelOptCost makeWriteMemoryIoCost(RelSubset subset) {
    double dRows = mq.getRowCount(subset);
    double dCpu = 0;
    // Assume 1 local write
    double dSize = dRows * mq.getAverageRowSize(subset) * 0.01;
    RelOptCostFactory costFactory = planner.getCostFactory();
    return costFactory.makeCost(dRows, dCpu, dSize);
  }

  @Override
  public void rollback() {
    directlyMaterialized.rollback();
    indirectlyMaterialized.rollback();
    memo.rollback();
    depChecker.rollback();
    propagator.reset(true);
  }

  @Override
  public void commit() {
    directlyMaterialized.commit();
    indirectlyMaterialized.commit();
    memo.commit();
    depChecker.commit();
    propagator.reset(false);
  }

  @Override
  public Set<RelSubset> getMaterialized() {
    return directlyMaterialized.keySet();
  }

  public RelOptCost getOverallCost() {
    OptImpl opt = memo.get(root);
    TRelOptCost overall = new TRelOptCost(opt.bestCost);

    ReuseCostVisitor reuseCostVisitor = new ReuseCostVisitor();
    reuseCostVisitor.visit(root, false, UseType.TO_OP, opt.time());
    TRelOptCost reuseCost = reuseCostVisitor.getCost();
    overall.plusInPlace(reuseCost);

    directlyMaterialized.forEach((subset, reuseImpl) -> {
      OptImpl optImpl = memo.get(subset);
      Set<MatType> matTypes = reuseCostVisitor.getRealMatTypes(subset);
//      if (isRangeQuery) {
//        Set<MatType> candidateTypes = new HashSet<>(matTypes);
//        for (MatType matType: matTypes) {
//          String digest = planDigestVisitor.getTableDigest(subset, optImpl, matType);
//          if (getStateTableForRangeQuery(digest) != null) {
//            // this table has been saved before,
//            // do not add write cost here.
//            candidateTypes.remove(matType);
//          }
//        }
//
//        if (candidateTypes.isEmpty()) {
//          return;
//        }
//        matTypes = candidateTypes;
//      }

      OptImpl newOptImpl = recomputeCost(subset, optImpl, optImpl.best, matTypes, true);
      if (newOptImpl != null) {
        optImpl = newOptImpl;
      }
      overall.plusInPlace(settleCost(optImpl.bestCost));
    });

    return overall;
  }

  public RelNode buildCheapestPlan(Collection<RelNode> bestNodes) {
    CheapestPlanBuilder cheapestPlanBuilder = new CheapestPlanBuilder();
    RelNode cheapestPlan = cheapestPlanBuilder.build(root);

    // TODO: remove this
    // Collect results from cheapestPlanBuilder
    ctx.mqcBestPlanMapping = cheapestPlanBuilder.visited.stream()
        .flatMap(m -> m.entrySet().stream()).distinct()
        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

    if (bestNodes != null) {
      bestNodes.addAll(cheapestPlanBuilder.visited.stream()
          .flatMap(m -> m.keySet().stream()).distinct().map(this::getBest)
          .collect(Collectors.toSet()));
    }

    return cheapestPlan;
  }

  private ReuseImpl getReuseImpl(RelSubset subset) {
    RelSubset cause = indirectlyMaterialized.get(subset);
    if (cause != null) {
      subset = cause;
    }

    return directlyMaterialized.get(subset);
  }

  @Override
  public RelOptCost potentialBenefit(RelSubset subset, int numReuse) {
    OptImpl optImpl = memo.get(subset);
    if (optImpl == null) {
      return null;
    }
    TRelOptCost cost = optImpl.bestCost;
    if (cost.earliest == lastTime) {
      numReuse = numReuse - 1;
    }
    cost = cost.multiplyBy(numReuse);
    return new TRelOptCost(cost.floating, lastTime, cost.fixed);
  }

  public static double getRowCount(TxnMap<RelSubset, OptImpl> memo, RelSubset subset) {
    OptImpl opt = memo.get(subset);
    double rowCount = opt.bestCost.getRows();
    if (opt.best instanceof Union) {
      rowCount = 0;
      for (RelNode r : opt.best.getInputs()) {
        rowCount += getRowCount(memo, (RelSubset) r);
      }
    }
    return rowCount;
  }

//  private String[] getStateTableForRangeQuery(String digest) {
//    if (!isRangeQuery) {
//      throw new IllegalArgumentException("This query is not a range query.");
//    }
//
//    String[] normalizedName = null;
//    Collection<String> stateTables = panguDigestManager.getStateTableName(digest);
//    if (stateTables != null) {
//      LOG.info("Find the state tables with the given plan digest (" + digest + "): "
//          + stateTables.stream().reduce((t1, t2) -> t1 + "," + t2));
//      for (String table: stateTables) {
//        LOG.info("Double check the plan digest in this table: " + table);
//        Table t = getTableFromMeta(table);
//        // double check whether the table digests are consistent
//        if (t != null && t.getParameters()
//            .getOrDefault(ODPS_PROGRESSIVE_TABLE_DIGEST, "")
//            .equalsIgnoreCase(digest)) {
//          // this state has already been saved before
//          normalizedName = new String[2];
//          normalizedName[0] = t.getProject();
//          normalizedName[1] = t.getName();
//          LOG.info("Find the state table " + normalizedName[0] + "."
//              + normalizedName[1] + "!");
//          break;
//        }
//      }
//    }
//    if (normalizedName == null) {
//      LOG.info("Can not find a state table for this plan: " + digest);
//    }
//    return normalizedName;
//  }

  abstract class BestPlanVisitor<T> {
    public final List<Map<RelSubset, T>> visited;
    public final EnumMap<MatType, Map<RelSubset, T>> materialized;

    BestPlanVisitor() {
      this(IdentityHashMap::new);
    }

    BestPlanVisitor(Supplier<Map<RelSubset, T>> supplier) {
      this.visited = new ArrayList<>(lastTime + 1);
      for (int i = 0; i <= lastTime; ++i) {
        visited.add(supplier.get());
      }
      this.materialized = new EnumMap<>(MatType.class);
      for (MatType m : MatType.values()) {
        this.materialized.put(m, supplier.get());
      }
    }

    public T visit(RelSubset subset, boolean mat, UseType howUsed, int whenUsed) {
      if (mat) {
        RelSubset cause = indirectlyMaterialized.get(subset);
        if (cause != null) {
          return visit(cause, true, howUsed, whenUsed);
        }

        if (howUsed != UseType.TO_OP) {
          MatType matType = useType2matType(howUsed);
          Map<RelSubset, T> ms = materialized.get(matType);
          T matRet = ms.get(subset);
          if (matRet == null) {
            OptImpl optImpl = memo.get(subset);
            matRet = matRel(subset, optImpl, matType, howUsed);
            ms.put(subset, matRet);
          }
          return matRet;
        }

        // howUsed == UseType.TO_OP
        OptImpl optImpl = memo.get(subset);
        if (whenUsed == optImpl.time() && !forceBuildSink) {
          // if forceBuildSink is true, this subset must be forced to materialize,
          // even if no one will reuse it in the future.
          return visit(subset, false, UseType.TO_OP, whenUsed);
        }

        Map<RelSubset, T> vs = visited.get(whenUsed);
        T matRet = vs.get(subset);
        if (matRet != null) {
          return matRet;
        }

        ReuseImpl reuseImpl = directlyMaterialized.get(subset);
        MatType matType = MatType.best(reuseImpl.matTypes);

        Map<RelSubset, T> ms = materialized.get(matType);
        matRet = ms.get(subset);
        if (matRet == null) {
          matRet = matRel(subset, optImpl, matType, matType2UseType(matType));
          ms.put(subset, matRet);
        }

        if (whenUsed == optImpl.time()) {
          // forceBuildSink == true
          // If the time when the node is reused is consistent with its execution time,
          // it is better to reuse itself directly instead of reusing its output state.
          matRet = visit(subset, false, UseType.TO_OP, whenUsed);
        } else {
          matRet = reuse(subset, matType, whenUsed, matRet);
        }
        vs.put(subset, matRet);
        return matRet;
      }

      Map<RelSubset, T> vs = visited.get(whenUsed);
      T ret  = vs.get(subset);
      if (ret != null) {
        return ret;
      }

      OptImpl optImpl = memo.get(subset);
      if (whenUsed != optImpl.time()) {
        for (int i = optImpl.time() + 1; i <= lastTime; ++i) {
          if (i != whenUsed) {
            ret  = visited.get(i).get(subset);
            if (ret != null) {
              vs.put(subset, ret);
              return ret;
            }
          }
        }
      }

      UseType how = howUsed;
      if (optImpl.best instanceof TableSink) {
        how = isMemoryTable((PhysicalTableSink) optImpl.best) ?
            UseType.TO_MEMORY :
            UseType.TO_DISK;
        whenUsed = optImpl.time();
      } else if (optImpl.best instanceof AdhocSink) {
        whenUsed = optImpl.time();
      }

      List<RelNode> inputs = optImpl.best.getInputs();
      List<T> inputRets = new ArrayList<>(inputs.size());
      for (int i = 0; i < inputs.size(); ++i) {
        RelSubset input = (RelSubset) inputs.get(i);
        boolean inputMaterialized = optImpl.inputMaterialized(i);
        T inputRet = visit(input, inputMaterialized, how, whenUsed);
        inputRets.add(inputRet);
      }
      ret = visit(subset, optImpl, inputRets);
      vs.put(subset, ret);
      return ret;
    }

    T matRel(RelSubset subset, OptImpl optImpl, MatType matType, UseType howUsed) {
      RelNode best = optImpl.best;
      boolean splittable = splittable(best);
      T matRet;
      if (splittable) {
        List<T> newInputs = new ArrayList<>();
        List<RelNode> inputs = best.getInputs();
        for (int i = 0; i < inputs.size(); i++) {
          RelNode rel = inputs.get(i);
          RelSubset input = (RelSubset) rel;
          T inputMatRet;
          if (optImpl.inputMaterialized(i)) {
            inputMatRet = visit(input, true, howUsed, memo.get(input).time());
          } else {
            Map<RelSubset, T> vs = materialized.get(matType);
            inputMatRet = vs.get(input);
            if (inputMatRet == null) {
              inputMatRet = visit(input, false, UseType.TO_OP, memo.get(input).time());
              // for re-optimization,
              // if this input rel node is not materialized in the past,
              // it can only be executed when the current time is greater than or equal to minTime.
              int matTime = Math.max(memo.get(input).time(), minTime);
              inputMatRet = materialize(input, memo.get(input), matTime, matType, inputMatRet);
              vs.put(input, inputMatRet);
            }
          }
          newInputs.add(inputMatRet);
        }
        matRet = materialize(subset, optImpl, matType, newInputs);
      } else {
        T nonMat = visit(subset, false, UseType.TO_OP, memo.get(subset).time());
        matRet = materialize(subset, optImpl, optImpl.time(), matType, nonMat);
      }
      return matRet;
    }

    abstract T visit(RelSubset subset, OptImpl optImpl, List<T> inputRets);

    abstract T materialize(RelSubset subset, OptImpl optImpl, int matTime,
        MatType matType, T nonMatRet);

    T materialize(RelSubset subset, OptImpl optImpl, MatType matType, List<T> inputRets) {
      T nonMat = visit(subset, optImpl, inputRets);
      return materialize(subset, optImpl, optImpl.time(), matType, nonMat);
    }

    abstract T reuse(RelSubset subset, MatType matType, int whenUsed, T matRet);

  }

  class BestPlanDepChecker extends BestPlanVisitor<Set<RelSubset>>
      implements TransactionSupport {

    BestPlanDepChecker() {
      super(() -> new TxnMap<>(IdentityHashMap::new));
    }

    @Override
    Set<RelSubset> visit(RelSubset subset, OptImpl optImpl, List<Set<RelSubset>> inputRets) {
      Set<RelSubset> dep = Sets.newIdentityHashSet();
      dep.add(subset);
      inputRets.forEach(dep::addAll);
      return dep;
    }

    @Override
    Set<RelSubset> materialize(RelSubset subset, OptImpl optImpl, int matTime,
        MatType matType, Set<RelSubset> nonMatRet) {
      return nonMatRet;
    }

    @Override
    Set<RelSubset> reuse(RelSubset subset, MatType matType, int whenUsed, Set<RelSubset> matRet) {
      return matRet;
    }

    void updateDep(RelSubset subset, OptImpl optImpl) {
      List<RelNode> inputs = optImpl.best.getInputs();
      List<Set<RelSubset>> inputRets = new ArrayList<>(inputs.size());
      for (int i = 0; i < inputs.size(); ++i) {
        RelSubset input = (RelSubset) inputs.get(i);
        Set<RelSubset> inputRet =
            visit(input, optImpl.inputMaterialized(i), UseType.TO_OP, optImpl.time());
        inputRets.add(inputRet);
      }

      Set<RelSubset> ret = visit(subset, optImpl, inputRets);
      for (int i = optImpl.time(); i <= lastTime; ++i) {
        visited.get(i).put(subset, ret);
      }
      materialized.values().forEach(map -> map.remove(subset));

      // need to clear the dirty cache for all subsets above whose current
      // best plan contains me
      invalidateCache(subset);
    }

    void invalidateCache(RelSubset s) {
      for (RelNode p : s.getParentRels()) {
        if (TvrUtils.isOdpsLogicalOperator(p) || !TvrUtils.notConverter(p) || p
            .getInputs().stream().anyMatch(input -> !memo.containsKey(input))) {
          continue;
        }
        if (p.getInputs().stream().noneMatch(
            input -> input == s || indirectlyMaterialized.get(input) == s)) {
          continue;
        }

        Set<RelSubset> as = planner.getSubset(p).getSatisfyingSubsets()
            .collect(Collectors.toSet());
        for (RelSubset subset : as) {
          OptImpl optImpl = memo.get(subset);
          if (optImpl == null || optImpl.best != p) {
            continue;
          }
          List<RelNode> inputs = optImpl.best.getInputs();
          for (int i = 0; i < inputs.size(); ++i) {
            RelNode dep = inputs.get(i);
            if (optImpl.matOption.get(i) && !directlyMaterialized.containsKey(dep)) {
              dep = indirectlyMaterialized.get(dep);
              assert dep != null;
            }
            // subset's best indeed depends on s
            if (dep == s) {
              boolean changed = false;
              for (Map<RelSubset, Set<RelSubset>> map : visited) {
                changed |= (map.remove(subset) != null);
              }
              for (Map<RelSubset, Set<RelSubset>> map : materialized.values()) {
                changed |= (map.remove(subset) != null);
              }
              if (changed) {
                invalidateCache(subset);
              }
              break;
            }
          }
        }
      }
    }

    @Override
    public void rollback() {
      visited.forEach(m -> ((TxnMap<RelSubset, Set<RelSubset>>) m).rollback());
      materialized.values().forEach(m -> ((TxnMap<RelSubset, Set<RelSubset>>) m).rollback());
    }

    @Override
    public void commit() {
      visited.forEach(m -> ((TxnMap<RelSubset, Set<RelSubset>>) m).commit());
      materialized.values().forEach(m -> ((TxnMap<RelSubset, Set<RelSubset>>) m).commit());
    }
  }

  class CheapestPlanBuilder extends BestPlanVisitor<RelNode> {

    // <digest, list of table names>
    // different tables may have the same digest
    private Multimap<String, String> rangeQueryStateTables = ArrayListMultimap.create();

    public RelNode build(RelSubset subset) {
      visit(subset, false, UseType.TO_OP, memo.get(subset).time());

      if (TvrUtils.progressiveDerivedTableEnabled(ctx) && !planner.getInputTvrToSinksMap().isEmpty()) {
        // If there is a table sink in the memo,
        // it means that its results may be used by other downstream queries,
        // so some tvr relations need to be recorded here.
        addSinksForDownStream();
      }

//      if (isRangeQuery) {
//        // record state tables and their digests
//        panguDigestManager.readyToDumpTableDigests(rangeQueryStateTables);
//      }

      return execPlanManager.buildCheapestPlan();
    }

    @Override
    final RelNode visit(RelSubset subset, OptImpl optImpl, List<RelNode> inputRets) {
      int time = optImpl.time();
      RelNode best = optImpl.best;
      if (best instanceof TableSink) {
        if (isVirtualTableSink(best)) {
          // skip virtual tablesink
          return best;
        }

        // this rel is the original table sink in the memo
        EnumerableValues values = EnumerableValues
            .create(best.getCluster(), best.getRowType(), ImmutableList.of());
        best = best.copy(best.getTraitSet(), ImmutableList.of(values));

        RelNode sink = inputRets.get(0);
        assert sink instanceof TableSink;
        assert best.getRowType().getFieldCount() == sink.getRowType().getFieldCount();

        execPlanManager.addHardLinkPlan(time, (PhysicalTableSink) best,
            ImmutableSet.of((PhysicalTableSink) sink));
      } else if (best instanceof AdhocSink) {
        best = best.copy(best.getTraitSet(), ImmutableList.of(inputRets.get(0)));
        execPlanManager.addPlan(time, best);
      } else {
        best = best.copy(best.getTraitSet(), inputRets);
      }
      return best;
    }

    @Override
    final RelNode materialize(RelSubset subset, OptImpl optImpl, int matTime, MatType matType, RelNode nonMatRet) {
      if (isVirtualTableSink(nonMatRet)) {
        return nonMatRet;
      }

      String projectName = TvrTableUtils.getProjectName(ctx);
      String tableName = TvrTableUtils.getStateTableName(ctx, subset.getId(), matType);
      Map<String, String> parameters = new HashMap<>();

      boolean createTable = true;
//      if (isRangeQuery) {
//        String digest = planDigestVisitor.getTableDigest(subset, optImpl, matType);
//        String[] stateTableName = getStateTableForRangeQuery(digest);
//        if (stateTableName != null) {
//          // this state has already been saved before
//          projectName = stateTableName[0];
//          tableName = stateTableName[1];
//          createTable = false;
//        }
//        parameters.put(ODPS_PROGRESSIVE_TABLE_DIGEST, digest);
//        rangeQueryStateTables.put(digest, projectName + "." + tableName);
//      }

      RelNode sink = buildMatSink(nonMatRet, matType, subset.getTraitSet(), projectName,
          tableName, getRowCount(memo, subset), parameters);
      if (createTable) {
        execPlanManager.addPlan(matTime, sink);
        LOG.info("try to create new state table " + projectName + "." + tableName);
      }
      return sink;
    }

    @Override
    final RelNode materialize(RelSubset subset, OptImpl optImpl, MatType matType, List<RelNode> inputs) {
      switch (matType) {
      case MAT_MEMORY:
      case MAT_DISK:
        if (isVirtualTableSink(optImpl.best)) {
          return optImpl.best;
        }

        // this table doesn't really exist,
        // when it is used, it will be replaced by a union with several soft-link tables.
        EnumerableValues values = EnumerableValues
            .create(subset.getCluster(), subset.getRowType(), ImmutableList.of());
        String projectName = TvrTableUtils.getProjectName(ctx);
        String tableName = TvrTableUtils.getStateTableName(ctx, subset.getId(), matType);
        PhysicalTableSink sink = (PhysicalTableSink) buildMatSink(values, matType, subset.getTraitSet(), projectName,
            tableName, getRowCount(memo, subset), Collections.emptyMap());
        execPlanManager.addSoftLinkPlan(sink,
            inputs.stream().map(r -> (PhysicalTableSink) r)
                .collect(Collectors.toSet()));
        return sink;
      default:
        throw new UnsupportedOperationException("Unknown MatType: " + matType);
      }
    }

    @Override
    RelNode reuse(RelSubset subset, MatType matType, int whenUsed, RelNode matRet) {
      if (isVirtualTableSink(matRet)) {
        return matRet;
      }

      assert matRet instanceof PhysicalTableSink;
      RelOptCluster cluster = subset.getCluster();
      List<PhysicalTableSink> srcTables =
          execPlanManager.getLinkedSrcSinks((PhysicalTableSink) matRet);
      assert srcTables.size() >= 1;
      List<RelNode> scans = srcTables.stream()
          .map(t -> EnumerableTableScan.create(cluster, t.getTable()))
          .collect(Collectors.toList());
      RelTraitSet traitSet = subset.getTraitSet();
      return scans.size() == 1 ?
          scans.get(0) :
          new EnumerableUnion(scans.get(0).getCluster(), scans.get(0).getTraitSet(), scans, true);
    }

    private void addSinksForDownStream() {
      // original table name -> serialized tvr relationship graph
      Map<String, String> outputTablesInfo = new HashMap<>();

      // Save only one type of table (disk, memory) per subset for downstream
      // <Subset, <list of the linked table sink, best MatType>>
      Map<RelSubset, Pair<List<PhysicalTableSink>, MatType>> targets = new IdentityHashMap<>();
      // indicate the subset that this table sink is built for
      Map<PhysicalTableSink, RelSubset> sinkMapping = new IdentityHashMap<>();

      // convert 'materialized' (<EnumMap<MatType, Map<RelSubset, T>>) to
      //  <RelSubset, EnumMap<MatType, RelNode>>
      //  so that it can be used to find the optimal storage medium type for each RelSubset.
      Map<RelSubset, EnumMap<MatType, RelNode>> matMap = new IdentityHashMap<>();
      materialized.forEach((matType, map) -> map.forEach((subset, relNode) -> {
        // skip virtual table sink
        if (isVirtualTableSink(subset)) {
          return;
        }

        EnumMap<MatType, RelNode> m = matMap.computeIfAbsent(subset, k -> new EnumMap<>(MatType.class));
        m.put(matType, relNode);
      }));

      matMap.forEach((subset, enumMap) -> {
        MatType matType = MatType.best(enumMap.keySet());
        RelNode matSink = Objects.requireNonNull(materialized.get(matType).get(subset));
        sinkMapping.put((PhysicalTableSink) matSink, subset);
        targets.put(subset, Pair.of(
            execPlanManager.getLinkedSrcSinks((PhysicalTableSink) matSink),
            matType));
      });

      /*
       * Start walking the tvr relationship map from original input tvr of each
       * original sink, and find the set of materialized subsets reachable.
       * Build a new tableSink for each found subset for each original sink.
       *
       * Note that:
       * inputTvr - oriSink: 1 to n mapping
       * related Subset - inputTvr: m to n mapping
       */
      planner.getInputTvrToSinksMap().entries().forEach(e -> {
        TvrMetaSet inputTvr = e.getKey().getActiveTvr();
        // Find the corresponding physical original sink
        PhysicalTableSink oriPhySink =
            (PhysicalTableSink) planner.getSet(e.getValue())
                .getRelsFromAllSubsets().stream()
                .filter(r -> r instanceof PhysicalTableSink).findFirst()
                .orElse(null);
        assert oriPhySink != null;

        // One graph constructor per <inputTvr, oriSink> pair
        TvrGraphConstructor tvrGraph =
            new TvrGraphConstructor(inputTvr, oriPhySink, targets, sinkMapping);
        TvrNode graphRoot = tvrGraph.buildTvrGraph();

        tvrGraph.newSinks.forEach((subset, newSink) -> execPlanManager
            .addHardLinkPlan(memo.get(subset).time(), newSink.left,
                ImmutableSet.of(newSink.right)));

        String str = TvrTableInfoForMeta
            .serialize(new TvrTableInfoForMeta(oriPhySink, graphRoot));
        String tableName = TvrTableUtils.getNormalizedTableName(oriPhySink);
        String existingTable = outputTablesInfo.put(tableName, str);
        assert existingTable == null;
        graphRoot.printGraph(tableName);
      });
      // Save the graph for downstream in real run
      TvrContext odpsRelContext =
              TvrContext.getInstance(root.getCluster());
//      odpsRelContext.getQueryContext().setOutputTableMapsStr(outputTablesInfo);

      // Save the graph for downstream in unit test
      String outputTableTvrInfoStr = new Gson().toJson(outputTablesInfo);
      odpsRelContext.getConfig()
          .set(TvrTableUtils.TvrTableRelation.CONFIG_OUTPUT_TVR_RELATED_TABLE_MAPS,
              outputTableTvrInfoStr);
    }
  }

  class ReuseCostVisitor extends BestPlanVisitor<RelOptCost> {
    private TRelOptCost cost = makeCost(costFactory.makeZeroCost(), 0);
    private final SetMultimap<RelSubset, MatType> mat = Multimaps.newSetMultimap(
        new IdentityHashMap<>(), () -> EnumSet.noneOf(MatType.class));

    @Override
    RelOptCost visit(RelSubset subset, OptImpl optImpl, List<RelOptCost> inputRets) {
      return costFactory.makeZeroCost();
    }

    @Override
    RelOptCost materialize(RelSubset subset, OptImpl optImpl, int matTime,
        MatType matType, RelOptCost nonMatRet) {
      mat.put(subset, matType);
      return costFactory.makeZeroCost();
    }

    @Override
    RelOptCost materialize(RelSubset subset, OptImpl optImpl, MatType matType, List<RelOptCost> inputs) {
      mat.put(subset, matType);
      return costFactory.makeZeroCost();
    }

    @Override
    RelOptCost reuse(RelSubset subset, MatType matType, int whenUsed, RelOptCost matRet) {
      this.cost = this.cost.pointPlusInPlace(makeReadIoCost(subset, matType), whenUsed);
      return costFactory.makeZeroCost();
    }

    public TRelOptCost getCost() {
      return cost;
    }

    public Set<MatType> getRealMatTypes(RelSubset subset) {
      return mat.get(subset);
    }
  }

//  class PlanDigestVisitor extends BestPlanVisitor<JsonElement> {
//
//    String getTableDigest(RelSubset subset, OptImpl optImpl, MatType matType) {
//      return jsonElementToStr(matRel(subset, optImpl, matType, matType2UseType(matType)));
//    }
//
//    @Override
//    JsonElement visit(RelSubset subset, OptImpl optImpl, List<JsonElement> inputRets) {
//      JsonObject jsonObject = new JsonObject();
//
//      JsonArray inputJsonArray = new JsonArray();
//      inputRets.forEach(inputJsonArray::add);
//      jsonObject.add("inputs", inputJsonArray);
//
//      JsonObject bestDigest = new JsonObject();
//      RelNode best = optImpl.best;
//      bestDigest.addProperty("node", best.getDigest());
//      if (best instanceof TableScan) {
//        RelOptTable table = (RelOptTable) best.getTable();
//        if (table.getTable().isPartitioned()) {
//          JsonArray partitionJsonArray = new JsonArray();
//          List<RelOptPartition> relOptPartitions = table.getQualifiedPartitions();
//          List<Partition> partitions;
//          if (relOptPartitions != null) {
//            partitions = relOptPartitions.stream().map(p -> p.getPartition().getInnerPartition())
//                .collect(Collectors.toList());
//          } else {
//            partitions = table.getTable().getPartitions();
//          }
//
//          for (Partition p : partitions) {
//            String partitionDigest = p.getPartSpecStr() + " (" + p.getLastModifiedTime() + ")";
//            partitionJsonArray.add(partitionDigest);
//          }
//          bestDigest.add("partitions", partitionJsonArray);
//        } else {
//          bestDigest.addProperty("tableLastModifiedTime",
//              table.getTable().getTableInternal().getLastModifiedTime()
//                  .toString());
//        }
//      }
//
//      jsonObject.add("digest", bestDigest);
//      return jsonObject;
//    }
//
//    @Override
//    JsonElement materialize(RelSubset subset, OptImpl optImpl, int matTime,
//        MatType matType, JsonElement nonMatRet) {
//      JsonObject jsonObject = new JsonObject();
//      jsonObject.addProperty("matType", matType.name());
//      jsonObject.add("input", nonMatRet);
//      return jsonObject;
//    }
//
//    @Override
//    JsonElement materialize(RelSubset subset, OptImpl optImpl, MatType matType,
//        List<JsonElement> inputRets) {
//      JsonObject jsonObject = new JsonObject();
//      jsonObject.addProperty("matType", matType.name());
//
//      JsonArray inputJsonArray = new JsonArray();
//      inputRets.forEach(inputJsonArray::add);
//      jsonObject.add("softLinkInputs", inputJsonArray);
//      return jsonObject;
//    }
//
//    @Override
//    JsonElement reuse(RelSubset subset, MatType matType, int whenUsed,
//        JsonElement matRet) {
//      JsonObject jsonObject = new JsonObject();
//      jsonObject.add("reuseNode", matRet);
//      return jsonObject;
//    }
//
//    private String jsonElementToStr(JsonElement jsonElement) {
//      // ignore node id
//      return jsonElement.toString().replaceAll("#\\d+", "");
//    }
//  }

  private TRelOptCost settleCost(TRelOptCost c) {
    return new TRelOptCost(costFactory.makeZeroCost(), c.earliest, c.fix());
  }

  private TRelOptCost settleCost(TRelOptCost c, int time) {
    return new TRelOptCost(costFactory.makeZeroCost(), c.earliest, c.fix(time));
  }

  private TRelOptCost makeCost(RelOptCost floating, int earliest) {
    RelOptCost[] fixed = new RelOptCost[lastTime + 1];
    Arrays.fill(fixed, costFactory.makeZeroCost());
    return new TRelOptCost(floating, earliest, fixed);
  }
}
