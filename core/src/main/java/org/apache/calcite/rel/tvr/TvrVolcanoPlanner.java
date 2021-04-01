package org.apache.calcite.rel.tvr;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.apache.calcite.adapter.enumerable.EnumerableFilterRule;
import org.apache.calcite.adapter.enumerable.EnumerableFilterToCalcRule;
import org.apache.calcite.adapter.enumerable.EnumerableTableScanRule;
import org.apache.calcite.adapter.enumerable.EnumerableValuesRule;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.*;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.TableSink;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.tvr.rels.EnumerablePhysicalVirtualRoot;
import org.apache.calcite.rel.tvr.rels.TvrExecPlansOp;
import org.apache.calcite.rel.tvr.rules.TvrRuleCollection;
import org.apache.calcite.rel.tvr.rules.operators.TvrDeduperApplyRule;
import org.apache.calcite.rel.tvr.rules.operators.TvrTableScanRule;
import org.apache.calcite.rel.tvr.rules.operators.TvrValuesRule;
import org.apache.calcite.rel.tvr.utils.*;
import org.apache.calcite.tools.visualizer.VolcanoRuleMatchVisualizerListener;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.apache.calcite.rel.tvr.utils.TvrUtils.*;


/**
 * Odps implementation of Volcano.
 */
public class TvrVolcanoPlanner extends VolcanoPlanner {

  private static final Log LOG = LogFactory.getLog(TvrVolcanoPlanner.class);
  RelOptCost zeroCost = null;
  RelOptCost infiniteCost = null;
  MqcOptimizer opt;
  // Original tvr of input RelSet of sinks -> original sinks
  Multimap<TvrMetaSet, TableSink> inputTvrToSinks = null;

  public TvrVolcanoPlanner(RelOptCostFactory costFactory, Context context) {
    super(costFactory, context);
    zeroCost = this.getCostFactory().makeZeroCost();
    infiniteCost = this.getCostFactory().makeInfiniteCost();
  }

  ThreadLocal<Long> lastTime = new ThreadLocal<Long>();

  public Multimap<TvrMetaSet, TableSink> getInputTvrToSinksMap() {
    return inputTvrToSinks;
  }

  @Override
  protected VolcanoPlannerPhaseRuleMappingInitializer getPhaseRuleMappingInitializer() {
    return phaseRuleMapping -> {

      List<String> nonTvrRules =
              TvrRuleCollection.tvrStandardRuleSet().stream().filter(
              r -> !TvrRuleCollection.RuleConfig.PROGRESSIVE.getRuleSet().contains(r)
                  && !TvrRuleCollection.RuleConfig.PROGRESSIVE_OUTER_JOIN_VIEW.getRuleSet().contains(r)
                  && !TvrRuleCollection.RuleConfig.PROGRESSIVE_STREAMING.getRuleSet().contains(r)
                  && !TvrRuleCollection.RuleConfig.PROGRESSIVE_DBTOASTER.getRuleSet().contains(r)
                  && !TvrRuleCollection.RuleConfig.PROGRESSIVE_COPY.getRuleSet().contains(r))
              .map(RelOptRule::toString)
              .collect(Collectors.toList());

      List<String> pruneRules =
              TvrRuleCollection.RuleConfig.PE.getRuleSet().stream()
              .map(RelOptRule::toString).collect(Collectors.toList());

      List<String> tvrRules =
              TvrRuleCollection.RuleConfig.PROGRESSIVE.getRuleSet().stream()
              .map(RelOptRule::toString).collect(Collectors.toList());

      List<String> tvrCopyRules =
              TvrRuleCollection.RuleConfig.PROGRESSIVE_COPY.getRuleSet().stream()
              .map(RelOptRule::toString).collect(Collectors.toList());

      List<String> tvrStreamingRules =
              TvrRuleCollection.RuleConfig.PROGRESSIVE_STREAMING.getRuleSet().stream()
              .map(RelOptRule::toString).collect(Collectors.toList());

      List<String> tvrDBToasterRules =
              TvrRuleCollection.RuleConfig.PROGRESSIVE_DBTOASTER.getRuleSet().stream()
              .map(RelOptRule::toString).collect(Collectors.toList());

      List<String> tvrOJVRules =
              TvrRuleCollection.RuleConfig.PROGRESSIVE_OUTER_JOIN_VIEW.getRuleSet()
              .stream().map(RelOptRule::toString).collect(Collectors.toList());

      List<String> tvrFlatteningRules =
              TvrRuleCollection.RuleConfig.PROGRESSIVE_FLATTENING.getRuleSet()
              .stream().map(RelOptRule::toString).collect(Collectors.toList());

      Set<String> optimize = phaseRuleMapping.get(VolcanoPlannerPhase.OPTIMIZE);
      optimize.addAll(nonTvrRules);
      optimize.addAll(tvrRules);
      optimize.addAll(tvrFlatteningRules);
      optimize.addAll(pruneRules);
      optimize.addAll(tvrStreamingRules);
      optimize.addAll(tvrDBToasterRules);
      optimize.addAll(tvrOJVRules);

      TvrContext ctx = context.unwrap(TvrContext.class);
      if (progressiveTranslationSymmetryEnabled(ctx)) {
        // 1. In the OPTIMIZE phase, all rules except the empty pruning rules will be triggered to
        //    build the sample plans, and the translation symmetry will refer to these sample plans
        //    to generate any other plans with different TVR versions.
        optimize.remove(TvrTableScanRule.INSTANCE.toString());
        optimize.remove(TvrValuesRule.INSTANCE.toString());
        optimize.removeAll(pruneRules);

        // 2. In the COPY phase, all the copy rules will be triggered to build
        //    the input nodes with different TVR versions. The entire plans at
        //    each time point will be generated from these input nodes by
        //    referring to the sample plans.
        // NOTE: empty pruning rules can still not be fired now to avoid pruning the sample plans.
        Set<String> copy = phaseRuleMapping.get(VolcanoPlannerPhase.COPY);
        copy.addAll(tvrCopyRules);
        copy.add(new EnumerableTableScanRule(RelFactories.LOGICAL_BUILDER).toString());
        copy.add(new EnumerableValuesRule(RelFactories.LOGICAL_BUILDER).toString());
//        copy.remove(TvrTableConsolidationRule.INSTANCE.toString());

        // 3. In the PRE-PROCESS phase:
        //    a. TvrTableConsolidationRule will be fired here to apply the
        //       time interval of each partition table to build the real qualified partitions.
        //    b. Empty pruning rules will be fired here to remove all the empty table scans
        //       and prune empty plans, and physical values build rule will be triggered to
        //       convert the new logical values created by pruning rules.
        //    c. Enforcer will be fired here to create the missing distributions
        //       and collations in the current memo.
        //    d. Flatten the adjacent dedupers.
        Set<String> preProcess = phaseRuleMapping.get(VolcanoPlannerPhase.PRE_POST);
//        preProcess.add(TvrTableConsolidationRule.INSTANCE.toString());
        preProcess.add(new EnumerableTableScanRule(RelFactories.LOGICAL_BUILDER).toString());
//        preProcess.add(Enforcer.INSTANCE.toString());
        preProcess.add(new EnumerableValuesRule(RelFactories.LOGICAL_BUILDER).toString());
        preProcess.addAll(pruneRules);

        preProcess.addAll(tvrFlatteningRules);
        preProcess.add(TvrDeduperApplyRule.INSTANCE.toString());
        // convert logical nodes in a Tvr Deduper to the physical nodes.
//        preProcess.add(GreedyWindowOrderingRule.INSTANCE.toString());
        preProcess.add(new EnumerableFilterRule().toString());
//        preProcess.add(((RelOptRule) ExchangeRules.SORT).toString());
        preProcess.add(new EnumerableFilterToCalcRule(RelFactories.LOGICAL_BUILDER).toString());

        // 4. In the POST-PROCESS phase, do nothing.
        Set<String> postProcess = phaseRuleMapping.get(VolcanoPlannerPhase.POST_PROCESS);
        postProcess.add("xxx");

        // 5. In the CLEANUP phase, do nothing.
        Set<String> cleanup = phaseRuleMapping.get(VolcanoPlannerPhase.CLEANUP);
        cleanup.add("xxx");

        // the tvr converter rules need to be delayed until the 'copy' phase is completed.
        // for example, the missing deltas may be created in the 'copy' phase,
        // and it is cheaper than triggering TvrSetSnapshotToSetDeltaRule in 'optimize' phase.
        phasesToDrainTvrConverters = EnumSet.of(VolcanoPlannerPhase.values()[1]);
      } else {
        optimize.remove(TvrTableScanRule.SAMPLE_INSTANCE.toString());
        optimize.remove(TvrValuesRule.SAMPLE_INSTANCE.toString());
        Arrays.stream(VolcanoPlannerPhase.values()).skip(1)
            .forEach(phase -> phaseRuleMapping.get(phase).add("xxx"));
      }
    };
  }

  /**
   * The method will be called when invoking findBestExp, at fireRule and
   * a rule match found. We check timed out and try best to get a plan here.
   */
  @Override public void checkCancel() {
    if (lastTime.get() == null){
      lastTime.set(System.currentTimeMillis());
    }

//    TvrContext context = TvrContext.getInstance(getRoot().getCluster());
//    Long[] now = {0L};
//    if (context.getTimer().timedout(now)) {
//      RelNode cheapest = null;
//      if (super.root.getBest() != null) {
//        cheapest = VolcanoPlannerUtil.buildCheapestPlan(this, super.root);
//      }
//      throw new TimeoutException(cheapest);
//    }
//
//    long delta = now[0] - lastTime.get();
//    if (delta < 0 || delta > 5000){
//      LOG.info(String.format("delta %d, report %s", delta,
//          context.getListener().report()));
//      lastTime.set(now[0]);
//    }
  }

  /**
   * Set default RelTraitSet
   * And for collation is OdpsRelCollationImp
   * If we don't overwrite, then there are both RelCollationImp and OdpsRelCollationImp
   * and for every relNode may exists same relNode except collation.
   * when merge subset, it assert equivSubset.set != subset.set;
   * @return
   */
  @Override public RelTraitSet emptyTraitSet() {
    RelTraitSet traitSet = super.emptyTraitSet();
    for (RelTraitDef traitDef : super.getRelTraitDefs()) {
      if (traitDef instanceof RelCollationTraitDef) {
        traitSet = traitSet.plus(RelCollations.EMPTY);
      } else {
        traitSet = traitSet.plus(traitDef.getDefault());
      }
    }
    return traitSet;
  }

  @Override
  public RelNode findBestExp() {
    TvrContext ctx = TvrContext.getInstance(this.root.getCluster());
    Config optimizerConfig = ctx.getConfig();

    final boolean enableVisualizer =
        optimizerConfig.getBool(TvrUtils.ENABLE_VOLCANO_VISUALIZER, false);
    VolcanoRuleMatchVisualizer visualizer = null;
    if (enableVisualizer) {
      VolcanoRuleMatchVisualizerListener volcanoRuleMatchVisualizerListener =
          new VolcanoRuleMatchVisualizerListener(this);
      this.getListener().addListener(volcanoRuleMatchVisualizerListener);
      visualizer = volcanoRuleMatchVisualizerListener.getVisualizer();
      visualizer.discardIfNoChange = false;
    }
    final boolean enableLogger =
        optimizerConfig.getBool(TvrUtils.ENABLE_VOLCANO_LOGGER, false);
    VolcanoRuleMatchLogger logger;
    if (enableLogger) {
      logger = new VolcanoRuleMatchLogger();
      this.getListener().addListener(logger);
    }

    // Only init the first time, skip for re-optimize
    if (inputTvrToSinks == null) {
      inputTvrToSinks = ArrayListMultimap.create();
      allSets.stream().flatMap(set -> set.getRelsFromAllSubsets().stream())
          .filter(rel -> rel instanceof TableSink && !TvrUtils.isVirtualTableSink(rel))
          .forEach(sink -> {
        TvrMetaSet inputTvr =
            getSet(sink.getInput(0)).getTvrForTvrSet(ctx.getDefaultTvrType());
        inputTvrToSinks.put(inputTvr, (TableSink) sink);
      });
    }

//    // Start: Added this place to support oracle estimator
//    if (optimizerConfig.get("odps.optimizer.oracle.enable", "false")
//        .equals("true") && oraclePlanner == null) {
//      String basePath = optimizerConfig.get("odps.optimizer.oracle.basePath");
//      String queryName = optimizerConfig.get("odps.optimizer.oracle.queryName");
//      oraclePlanner = new OdpsOraclePlanner(basePath, queryName, this);
//    }

    // End
    RelNode best;

    Predicate<? super Entry<RelSubset, Integer>> candPrunePred = e -> true;
    if (TvrUtils.progressivePruneReuseCandidate(ctx)) {
      final Set<RelSet> reuseCandidateRelSet =
          ProgressiveExperimentUtil.getReuseRelSet(this);
      candPrunePred = e -> e.getKey().getTvrLinks().values().stream()
          .flatMap(metaSet -> {
            TvrUtils.TvrMetaSetVisitor visitor =
                new TvrUtils.TvrMetaSetVisitor();
            visitor.go(metaSet);
            return visitor.metaSets.stream();
          }).map(metaSet -> metaSet.getRelSet(TvrSemantics.SET_SNAPSHOT_MAX))
          .filter(Objects::nonNull).anyMatch(reuseCandidateRelSet::contains);
    }

    ProgressiveMetrics pm = ctx.getProgressiveMetrics();
    try {
      best = super.findBestExp();
    } catch (Throwable e) {
      if (enableVisualizer) {
        visualizer.addFinalPlan();
        visualizer.writeToFile("./volcano-viz", null);
      }
      if (progressivePrintJoins(ctx)) {
        VolcanoPlannerUtil.printAllJoins(this);
      }
      pm.endFindBestExp();

      VolcanoPlannerUtil.recordMemoMetrics(this, pm);
      String metricsSummary = pm.summarize(optimizerConfig);
//      ctx.getQueryContext().setMetricsSummary(metricsSummary);
      throw e;
    }

//    if (progressiveAccelerateDataInsertion(ctx)) {
//      return accelerateDataInsertion(best);
//    }

    pm.endFindBestExp();
    VolcanoPlannerUtil.recordMemoMetrics(this, pm);
    pm.fetchRuleMatchData(ctx.getListener());
    LOG.info("Total time in findBestExp: " + pm.getFindBestExpMillis()
        + ". Total rules fired: " + pm.getRulesFired());
    if (progressivePrintJoins(ctx)) {
      VolcanoPlannerUtil.printAllJoins(this);
    }

//    try {
//      if (optimizerConfig.getBool(Constants.PLAN_VALIDATE, true)) {
//        isValid(Litmus.THROW);
//      }
//    } catch (AssertionError e) {
//      optimizerConfig.set(Constants.PLAN_VALIDATE, false);
//    }

    // Initialize/clear tvr cost results
//    ctx.getQueryContext().initTvrCost();

    if (progressiveEnabled(ctx)) {
      Map<RelSubset, Integer> execTime =
          new VolcanoMemoTimeVisitor(this).assignTime(root);


      List<double[]> costWeights = TvrUtils.getCostWeights(ctx);

      // Multi-query optimization searching for the optimal cross-time plan
      try {
        if (costWeights == null || costWeights.isEmpty()) {
          best = runMqc(ctx, pm, visualizer, execTime, candPrunePred);
        } else {
          // configured to use (multiple sets of) weights
          for (double[] weights : costWeights) {
            assert weights.length == ctx.getTvrVersions().length;
            TRelOptCost.weights = weights;

            best = runMqc(ctx, pm, visualizer, execTime, candPrunePred);
          }
        }
      } catch (Throwable e) {
        if (enableVisualizer) {
          visualizer.addFinalPlan();
          visualizer.writeToFile("./volcano-viz", null);
        }
        pm.summarize(optimizerConfig);
        throw e;
      }

      TRelOptCost.weights = null;

    } else {
      dumpNonTvrCost(best);
    }

//    String metricsSummary = pm.summarize(optimizerConfig);
//    ctx.getQueryContext().setMetricsSummary(metricsSummary);

    if (enableVisualizer) {
      visualizer.addFinalPlan();
      visualizer.writeToFile("./volcano-viz", null);
    }

//    // Start: Added this place to support oracle estimator
//    if (optimizerConfig.get("odps.optimizer.oracle.enable", "false")
//        .equals("true") && oraclePlanner != null) {
//      oraclePlanner.generateDigestSqlMap();
//      oraclePlanner.serializeDigestSqlMap();
//    }
    // End
    return best;
  }

  // Multi query optimization. This is according to the algorithm in
  // 'P. Roy, S. Sehadri, S. Sudarshan, and S. Bhobe. Efficient and
  // Extensible Algorithms for Multi Query Optimization. In SIGMOD 2000.'
  private RelNode runMqc(TvrContext ctx, ProgressiveMetrics pm,
      VolcanoRuleMatchVisualizer visualizer, Map<RelSubset, Integer> execTime,
      Predicate<? super Entry<RelSubset, Integer>> candPrunePred) {

    pm.startMultiQueryOpt();

    ctx.loadJoinHugeCost();

    // We simply ignore the optimal plan returned by findBestExp,
    // and directly work on the memo built by the volcano planner.
    final boolean matAcrossTvr = progressiveMatAcrossTvrEnabled(ctx);
    if (opt == null) {
      opt = new MqcOptimizer(ctx, this, root, execTime, matAcrossTvr);
    } else {
      // TODO: cleanup
      opt = opt.reset(root, execTime);
    }

    // for experiments
    Set<Integer> printSets = TvrUtils.progressivePrintRelSets(ctx);
    if (printSets != null) {
      System.out.println("Printing selected RelSets");
      System.out.println(VolcanoPlannerUtil.printRelSets(allSets, printSets));
    }

    // load materialization subsets from config
    Pair<Map<RelSubset, MatType>, Map<RelSubset, MatType>> matSubsets =
        getMatSubsetFromConf(ctx);
    Map<RelSubset, MatType> forceMatSubsets = matSubsets.left;
    Map<RelSubset, MatType> optMatSubsets= matSubsets.right;

    // force materialization
    if (forceMatSubsets != null) {
      forceMatSubsets.forEach((subset, matType) -> {
        opt.tryToMat(ImmutableSet.of(subset), matType, false);
        LOG.info("force committing " + subset + " with " + matType);
        opt.commit();
      });
    }

    // mark all inputs of TableSink as directly materialized and commit
    collectSinks(root).stream()
        .filter(r -> !r.getConvention().equals(Convention.NONE))
        .forEach(sink -> {
          RelSubset sinkInput = (RelSubset) sink.getInput(0);
          if (sink instanceof TableSink) {
            opt.tryToMat(ImmutableSet.of(sinkInput), MatType.MAT_DISK, false);
            LOG.info("committing " + sinkInput + " with " + MatType.MAT_DISK);
          }
          opt.commit();
        });

    // find the reuse candidates
    SubsetReuseStatsCollector reuseCollector = new SubsetReuseStatsCollector(root);
    Set<ReuseCandidate> reuseCandidates;
    Map<RelSubset, MatType> matTypeMapping;
    if (optMatSubsets != null) {
      reuseCollector.addPruningPredicate(p -> optMatSubsets.containsKey(p.getKey()));
      reuseCandidates = reuseCollector.getReuseCandidates(ctx, opt);
      matTypeMapping = optMatSubsets;
    } else {
      final int lastTime = execTime.get(root);
      reuseCollector.addPruningPredicate(candPrunePred)
          .addPruningPredicate(p -> p.getValue() > 1 || execTime.get(p.getKey()) < lastTime)
          // filter out empty subsets
          .addPruningPredicate(e -> StreamSupport
              .stream(e.getKey().getRels().spliterator(), false)
              .anyMatch(relNode -> getSubset(relNode) == e.getKey()))
          // do not materialize the subset with empty row type
          .addPruningPredicate(e -> e.getKey().getRowType().getFieldCount() > 0)
//          // do not keep subset with broadcast distribution
//          .addPruningPredicate(e -> {
//            RelDistribution distribution = e.getKey().getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
//            return distribution != null && distribution .getType() != RelDistribution.Type.BROADCAST_DISTRIBUTED;
//          })
          // only keep subsets with TVR link
          .addPruningPredicate(e -> e.getKey().getTvrLinks() != null && !e.getKey().getTvrLinks().isEmpty());
      int currentOptimizationRound = ctx.getCurrentOptimizationRound();
      if (currentOptimizationRound != 0) {
        // for re-optimize
        // reuse candidate set cannot contain the subsets with past execution time
        final Set<RelSubset> validRelSubsets = execTime.entrySet().stream()
            .filter(e -> e.getValue() >= currentOptimizationRound)
            .map(Entry::getKey).collect(Collectors.toSet());
        reuseCollector.addPruningPredicate(e -> validRelSubsets.contains(e.getKey()));
        reuseCandidates = reuseCollector.getReuseCandidates(ctx, opt);
      } else {
        reuseCandidates = reuseCollector.getReuseCandidates(ctx, opt);
      }
      matTypeMapping = Collections.emptyMap();
    }
    pm.setReuseCandidateNum(
        (int) reuseCandidates.stream().map(c -> c.getCandidates().size()).count());

    // looking for the best materialization combination
    RelOptCost bestCost = opt.getOverallCost();
    PriorityQueue<ReuseCandidate> heap =
        new PriorityQueue<>(ReuseCandidate.PLAN_COST_COMPARATOR);
    heap.addAll(reuseCandidates);

    LOG.info("mqc while loop starts");
    ReuseCandidate poll;
    while ((poll = heap.poll()) != null) {
      Set<RelSubset> candidate = poll.getCandidates();
      assert !candidate.isEmpty();

      // By default, all candidates are tried to save in memory
      MatType matType = TvrUtils.progressiveCacheEnabled(ctx) ?
          MatType.MAT_MEMORY : MatType.MAT_DISK;
      matType =
          candidate.stream().map(matTypeMapping::get).filter(Objects::nonNull)
              .findFirst().orElse(matType);
      LOG.info("tryToMat " + matType + " for " + candidate);
      boolean suc = opt.tryToMat(candidate, matType, matAcrossTvr);
      if (!suc) {
        LOG.info("tryToMat failed");
        continue;
      }

      RelOptCost cost = opt.getOverallCost();
      assert !cost.isInfinite();
      if (bestCost.isLe(cost)) {
        opt.rollback();
        LOG.info("rollback because of no benefit");
        continue;
      }

      RelOptCost benefit = bestCost.minus(cost);
      if (heap.isEmpty() || heap.peek().getBenefit().isLe(benefit)) {
        opt.commit();
        bestCost = cost;
        LOG.info("committed");
      } else {
        opt.rollback();
        LOG.info("rollback and will retry later");
        heap.offer(new ReuseCandidate(candidate, benefit));
      }
    }

    ArrayList<RelNode> bestNodes =
        visualizer != null ? new ArrayList<>() : null;
    RelNode best = opt.buildCheapestPlan(bestNodes);

    ProgrExecPlanIterator progrExecPlanIterator = new ProgrExecPlanIterator(best,
            new ProgrExecPlanIterator.ReservedTransformer(this, null));
    List<RelNode> plans = new ArrayList<>();
    while (progrExecPlanIterator.hasNext()) {
      RelNode next = progrExecPlanIterator.next();
      if (next instanceof TvrExecPlansOp) {
        assert next.getInputs().size() == 1;
        plans.add(next.getInput(0));
      } else {
        plans.add(next);
      }
    }
    best = EnumerablePhysicalVirtualRoot.create(plans);

    System.out.println("--- raw best plan ---");
    System.out.println(DotPrinter.getDot(best));

    dumpTvrCost(opt, ctx);

    pm.setMatNum(opt.getMaterialized().size());
    pm.endMultiqueryOpt();
    LOG.info(
        "Total time in multi-query opt: " + pm.getMultiQueryOptMillis());

    if (visualizer != null) {
      Set<RelSubset> chosen = opt.getMaterialized();
      List<RelNode> matToVisualize = new ArrayList<>(chosen);
      chosen.forEach(subset -> matToVisualize.add(opt.getBest(subset)));
      visualizer.addRuleMatch("Materialized-Final", matToVisualize, false);
      visualizer
          .addRuleMatch("MultiQuery-Final", bestNodes, false);
    }
    return best;
  }

  public RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
    assert rel != null : "pre-condition: rel != null";
    if (rel instanceof RelSubset) {
      return super.getCost(rel, mq);
    }
    if (rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE)
        == Convention.NONE) {
      return infiniteCost;
    }
    RelOptCost inputCost = zeroCost;
    for (RelNode input : rel.getInputs()) {
      inputCost = inputCost.plus(getCost(input, mq));
      // As the RelOptCost.isInfinited means the input's cost has not been computed or
      // any attribute of the RelOptCost is null (value overflow).
      // So we use the INFINITY constant to judge whether the input's cost has not been computed.
      if (infiniteCost.equals(inputCost)) {
        return infiniteCost;
      }
    }

    RelOptCost cost = mq.getNonCumulativeCost(rel);
    if (!zeroCost.isLt(cost)) {
      // cost must be positive, so nudge it
      cost = costFactory.makeTinyCost();
    }

    return cost.plus(inputCost);
  }

  /**
   * Dump progressive execution total cost and last time cost to QueryContext.
   * Used by Playback.
   */
  private void dumpTvrCost(MultiQueryCostOptimizer costVisitor, TvrContext ctx) {
//    QueryContext queryContext = ctx.getQueryContext();
//    TvrCost tvrCost = new TvrCost();
//    MqcOptimizer mqc = (MqcOptimizer) costVisitor;
//
//    RelOptCost[] costs = ((TRelOptCost) mqc.getOverallCost()).fix();
//    RelOptCost lastCost = costs[costs.length - 1];
//    RelOptCost totalCost = Arrays.stream(costs).reduce(RelOptCost::plus).get();
//
//    tvrCost.costs = Arrays.stream(costs).map(TvrUtils::makeDumpedCost)
//        .collect(Collectors.toList());
//    tvrCost.totalCost = TvrUtils.makeDumpedCost(totalCost);
//    tvrCost.lastCost = TvrUtils.makeDumpedCost(lastCost);
//
//    if (TRelOptCost.weights != null) {
//      tvrCost.weightedCosts = IntStream.range(0, tvrCost.costs.size())
//          .mapToObj(
//              i -> tvrCost.costs.get(i).Cost * TRelOptCost.weights[i])
//          .collect(Collectors.toList());
//      tvrCost.weightedTotalCost =
//          tvrCost.weightedCosts.stream().mapToDouble(d -> d).sum();
//    }
//    queryContext.addTvrCost(tvrCost);
  }

  private void dumpNonTvrCost(RelNode best) {
//    TvrContext ctx = TvrContext.getInstance(best.getCluster());
//    QueryContext queryContext = ctx.getQueryContext();
//
//    RelOptCost totalCost =
//        this.getCost(best, best.getCluster().getMetadataQuery());
//
//    TvrCost tvrCost = new TvrCost();
//    tvrCost.totalCost = TvrUtils.makeDumpedCost(totalCost);
//    tvrCost.lastCost = TvrUtils.makeDumpedCost(totalCost);
//    tvrCost.rowCount =
//        best.getCluster().getMetadataQuery().getRowCount(best);
//    queryContext.addTvrCost(tvrCost);
  }

  private Pair<Map<RelSubset, MatType>, Map<RelSubset, MatType>> getMatSubsetFromConf(
      TvrContext ctx) {
    String forceMatStr = ctx.getConfig()
        .get(TvrUtils.PROGRESSIVE_FORCE_MATERIALIZATION, null);
    String optMatStr = ctx.getConfig()
        .get(TvrUtils.PROGRESSIVE_OPT_MATERIALIZATION, null);

    Map<RelSubset, MatType> forceMap = null, optMap = null;
    if (forceMatStr != null || optMatStr != null) {
      Map<Integer, RelSubset> id2SubsetMapping = this.allSets.stream().flatMap(
          relSet -> relSet.getSubsets().stream()
              .map(subset -> Pair.of(subset.getId(), subset)))
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

      if (forceMatStr != null) {
        forceMap = TvrUtils.parseMatString(forceMatStr, id2SubsetMapping);
      }
      if (optMatStr != null) {
        optMap = TvrUtils.parseMatString(optMatStr, id2SubsetMapping);
      }
    }
    return Pair.of(forceMap, optMap);
  }

  private void resetTimer(TvrContext ctx) {
    lastTime.remove();
  }

  // TODO: reoptimize temporarliy disabled
  public RelNode reoptimize(long newOptimizationTime) {
    TvrContext ctx = TvrContext.getInstance(root.getCluster());

    // FIXME: force downstream query to sleep for 3 seconds in the last round
    //  to wait for the upstream to completely update the meta
    if (ctx.isDownstream() && newOptimizationTime == Long.MAX_VALUE) {
      try {
        LOG.info("sleep 3000");
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    LOG.info(
        "re-optimization start, add new time " + newOptimizationTime + " to "
            + Arrays.toString(ctx.getTvrVersions()));
    // 1. add new time instant & re-init default tvr meta type
    boolean addNew = ctx.addNewVersion(newOptimizationTime);

//    // update all the tables in the memo
//    RelOptCluster cluster = root.getCluster();
//    allSets.forEach(set -> set.getRelsFromAllSubsets().stream().filter(
//        r -> r instanceof TableSink || r instanceof TableScan)
//        .map(r -> (RelOptTableImpl) r.getTable()).forEach(table -> {
//          if (table.interval.to >= ctx.getCurrentOptimizationTime()) {
//            TvrReoptimizationUtils.updateOdpsRelOptTable(table, ctx, cluster);
//          }
//        }));

    // reload latest meta info for input tables written by upstream query
    ctx.reloadUpstreamTablePropertyFromMeta();
    // for downstream query, update version based on upstream query
    boolean addNewUpstream = ctx.updateTvrVersionBasedOnUpstream();
    ctx.updateCurrentOptimizationTimeIndex(newOptimizationTime);
    if (addNew || addNewUpstream || ctx.isDownstream()) {
      // 2. update tvr meta type
      this.allSets.stream().flatMap(set -> set.getTvrLinks().values().stream()
          .map(TvrMetaSet::getTvrType)).distinct().forEach(tvrMetaSetType -> {
        for (long tvrVersion : ctx.getTvrVersions()) {
          tvrMetaSetType.addNewVersion(tvrVersion);
        }
      });

      // 3. re-trigger table scan and values
      TvrMetaSetType newDefaultTvrType = ctx.getDefaultTvrType();
      this.allSets.forEach(set -> {
        // find the original table scans and values
        if (set.getTvrForTvrSet(newDefaultTvrType) == null) {
          return;
        }

        set.getRelsFromAllSubsets().stream().filter(
            rel -> rel instanceof LogicalTableScan
                || rel instanceof LogicalValues)
            .forEach(rel -> fireRules(rel, true));
      });
    }

    // 4. reset timer
    resetTimer(ctx);

    // 5. re-optimize
    return this.findBestExp();
  }

  /************************************************
   * Copy
   ************************************************/
  public TvrCopyHelper copyHelper = null;

  @Override
  protected void postPhase(VolcanoPlannerPhase phase) {
    TvrContext ctx = context.unwrap(TvrContext.class);
    ProgressiveMetrics metrics = ctx.getProgressiveMetrics();

    if (progressiveTranslationSymmetryEnabled(ctx)) {
      switch (phase) {
      case OPTIMIZE:
        copyHelper = new TvrCopyHelper(this, ctx.getProgressiveMetrics());
        break;
      case COPY:
        copyHelper.kickOff();
        break;
      default:
      }
    }
    metrics.endPhase(phase);
  }

  @Override
  protected void prePhase(VolcanoPlannerPhase phase) {
    ProgressiveMetrics metrics =
        context.unwrap(TvrContext.class).getProgressiveMetrics();
    metrics.startPhase(phase);
  }

}
