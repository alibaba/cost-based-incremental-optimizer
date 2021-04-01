package org.apache.calcite.rel.tvr.utils;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrSetSemantics;
import org.apache.calcite.plan.tvr.TvrSetSnapshot;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.*;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.tvr.rels.TvrVirtualSpool;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.calcite.rel.tvr.utils.TvrTableUtils.tableMetaReusable;


public class TvrUtils {

  private static final Log LOG = LogFactory.getLog(TvrUtils.class);

  public static final String PROGRESSIVE_ENABLE =
          "progressive.enable";
  public static final String PROGRESSIVE_INSTANTS =
          "progressive.instants";
  public static final String ENABLE_VOLCANO_VISUALIZER =
          "enable.volcano.visualizer";
  public static final String ENABLE_VOLCANO_LOGGER =
          "enable.volcano.logger";
  public static final String PROGRESSIVE_SUPER_DELTA_ENABLE =
          "progressive.super.delta.enable";
  public static final String PROGRESSIVE_BIG_DELTA_ENABLE =
          "progressive.big.delta.enable";

  public static final String PROGRESSIVE_EXP_DATA_ARRIVAL_INFO =
          "progressive.experiment.arrival";
  public static final String PROGRESSIVE_DIMENSION_TABLES =
          "optimzier.dimension.tables";
  public static final String PROGRESSIVE_LOGICAL_PLAN_ONLY =
          "progressive.logical.plan.only";
  public static final String PROGRESSIVE_USE_CACHE =
          "progressive.use.cache";

  // for progressive aggregate rules,
  //  split-mode: split the final aggregate into two aggregates,
  //  one in the delta phase the other in the merge phase
  public static final String PROGRESSIVE_AGG_SPLIT =
          "progressive.agg.split";
  //  cut-mode: move the final aggregate to the merge phase
  public static final String PROGRESSIVE_AGG_CUT =
          "progressive.agg.cut";
  /**
   * Flag for Progressive Computing to indicate if all relations have NO exactly duplicate tuples.
   * Enabling this flag can optimize the physical execution of ProgressiveMergeDelta operator.
   * Default is false when real execution of the query.
   * When doing experiments (playback, local UT, it's set to true to speed up optimization)
   */
  public static final String PROGRESSIVE_TUPLE_NO_DUPLICATE =
          "progressive.tuple.noduplicate";
  public static final String PROGRESSIVE_META_AVAILABLE =
          "progressive.tvr.meta.available";

  public static final String PROGRESSIVE_METRICS_OUTPUT_ENABLE =
          "optimizer.metrics.output.enable";
  public static final String PROGRESSIVE_METRICS_OUTPUT_PATH =
          "optimizer.metrics.output.path";
  public static final String PROGRESSIVE_METRICS_OUTPUT_PATH_DEFAULT =
          "progressive-metrics.txt";

  public static final String PROGRESSIVE_METRICS_QUERY_NAME =
          "optimizer.metrics.query.name";
  public static final String PROGRESSIVE_PLAYBACK_TIME_LIMIT =
          "optimizer.playback.time.limit";

  public static final String PROGRESSIVE_VALIDATE_PLAN =
          "optimizer.validate.plan";
  public static final String PROGRESSIVE_PLAN_PATH_PREFIX =
          "optimizer.plan.path.prefix";
  public static final String PROGRESSIVE_QUERY_SIGNATURE_KEY =
          "sql.playback.original.signature";
  // Indicate that the table is created to store some of the query's own intermediate states,
  // so that it can be cleaned up accurately.
  public static final String PROGRESSIVE_STATE_TABLE =
          "optimizer.state.table";
  // Indicate which time instant the current running job belongs to.
  public static final String PROGRESSIVE_INSTANT_INDEX =
          "progressive.instant.index";
  // Determine whether a virtual tablesink needs to be added to simulate the downstream query
  public static final String PROGRESSIVE_VIRTUAL_TABLESINK =
          "optimizer.virtual.tablesink";

  public static final String PROGRESSIVE_TRANSLATION_SYMMETRY =
          "optimizer.translation.symmetry";

  // just for experiment
  public static final String PROGRESSIVE_RELSET_ROW_COUNT =
          "optimizer.relset.row.count";

  // indicate whether to convert a simple data insertion plan to a hardlink plan,
  // e.g., INSERT INTO TABLE B SELECT * FROM A    =>    link table A to B
  public static final String PROGRESSIVE_ACCELERATE_DATA_INSERTION =
          "optimizer.accelerate.data.insertion";

  public static final String PROGRESSIVE_PRUNE_REUSE_CANDIDATE =
          "optimizer.prune.reuse.candidate";

  public static final String PROGRESSIVE_FAKE_ROW_COUNT_FILE =
          "optimizer.fake.row.count.file";

  public static final String PROGRESSIVE_FAKE_ROW_COUNT_STR =
          "optimizer.fake.row.count.string";

  public static final String PROGRESSIVE_ENABLE_STREAMING_RULES =
          "progressive.rules.streaming.enable";

  public static final String PROGRESSIVE_ENABLE_DBTOASTER_RULES =
          "progressive.rules.dbtoaster.enable";

  public static final String PROGRESSIVE_ENABLE_OUTERJOINVIEW_RULES =
          "progressive.rules.outerjoinview.enable";

  public static final String PROGRESSIVE_STRICTLY_OUTERJOINVIEW =
          "progressive.rules.outerjoinview.strict";

  public static final String PROGRESSIVE_CONSOLIDATE_IN_ALL_RELSET =
          "progressive.consolidate.in.all.relset";

  public static final String PROGRESSIVE_STRICTLY_DBTOASTER =
          "progressive.rules.dbtoaster.strict";

  public static final String PROGRESSIVE_REQUIRE_OUTPUT_VIEW =
          "progressive.exp.output.view";

  public static final String PROGRESSIVE_COST_WEIGHTS =
          "progressive.cost.weights";

  public static final String PROGRESSIVE_JOIN_HUGE_COST =
          "progressive.join.huge.cost";

  public static final String PROGRESSIVE_FORCE_MATERIALIZATION =
          "progressive.force.materialization";

  public static final String PROGRESSIVE_OPT_MATERIALIZATION =
          "progressive.optional.materialization";

  public static final String PROGRESSIVE_PRINT_RELSET =
          "progressive.print.relset";

  public static final String PROGRESSIVE_PRINT_JOINS =
          "progressive.print.joins";

  public static final String PROGRESSIVE_VIRTUAL_SPOOL_COMPUTE_WEIGHT =
          "progressive.virtual.spool.compute.weight";

  // indicate whether the row count of a table will be estimated with the real metadata.
  // if it is true, the arrival time of the earliest data (it may be the creation time of the table / partition)
  //  and query's end time (set by odps.progressive.end.time) will be used to estimate the amount of future data.
  //  please make sure the time instants (set by odps.progressive.instants)
  //  are set within the interval: [arrival time of the earliest data, end time of the query].
  public static final String PROGRESSIVE_ESTIMATE_ROW_COUNT_WITH_META =
          "progressive.estimate.row.count.with.meta";

  public static final String PROGRESSIVE_RANGE_QUERY_OPTIMIZATION =
          "progressive.range.query.optimization";

  // progressive trigger type : timer or delta
  // timer: generate all plans at once for several given time instants
  // delta: when new data arrives, optimizer will re-optimize again based on the current time
  public static final String PROGRESSIVE_TRIGGER_TYPE =
          "sql.pmctask.trigger.type";

  public static final String PROGRESSIVE_END_TIME =
          "progressive.end.time";

  // indicate whether mqc needs to create derived tables for downstream queries
  // and whether TvrTableScanRule can load tvr relations to reuse derived tables.
  public static final String PROGRESSIVE_DERIVED_TABLE_ENABLE =
          "progressive.derived.table.enable";

  // indicate whether to reduce unions when generating plans
  //        C                          C
  //        |                          |
  //      Union    D                 Union   D
  //      /   \   /                  / | \  /
  //    Union  Union      ======>    A  B Union
  //   /  \    / \                        / \
  //  A    B  E   F                      E   F
  public static final String PROGRESSIVE_REDUCE_UNION_ENABLE =
          "progressive.reduce.union";

  public static final String SCAN_INTERVAL_FROM = "interval.from";
  public static final String SCAN_INTERVAL_TO = "interval.to";

  public static final String MAT_ACROSS_TVR="mat.across.tvr";

  public enum TriggerType {
    none,
    delta,
    timer
  }
  public static final TriggerType DEFAULT_TRIGGER_TYPE = TriggerType.none;

  public static boolean progressiveEnabled(TvrContext ctx) {
    return ctx.getConfig().getBool(PROGRESSIVE_ENABLE, false);
  }

  public static long getProgressiveEndTime(TvrContext ctx) {
    String endTime = ctx.getConfig().get(PROGRESSIVE_END_TIME);
    if (endTime == null) {
      throw new IllegalArgumentException(
              "Please set the end time of this progressive execution (such as 24 o'clock every day)."
                      + " For example, set odps.progressive.end.time=1589558400000;");
      // or set 86400000 for local unit tests
    }
    return Long.parseLong(endTime);
  }

  public static boolean progressiveReoptimizeEnabled(TvrContext ctx) {
    return TriggerType.delta == TriggerType.valueOf(
            ctx.getConfig().get(PROGRESSIVE_TRIGGER_TYPE, DEFAULT_TRIGGER_TYPE.toString()));
  }

  public static boolean progressiveAccelerateDataInsertion(TvrContext ctx) {
    return ctx.getConfig().getBool(PROGRESSIVE_ACCELERATE_DATA_INSERTION, false);
  }

  public static boolean logicalPlanOnly(TvrContext ctx) {
    return ctx.getConfig().getBool(PROGRESSIVE_LOGICAL_PLAN_ONLY, false);
  }

  public static boolean progressiveMetaAvailable(TvrContext ctx) {
    return ctx.getConfig()
            .getBool(TvrUtils.PROGRESSIVE_META_AVAILABLE, true);
  }

  public static boolean progressiveEstimateRowCountWithMeta(TvrContext ctx) {
    return ctx.getConfig()
            .getBool(TvrUtils.PROGRESSIVE_ESTIMATE_ROW_COUNT_WITH_META, false);
  }

  public static boolean progressivePlaybackTimeLimit(TvrContext ctx) {
    return ctx.getConfig().getBool(PROGRESSIVE_PLAYBACK_TIME_LIMIT, false);
  }

  // Some manual rule disabling/enabling are needed to turn this off
  public static boolean progressiveSuperDeltaEnabled(RelOptCluster cluster) {
    return progressiveSuperDeltaEnabled(TvrContext.getInstance(cluster));
  }

  public static boolean progressiveSuperDeltaEnabled(TvrContext ctx) {
    return ctx.getConfig().getBool(PROGRESSIVE_SUPER_DELTA_ENABLE, true);
  }

  public static boolean progressiveBigDeltaEnabled(TvrContext ctx) {
    return ctx.getConfig().getBool(PROGRESSIVE_BIG_DELTA_ENABLE, false);
  }

  public static boolean progressivePlanValidateEnabled(TvrContext ctx) {
    return ctx.getConfig().getBool(PROGRESSIVE_VALIDATE_PLAN, false);
  }

  public static String  progressivePlanPathPrefix(TvrContext ctx) {
    return ctx.getConfig().get(PROGRESSIVE_PLAN_PATH_PREFIX,
            "/progressive/optimizer/plan");
  }

  public static String progressiveQuerySignature(TvrContext ctx) {
    return ctx.getConfig().get(PROGRESSIVE_QUERY_SIGNATURE_KEY, "");
  }

  public static Boolean progressiveCacheEnabled(TvrContext ctx) {
    return ctx.getConfig().getBool(PROGRESSIVE_USE_CACHE, true);
  }

  public static Boolean progressiveMatAcrossTvrEnabled(TvrContext ctx) {
    return ctx.getConfig().getBool(MAT_ACROSS_TVR, false);
  }

  public static Boolean progressiveVirtualTablesinkEnabled(TvrContext ctx) {
    return ctx.getConfig().getBool(PROGRESSIVE_VIRTUAL_TABLESINK, false);
  }

  public static boolean progressiveGetRelsetRowCount(TvrContext ctx) {
    return ctx.getConfig().getBool(PROGRESSIVE_RELSET_ROW_COUNT, false);
  }

  public static boolean progressivePruneReuseCandidate(TvrContext ctx) {
    return ctx.getConfig().getBool(PROGRESSIVE_PRUNE_REUSE_CANDIDATE, false);
  }

  public static boolean progressiveRangeQueryOptEnabled(TvrContext ctx) {
    return ctx.getConfig().getBool(PROGRESSIVE_RANGE_QUERY_OPTIMIZATION, false);
  }

  public static boolean progressiveDerivedTableEnabled(TvrContext ctx) {
    return ctx.getConfig().getBool(PROGRESSIVE_DERIVED_TABLE_ENABLE, true);
  }

  public static boolean progressiveReduceUnionEnabled(TvrContext ctx) {
    return ctx.getConfig().getBool(PROGRESSIVE_REDUCE_UNION_ENABLE, false);
  }

  public static double getVirtualSpoolComputeWeight(TvrContext ctx) {
    return ctx.getConfig()
            .getDouble(PROGRESSIVE_VIRTUAL_SPOOL_COMPUTE_WEIGHT, 200d);
  }

  public static boolean progressiveTranslationSymmetryEnabled(TvrContext ctx) {
    boolean translationSymmetryEnabled =
            ctx.getConfig().getBool(PROGRESSIVE_TRANSLATION_SYMMETRY, false);
    if (translationSymmetryEnabled && (progressiveStreamingRulesEnabled(ctx)
            || progressiveDbtoasterRulesEnabled(ctx)
            || progressiveOuterJoinViewRulesEnabled(ctx))) {
      throw new IllegalArgumentException(
              "To ensure correctness, translation symmetry does not support "
                      + "these incremental methods (Streaming, DBT and OJV) for the time being.");
    }
    return translationSymmetryEnabled;
  }

  public static boolean aggSplitModeOn(Config config) {
    return config.getBool(PROGRESSIVE_AGG_SPLIT, true);
  }

  public static boolean aggCutModeOn(Config config) {
    return config.getBool(PROGRESSIVE_AGG_CUT, true);
  }

  public static String getProgressiveMetricsOutputPath(Config config) {
    return config.get(PROGRESSIVE_METRICS_OUTPUT_PATH,
            PROGRESSIVE_METRICS_OUTPUT_PATH_DEFAULT);
  }

  public static boolean progressiveStreamingRulesEnabled(TvrContext ctx) {
    return ctx.getConfig()
            .getBool(PROGRESSIVE_ENABLE_STREAMING_RULES, true);
  }

  public static boolean progressiveDbtoasterRulesEnabled(TvrContext ctx) {
    return ctx.getConfig()
            .getBool(PROGRESSIVE_ENABLE_DBTOASTER_RULES, false);
  }

  public static boolean progressiveOuterJoinViewRulesEnabled(
          TvrContext ctx) {
    return ctx.getConfig()
            .getBool(PROGRESSIVE_ENABLE_OUTERJOINVIEW_RULES, false);
  }

  public static boolean progressiveOuterJoinViewStrict(TvrContext ctx) {
    boolean ret =
            ctx.getConfig().getBool(PROGRESSIVE_STRICTLY_OUTERJOINVIEW, false);
    assert !ret || progressiveOuterJoinViewRulesEnabled(ctx);
    return ret;
  }

  public static boolean progressiveConsolidateInAllRelset(TvrContext ctx) {
    return ctx.getConfig()
            .getBool(PROGRESSIVE_CONSOLIDATE_IN_ALL_RELSET, false);
  }

  public static boolean progressiveDbToasterStrict(TvrContext ctx) {
    boolean ret =
            ctx.getConfig().getBool(PROGRESSIVE_STRICTLY_DBTOASTER, false);
    return ret && progressiveEnabled(ctx) && progressiveDbtoasterRulesEnabled(ctx) && !ctx.allNonDimTablesSorted().isEmpty();
  }

  public static boolean progressiveRequireOutputView(TvrContext ctx) {
    return ctx.getConfig()
            .getBool(PROGRESSIVE_REQUIRE_OUTPUT_VIEW, false);
  }

  public static Set<Integer> progressiveJoinHugeCost(TvrContext ctx) {
    String relNodeIdBlackList =
            ctx.getConfig().get(PROGRESSIVE_JOIN_HUGE_COST, "");

    return Arrays.stream(relNodeIdBlackList.split(",")).map(String::trim)
            .filter(s -> !s.isEmpty()).mapToInt(Integer::parseInt).boxed()
            .collect(Collectors.toCollection(HashSet::new));
  }

  public static Set<Integer> progressivePrintRelSets(TvrContext ctx) {
    String relSets = ctx.getConfig().get(PROGRESSIVE_PRINT_RELSET, null);
    if (relSets == null) {
      return null;
    }
    return Arrays.stream(relSets.split(",")).map(String::trim)
            .filter(s -> !s.isEmpty()).mapToInt(Integer::parseInt).boxed()
            .collect(Collectors.toCollection(HashSet::new));
  }

  public static boolean progressivePrintJoins(TvrContext ctx) {
    return progressiveEnabled(ctx) && ctx.getConfig()
            .getBool(PROGRESSIVE_PRINT_JOINS, false);
  }

  /**
   * Use one or multiple sets of cost weights for mqc, e.g:
   * 0.3, 0.5, 1; 1, 1, 1;
   */
  public static List<double[]> getCostWeights(TvrContext ctx) {
    String weightsStr = ctx.getConfig().get(PROGRESSIVE_COST_WEIGHTS);
    if (weightsStr == null) {
      return null;
    }
    return Arrays.stream(weightsStr.split(";")).map(String::trim).map(
            str -> Arrays.stream(str.split(",")).map(String::trim)
                    .mapToDouble(Double::parseDouble).toArray())
            .collect(Collectors.toList());
  }

  public static boolean isOdpsLogicalOperator(RelNode rel) {
    return rel.getConvention() == Convention.NONE;
  }

  public static boolean notConverter(RelNode rel) {
    return !(rel instanceof AbstractConverter);
  }

  public static Predicate<TvrSemantics> notSetSnapshot =
          t -> !(t instanceof TvrSetSnapshot);

  public static Predicate<TvrSemantics> isSnapshotTime =
          t -> t.fromVersion.isMin();

  // the tvrTrait is equivalent to a set snapshot
  // SetSnapshot(t) itself / SetDelta (MIN - t)
  public static final Predicate<TvrSemantics> equivSnapshot =
          isSnapshotTime.and(t -> t instanceof TvrSetSemantics);

  public static final Predicate<TvrSemantics> isSetDelta =
          t -> t instanceof TvrSetDelta;

  public static final Predicate<TvrSemantics> isPositiveOnlySetDelta =
          isSetDelta.and(t -> ((TvrSetDelta) t).isPositiveOnly());

  public static final Predicate<TvrSemantics> isPositiveNegativeSetDelta =
          isSetDelta.and(t -> !((TvrSetDelta) t).isPositiveOnly());

  public static final Predicate<TvrSemantics> isNormalSetDelta =
          isSetDelta.and(isSnapshotTime.negate());

  public static List<RexNode> getProjects(RelNode input) {
    return getProjects(input,
            ImmutableIntList.range(0, input.getRowType().getFieldCount()));
  }

  public static List<RexNode> getProjects(RelNode input,
                                          List<Integer> projectRefs) {
    RexBuilder builder = input.getCluster().getRexBuilder();
    return projectRefs.stream().map(i -> builder.makeInputRef(input, i))
            .collect(Collectors.toList());
  }

  public static String getMultiplicityName() {
    return "__multiplicity__";
  }

  public static RelDataType getMultiplicityType(RelDataTypeFactory typeFactory,
                                                boolean nullable) {
    return typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.BIGINT), nullable);
  }

  public static AggregateCall getRowCountAggCall(RelNode input) {
    return AggregateCall
            .create(SqlStdOperatorTable.COUNT, false, false, ImmutableList.of(), -1, 1, input, null,
                    "__row_count__");
  }

  public static boolean inputExistsKey(RelNode relNode) {
    RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
    Set<ImmutableBitSet> uniqueKeys = mq.getUniqueKeys(relNode);
    if (uniqueKeys != null && !uniqueKeys.isEmpty()) {
      return true;
    }
    return "true".equalsIgnoreCase(
            TvrContext.getInstance(relNode.getCluster()).getConfig()
                    .get(TvrUtils.PROGRESSIVE_TUPLE_NO_DUPLICATE, "false"));
  }

//  public static QueryContext.Cost makeDumpedCost(RelOptCost cost) {
//    if (cost == null) {
//      return null;
//    }
//
//    OdpsRelOptCostImpl odpsCost;
//    if (cost instanceof OdpsRelOptCostImpl) {
//      odpsCost = (OdpsRelOptCostImpl) cost;
//    } else {
//      odpsCost = (OdpsRelOptCostImpl) OdpsRelOptCostImpl.FACTORY
//          .makeCost(cost.getRows(), cost.getCpu(), cost.getIo());
//    }
//
//    QueryContext.Cost dumpedCost = new QueryContext.Cost();
//    if (odpsCost.isInfinite() || odpsCost.getCost() == null) {
//      dumpedCost.Cost = Double.MAX_VALUE;
//      dumpedCost.cpu = Double.MAX_VALUE;
//      dumpedCost.io = Double.MAX_VALUE;
//      dumpedCost.memory = Double.MAX_VALUE;
//      dumpedCost.network = Double.MAX_VALUE;
//      return dumpedCost;
//    }
//    dumpedCost.Cost = odpsCost.getCost().doubleValue();
//    dumpedCost.cpu = odpsCost.getCpu();
//    dumpedCost.io = odpsCost.getIo();
//    dumpedCost.memory = odpsCost.getMemory();
//    dumpedCost.network = odpsCost.getNetwork();
//    return dumpedCost;
//  }

  public static String getDimensionTables(TvrContext ctx) {
    return ctx.getConfig().get(PROGRESSIVE_DIMENSION_TABLES, "").trim();
  }

  public static boolean isDimTable(TvrContext ctx, RelNode tableScan) {
    return false;
  }

  public static boolean isDimTable(TvrContext ctx, String normalizedName) {
    return false;
  }

  public static boolean isBaseTableAppendOnly(RelOptTable relOptTable) {
    // TODO: add a config that specifies the append only tables
    return true;
  }

  public static RelNode makeLogicalEmptyValues(RelOptCluster cluster,
                                               RelDataType rowType) {
    return LogicalValues.create(cluster, rowType, ImmutableList.of());
  }

  public static boolean isEmpty(RelNode node) {
    if (node instanceof Values) {
      return ((Values) node).getTuples().isEmpty();
    }
    if (node instanceof HepRelVertex) {
      return isEmpty(((HepRelVertex) node).getCurrentRel());
    }
    // Note: relation input might be a RelSubset, so we just iterate over the relations
    // in order to check if the subset is equivalent to an empty relation.
    if (!(node instanceof RelSubset)) {
      return false;
    }
    RelSubset subset = (RelSubset) node;
    for (RelNode rel : subset.getRels()) {
      if (isEmpty(rel)) {
        return true;
      }
    }
    return false;
  }

  public static RelNode matchNullable(RelNode rel, RelDataType target, RelBuilder builder) {
    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    List<RelDataTypeField> src = rel.getRowType().getFieldList();
    List<RelDataTypeField> tgt = target.getFieldList();
    assert src.size() >= tgt.size();
    List<RexNode> projects = new ArrayList<>();
    boolean converted = false;
    for (int i = 0; i < src.size(); ++i) {
      RelDataType s = src.get(i).getType();
      RexNode expr;
      if (i >= tgt.size()) {
        expr = rexBuilder.makeInputRef(rel, i);
      } else {
        RelDataType t = tgt.get(i).getType();
        if (s.isNullable() != t.isNullable()) {
          assert !s.isNullable() && t.isNullable();
          converted = true;
          expr = rexBuilder.makeAbstractCast(t, rexBuilder.makeInputRef(rel, i));
        } else {
          expr = rexBuilder.makeInputRef(rel, i);
        }
      }
      projects.add(expr);
    }
    return converted ? builder.push(rel).project(projects).build() : rel;
  }

  public static RelNode convertToSetDeltaIfNot(RelNode rel, TvrSetDelta trait) {
    if (!trait.isPositiveOnly()) {
      return rel;
    }
    return ProjectBuilder.anchor(rel).addAll() // original columns
            .addConstMultiplicity(BigDecimal.ONE) // multiplicity = +1
            .build();
  }

  public static void addDistinctName(RelDataTypeFactory.Builder builder,
                                     Set<String> names, String base, RelDataType type) {
    String name = makeUniqueName(base, names);
    names.add(name);
    builder.add(name, type);
  }

  public static String makeUniqueName(String base, Collection<String> names) {
    String name = base;
    int i = 0;
    while (names.contains(name)) {
      name = base + "_" + (i++);
    }
    return name;
  }

  public static boolean hasDistinctAggCall(List<AggregateCall> aggCalls) {
    return aggCalls.stream().anyMatch(AggregateCall::isDistinct);
  }

  public static String getPlanPath(TvrContext ctx) {
    String querySignature = progressiveQuerySignature(ctx);
    if (querySignature == null || querySignature.isEmpty()) {
      throw new IllegalArgumentException("Empty query signature");
    }

    String clusterName = ctx.getConfig().get("compiler.running.cluster");
    if (clusterName == null || clusterName.isEmpty()) {
      throw new IllegalArgumentException("Cluster name not known.");
    }

    long[] intervals = ctx.getTvrVersions();
    String intervalStr = Arrays.stream(intervals).mapToObj(String::valueOf).collect(Collectors.joining("_"));
    String fileName = String.join("-", "plan", querySignature, intervalStr);
    String prefix = progressivePlanPathPrefix(ctx);
    return "pangu://" + clusterName + prefix + "/" + fileName + ".gv";
  }


  public static String formatTimeInstant(Long time) {
    if (time == TvrVersion.MIN_TIME) {
      return "MIN";
    } else if (time == TvrVersion.MAX_TIME) {
      return "MAX";
    } else {
      return Long.toString((time / 3600000L) % 24L);
    }
  }

  // collect the tvr update table names that is used by a relNode
  public static Set<String> collectUpdateTableNames(RelNode root) {
    TvrContext ctx = TvrContext.getInstance(root.getCluster());
    return collectTables(root).stream().map(
            r -> TvrTableUtils.getNormalizedTableName(r))
            .flatMap(t -> ctx.getUpdateTableMapping().get(t).stream())
            .collect(Collectors.toSet());
  }

  // collect the tableScan operators that is used by a relNode
  public static Set<LogicalTableScan> collectTables(RelNode root) {
    Set<LogicalTableScan> tables = new HashSet<>();
    Set<RelNode> visited = new HashSet<>();
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node == null) {
          return;
        }
        if (visited.contains(node)) {
          return;
        }
        visited.add(node);
        super.visit(node, ordinal, parent);
        if (node instanceof RelSubset) {
          RelNode original = ((RelSubset) node).getOriginal();
          if (original != null) {
            visit(original, ordinal, parent);
          }
        }
        if (node instanceof LogicalTableScan) {
          tables.add((LogicalTableScan) node);
        }
      }
    }.go(root);
    return tables;
  }

  // collect the tableScan operators and their TvrSemantics used by a relNode
  public static Set<Pair<LogicalTableScan, TvrSetSemantics>> collectTablesAndTvrs(RelNode root) {
    VolcanoPlanner planner = (VolcanoPlanner) root.getCluster().getPlanner();
    TvrContext ctx = TvrContext.getInstance(root.getCluster());
    Set<Pair<LogicalTableScan, TvrSetSemantics>> tables = new LinkedHashSet<>();

    Set<RelNode> visited = new LinkedHashSet<>();
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node == null) {
          return;
        }
        if (visited.contains(node)) {
          return;
        }
        visited.add(node);
        super.visit(node, ordinal, parent);
        if (node instanceof RelSubset) {
          RelNode original = ((RelSubset) node).getOriginal();
          if (original != null) {
            visit(original, ordinal, parent);
          }
          return;
        }

        RelSubset nodeSubset = planner.getSubset(node);
        if (nodeSubset == null) {
          return;
        }

        nodeSubset.getTvrLinks().asMap().forEach((tvrTrait, tvrs) -> {
          if (!(tvrTrait instanceof TvrSetSemantics)) {
            return;
          }
          if (tvrTrait.toVersion.isMax()) {
            return;
          }
          if (tvrTrait instanceof TvrSetDelta && tvrTrait.fromVersion.isMin()) {
            return;
          }
          for (TvrMetaSet tvr : tvrs) {
            if (!tvr.getTvrType().equals(ctx.getDefaultTvrType())) {
              continue;
            }
            RelSubset subset = tvr.getSubset(TvrSemantics.SET_SNAPSHOT_MAX,
                    root.getCluster().traitSet());
            if (subset == null) {
              continue;
            }
            RelNode original = subset.getOriginal();
            if (original instanceof LogicalTableScan) {
              if (!TvrUtils.isDimTable(ctx, original)) {
                tables.add(Pair.of((LogicalTableScan) original, (TvrSetSemantics) tvrTrait));
              } else {
                tables.add(Pair.of((LogicalTableScan) original, TvrSemantics.SET_SNAPSHOT_MAX));
              }
            }
          }
        });
      }
    }.go(root);
    return tables;
  }

  public static List<RelNode> collectSinks(RelNode root) {
    if (root instanceof RelSubset) {
      return ((RelSubset) root).getRelList().stream()
              .flatMap(r -> collectSinks(r).stream()).collect(Collectors.toList());
//    } else if (root instanceof VirtualRoot) {
//      return root.getInputs().stream().flatMap(i -> collectSinks(i).stream())
//          .collect(Collectors.toList());
    } else if (root instanceof AdhocSink || root instanceof TableSink) {
      return Collections.singletonList(root);
    } else {
      return Collections.emptyList();
    }
  }

  public static boolean isVirtualTableSink(RelNode sink) {
    if (!TvrUtils.progressiveVirtualTablesinkEnabled(
            TvrContext.getInstance(sink.getCluster()))) {
      return false;
    }

    VirtualTableSinkFinder f = new VirtualTableSinkFinder();
    f.go(sink);
    return f.virtualSpoolInInput;
  }

  public static List<RelNode> findActualSinks(List<RelNode> sinks) {
    return sinks.stream().filter(sink -> !isVirtualTableSink(sink))
            .collect(Collectors.toList());
  }

  public static boolean splittable(RelNode rel) {
    boolean checkTableMeta = false;
    if (rel instanceof Union && ((Union) rel).all) {
      RelCollation collation = rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
      if (collation == null || collation.getFieldCollations().isEmpty()) {
        checkTableMeta = true;
      }
    }
    if (rel instanceof TableSink) {
      checkTableMeta = true;
    }
    if (checkTableMeta) {
      return rel.getInputs().stream().allMatch(
              input -> tableMetaReusable(rel.getTraitSet(), input.getTraitSet()));
    }
    return false;
  }

  public static class VirtualTableSinkFinder extends RelVisitor {
    boolean virtualSpoolInInput = false;
    private Set<RelNode> visited = new HashSet<>();
    @Override public void visit(RelNode node, int ordinal, RelNode parent) {
      if (visited.contains(node) || virtualSpoolInInput) {
        return;
      }

      visited.add(node);
      if (node instanceof TvrVirtualSpool) {
        virtualSpoolInInput = true;
      } else if (node instanceof RelSubset) {
        ((RelSubset) node).getRelList().forEach(r -> visit(r, ordinal, parent));
      } else {
        super.visit(node, ordinal, parent);
      }
    }
  }

  public static Map<RelSubset, MatType> parseMatString(String matString,
                                                       Map<Integer, RelSubset> id2SubsetMap) {
    if (matString == null || matString.equals("")) {
      return Collections.emptyMap();
    }

    Map<RelSubset, MatType> mapping = new IdentityHashMap<>();
    for (String s : matString.split(",")) {
      String[] strPair = s.trim().split(":");
      assert strPair.length == 2;
      int subsetId = Integer.parseInt(strPair[0]);
      int matType = Integer.parseInt(strPair[1]);
      RelSubset subset = id2SubsetMap.get(subsetId);
      if (subset != null) {
        mapping.put(subset, MatType.values()[matType]);
      }
    }

    return mapping;
  }

  public static class TvrMetaSetVisitor extends TvrVisitor {
    public Set<TvrMetaSet> metaSets = new HashSet<>();

    @Override
    public void visit(TvrMetaSet tvr, TvrProperty propertyEdge,
                      TvrMetaSet parent) {
      if (!metaSets.add(tvr)) {
        return;
      }
      super.visit(tvr, propertyEdge, parent);
    }
  }

  public static long[] addVersion(long newVersion, long[] versions, int insertIndex) {
    assert insertIndex >= 0;
    long[] newVersions = new long[versions.length + 1];
    int i = 0;
    for (; i < insertIndex; i++) {
      newVersions[i] = versions[i];
    }
    newVersions[i++] = newVersion;
    for (; i < newVersions.length; i++) {
      newVersions[i] = versions[i - 1];
    }
    return newVersions;
  }

  public static CalciteCatalogReader getRootRelOptSchema(RelOptCluster cluster) {
    return (CalciteCatalogReader) ((VolcanoPlanner) cluster.getPlanner()).registeredSchemas.iterator().next();
  }


}
