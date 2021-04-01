package org.apache.calcite.rel.tvr.rules;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.tvr.rels.EnumerablePhysicalAdhocSink;
import org.apache.calcite.rel.tvr.rels.EnumerablePhysicalTvrVirtualSpool;
import org.apache.calcite.rel.tvr.rels.EnumerablePhysicalVirtualRoot;
import org.apache.calcite.rel.tvr.rules.dbtoaster.TvrDBTGooOrderingMultiJoinPropRule;
import org.apache.calcite.rel.tvr.rules.dbtoaster.TvrDBTPropRule;
import org.apache.calcite.rel.tvr.rules.dbtoaster.TvrDBTTableScanRule;
import org.apache.calcite.rel.tvr.rules.dbtoaster.TvrDBTValuesRule;
import org.apache.calcite.rel.tvr.rules.logical.*;
import org.apache.calcite.rel.tvr.rules.operators.*;
import org.apache.calcite.rel.tvr.rules.outerjoinview.*;
import org.apache.calcite.rel.tvr.rules.streaming.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Rule filter.
 * <p/>
 * Generate RuleSet based on config string. This class is used in main class
 * that accept user's config on switching on/off specific rules.
 * <p/>
 */
public class TvrRuleCollection {
  private static final Log LOG = LogFactory.getLog(TvrRuleCollection.class);
  public enum RuleConfig {

    PROGRESSIVE_ENUMERABLE(ImmutableSet.of(
            EnumerablePhysicalVirtualRoot.ENUMERABLE_PHYSICAL_VIRTUALROOT_RULE,
            EnumerablePhysicalTvrVirtualSpool.ENUMERABLE_PHYSICAL_TVR_VIRTUAL_SPOOL_RULE,
            EnumerablePhysicalAdhocSink.ENUMERABLE_PHYSICAL_TVR_ADHOC_SINK_RULE
    ), false),

    PE(ImmutableSet.of(
            PruneEmptyRules.AGGREGATE_INSTANCE,
            PruneEmptyRules.FILTER_INSTANCE,
            PruneEmptyRules.JOIN_LEFT_INSTANCE,
            PruneEmptyRules.JOIN_RIGHT_INSTANCE,
            PruneEmptyRules.PROJECT_INSTANCE,
            PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
            PruneEmptyRules.SORT_INSTANCE
    ), false),

    PROGRESSIVE_OUTER_JOIN_VIEW(ImmutableSet.of(
        // Outer join view rules
        // (Efficient Maintenance of Materialized Outer-Join Views, ICDE'07)
        // Part one: tvr property propagation rules
        TvrOjvTableScanPropRule.INSTANCE,
        TvrOjvFilterPropRule.INSTANCE,
        TvrOjvJoinPropRule.INSTANCE,
        TvrOjvValuesPropRule.INSTANCE,
        TvrOjvProjectPropRule.INSTANCE,
        TvrOjvUnionPropRule.INSTANCE,
        // Modified GooJoinOrderingRule for OJV that lays out potential join
        // order for different VDs.
        TvrOjvGooJoinOrderRule.INSTANCE,
        // Part two: the actual tvr rules using tvr property
        TvrOuterJoinViewRule.INSTANCE
    ), false),

    PROGRESSIVE_STREAMING(ImmutableSet.of(
        // Streaming propagation rules
        TvrStreamingTableScanPropRule.INSTANCE,
        TvrStreamingFilterPropRule.INSTANCE,
        TvrStreamingProjectPropRule.INSTANCE,
        TvrStreamingJoinPropRule.INSTANCE,
        TvrStreamingVirtualSpoolPropRule.INSTANCE,
        TvrStreamingValuesPropRule.INSTANCE
    ), false),

    PROGRESSIVE_DBTOASTER(ImmutableSet.of(
        // DBToaster propagation rules

        TvrDBTGooOrderingMultiJoinPropRule.INSTANCE,

        TvrDBTPropRule.FILTER_INSTANCE,
        TvrDBTPropRule.PROJECT_INSTANCE,
        TvrDBTPropRule.AGGREGATE_INSTANCE,
        TvrDBTPropRule.SORT_INSTANCE,
        TvrDBTPropRule.WINDOW_INSTANCE,
        TvrDBTPropRule.MULTI_JOIN_INSTANCE,
        TvrDBTPropRule.UNION_INSTANCE,

        TvrDBTValuesRule.INSTANCE,
        TvrDBTTableScanRule.INSTANCE
    ), false),

    PROGRESSIVE_COPY(ImmutableSet.of(
        TvrTableScanRule.COPY_INSTANCE,
        TvrValuesRule.COPY_INSTANCE
//        TvrTableConsolidationRule.INSTANCE
    ), false),

    PROGRESSIVE_FLATTENING(ImmutableSet.of(
        TvrValueDeltaDedupFlatteningRule.INSTANCE
    ), false),

    PROGRESSIVE(ImmutableSet.of(
        // hacky, remove in the future?
        TvrSetSnapshotToSetDeltaRule.INSTANCE,

        // general rules
        TvrAnyToSetSnapshotRule.INSTANCE,
        TvrSuperValueDeltaToSetSnapshotRule.INSTANCE,
        TvrValueDeltaMergeRule.INSTANCE,
        SetDeltaTvrBridgingRule.INSTANCE,

        // operator-specific rules
        TvrFilterRules.TVR_SET_DELTA_FILTER_RULE,
        TvrFilterRules.TVR_VALUE_SEMANTICS_FILTER_RULE,
        TvrProjectRules.TVR_SET_DELTA_PROJECT_RULE,
        TvrProjectRules.TVR_VALUE_SEMANTICS_PROJECT_RULE,
//        TvrExpandRules.TVR_SET_DELTA_EXPAND_RULE,
//        TvrExpandRules.TVR_VALUE_SEMANTICS_EXPAND_RULE,
        TvrTableScanRule.INSTANCE,
        TvrTableScanRule.SAMPLE_INSTANCE,
        TvrValuesRule.INSTANCE,
        TvrValuesRule.SAMPLE_INSTANCE,
//        TvrTableFunctionScanRule.INSTANCE,
        TvrAggregateRules.POSITIVE_AGG_RULE,
        TvrAggregateRules.NON_POSITIVE_AGG_RULE,
        TvrAggregateRules.POSITIVE_VALUE_DELTA_AGG_RULE,
        TvrDeduperApplyRule.INSTANCE,
        TvrUnionRule.INSTANCE,
        new TvrSetDeltaUnionRule(2),
        new TvrSetDeltaUnionRule(3),
        new TvrSetDeltaUnionRule(4),
        new TvrSetDeltaUnionRule(5),
        TvrSetDeltaMergeRule.INSTANCE,
        TvrSetDeltaConsolidateToSetSnapshotRule.INSTANCE,
        TvrSetSnapshotToPositiveSuperSetDeltaRule.INSTANCE,
        TvrWindowRules.WINDOW_RULE,
        TvrWindowRules.FILTER_WINDOW_DECOMPOSITION_RULE,
        TvrSortRules.TVR_SET_SEMANTICS_SORT_RULE,
        TvrSortRules.TVR_VALUE_SEMANTICS_SORT_RULE,
//        TvrSortRules.TVR_TWO_PHASE_LIMIT_RULE,
//        TvrSortRules.TVR_THREE_PHASE_LIMIT_RULE,
        TvrVirtualSpoolRules.TVR_SET_DELTA_TvrVirtualSpool_RULE,
        TvrVirtualSpoolRules.TVR_VALUE_SEMANTICS_TvrVirtualSpool_RULE,
        TvrJoinRuleOneSideMultiMatch.INSTANCE
        ), false),

    ;

    private final boolean def;
    private final Set<RelOptRule> ruleSet;

    private static final Map<String, RuleConfig> dict;
    static {
      ImmutableMap.Builder<String, RuleConfig> dictBuilder = ImmutableMap
              .builder();
      for (RuleConfig c : RuleConfig.values()) {
        dictBuilder.put(c.name(), c);
      }
      dict = dictBuilder.build();
    }

    RuleConfig(RelOptRule rule, boolean def) {
      this(ImmutableSet.of(rule), def);
    }

    RuleConfig(Set<RelOptRule> ruleSet, boolean def) {
      this.ruleSet = ruleSet;
      this.def = def;
    }

    public Set<RelOptRule> getRuleSet() {
      return ruleSet;
    }


  }

  public static class RuleCollectionBuilder {
    Set<RelOptRule> ruleSet;

    private RuleCollectionBuilder() {
      this(ImmutableSet.of());
    }

    public RuleCollectionBuilder(
        Collection<RelOptRule> rules) {
      this.ruleSet = Sets.newLinkedHashSet();
      ruleSet.addAll(rules);
    }

    public RuleCollectionBuilder with(String with) {
      with = with.toUpperCase();
      RuleConfig r = RuleConfig.dict.get(with);
      if (r != null) {
        ruleSet.addAll(r.getRuleSet());
      } else {
        LOG.warn("Unknown rule :" + String.valueOf(with));
      }
      return this;
    }

    public RuleCollectionBuilder without(String without) {
      RuleConfig r = RuleConfig.dict.get(without.toUpperCase());
      if (r != null) {
        ruleSet.removeAll(r.getRuleSet());
      } else {
        LOG.warn("Unknown rule :" + String.valueOf(without));
      }
      return this;
    }

    public RuleCollectionBuilder withAll(Iterable<String> with) {
      for (String w : with) {
        with(w);
      }
      return this;
    }

    public RuleCollectionBuilder withoutAll(Iterable<String> without) {
      for (String w : without) {
        without(w);
      }
      return this;
    }

    public boolean hasAdded(String rule) {
      RuleConfig r = RuleConfig.dict.get(rule.toUpperCase());
      return r != null && ruleSet.containsAll(r.getRuleSet());
    }

    public Set<RelOptRule> build() {
      return ruleSet;
    }

    public RuleCollectionBuilder with(RuleConfig rule) {
      return with(rule.name());
    }

    public RuleCollectionBuilder without(RuleConfig rule) {
      return without(rule.name());
    }

    public boolean hasAdded(RuleConfig rule) {
      return hasAdded(rule.name());
    }
  }

  /**
   * Create a default rule builder
   *
   * @return rule builder.
   */
  public static RuleCollectionBuilder builder() {
    return new RuleCollectionBuilder();
  }

  /**
   * create a rule build only with specified rules, don't add other default rule
   * @param rules
   * @return
   */
  public static RuleCollectionBuilder builder(Collection<RelOptRule> rules) {
    return new RuleCollectionBuilder(rules);
  }


  public static Set<RelOptRule> tvrStandardRuleSet() {
    Set<RelOptRule> ruleSet = new HashSet<>();
    // add basic logical rules
    ruleSet.addAll(CalcitePrepareImpl.DEFAULT_RULES);
    ruleSet.remove(ProjectMergeRule.INSTANCE);
    ruleSet.add(ProjectToCalcRule.INSTANCE);
    ruleSet.add(FilterToCalcRule.INSTANCE);

    // add physical rules
    ruleSet.addAll(CalcitePrepareImpl.ENUMERABLE_RULES);
    ruleSet.remove(EnumerableRules.ENUMERABLE_PROJECT_RULE);
    ruleSet.remove(EnumerableRules.ENUMERABLE_FILTER_RULE);
    ruleSet.add(EnumerableRules.ENUMERABLE_CALC_RULE);

    // add LoptOptimizeJoinRule (MultiJoin -> Join)
    ruleSet.add(LoptOptimizeJoinRule.INSTANCE);

    // add tvr rules
    ruleSet.addAll(TvrRuleCollection.builder()
            .with(RuleConfig.PROGRESSIVE).build());
    ruleSet.addAll(TvrRuleCollection.builder()
            .with(RuleConfig.PROGRESSIVE_ENUMERABLE).build());

    return ruleSet;
  }

}
