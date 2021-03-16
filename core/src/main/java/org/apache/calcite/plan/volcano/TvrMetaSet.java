package org.apache.calcite.plan.volcano;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toMap;
import static org.apache.calcite.plan.volcano.VolcanoPlanner.equivRoot;

/**
 * Represent a time varying relation (TVR).
 */
public class TvrMetaSet implements Comparable<TvrMetaSet> {

  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  private static int nextTvrId = 0;

  public static void resetNextTvrId() {
    nextTvrId = 0;
  }

  int tvrId;

  private TvrMetaSetType tvrType;

  /**
   * Whether this tvr has been merged into other tvr and thus obsolete.
   */
  private TvrMetaSet equivTvr;

  /**
   * Links to all snapshots, deltas and Anys associated with this tvr.
   */
  Map<TvrSemantics, RelSet> tvrSets = new LinkedHashMap<>();

  /**
   * Tvr property component links (directed) between tvrs used by various
   * progressive computing methods
   */
  Map<TvrProperty, TvrMetaSet> tvrPropertyLinks;
  SetMultimap<TvrProperty, TvrMetaSet> reverseTvrPropertyLinks;

  // Row type of the set snapshot schema
  RelDataType standardSchema;

  Queue<TvrConvertMatch> delayedTvrConverters;

  // No public constructor to ban rules from creating new tvrMetaSet directly
  protected TvrMetaSet(TvrMetaSetType tvrType) {
    tvrId = nextTvrId++;
    tvrPropertyLinks = new LinkedHashMap<>();
    reverseTvrPropertyLinks = LinkedHashMultimap.create();
    delayedTvrConverters = new ArrayDeque<>();
    equivTvr = null;
    this.tvrType = tvrType;
    assert tvrType != null;
  }

  public int getTvrId() {
    return tvrId;
  }

  public Map<TvrProperty, TvrMetaSet> getTvrPropertyLinks() {
    return tvrPropertyLinks;
  }

  public <T extends TvrProperty> Map<T, TvrMetaSet> getTvrPropertyLinks(
      Class<? extends TvrProperty> propertyClass) {
    return getTvrPropertyLinks(
        property -> propertyClass.isAssignableFrom(property.getClass()));
  }

  public <T extends TvrProperty> Map<T, TvrMetaSet> getTvrPropertyLinks(
      Predicate<TvrProperty> propertyPredicate) {
    Map<T, TvrMetaSet> ret = new LinkedHashMap<>();
    tvrPropertyLinks.entrySet().stream()
        .filter(entry -> propertyPredicate.test(entry.getKey()))
        .forEach(entry -> ret.put((T) entry.getKey(), entry.getValue()));
    return ret;
  }

  public SetMultimap<TvrProperty, TvrMetaSet> getReverseTvrPropertyLinks() {
    return reverseTvrPropertyLinks;
  }

  public TvrMetaSetType getTvrType() {
    return tvrType;
  }

  public RelSet getRelSet(TvrSemantics tvrSemantics) {
    return tvrSets.get(tvrSemantics);
  }

  public TvrMetaSet getActiveTvr() {
    TvrMetaSet ret = this;
    while (ret.equivTvr != null) {
      ret = ret.equivTvr;
    }
    return ret;
  }

  public RelSet addTvrLink(TvrSemantics tvrKey, RelSet set) {
    return tvrSets.put(tvrKey, set);
  }

  public RelSet removeTvrLink(TvrSemantics tvrKey) {
    return tvrSets.remove(tvrKey);
  }

  public RelDataType getStandardSchema() {
    if (standardSchema == null) {
      RelSet set = getRelSet(TvrSemantics.SET_SNAPSHOT_MAX);
      assert set != null;
      standardSchema = set.getRowType();
    }
    return standardSchema;
  }

  public Set<TvrSemantics> allTvrSemantics() {
    return tvrSets.keySet();
  }

  public Map<TvrSemantics, RelSet> tvrSets() {
    return tvrSets;
  }

  public boolean isObsolete() {
    return equivTvr != null;
  }

  public TvrMetaSet addPropertyEdge(VolcanoPlanner planner, TvrProperty tvrProperty,
      TvrMetaSet toTvr) {
    LOGGER
        .debug("Adding tvr property link {} from tvr {} to tvr {}", tvrProperty,
            this.getTvrId(), toTvr.getTvrId());
    planner.registerTvrPropertyClass(tvrProperty);

    TvrMetaSet existing = tvrPropertyLinks.get(tvrProperty);
    if (existing != null) {
      if (existing != toTvr) {
        existing.mergeTvr(planner, toTvr);
      }
      return existing; // no need to trigger fire rules on property edge
    }

    tvrPropertyLinks.put(tvrProperty, toTvr);
    toTvr.reverseTvrPropertyLinks.put(tvrProperty, this);

    // Fire rules as a result of this new link
    planner.fireRules(this, toTvr, tvrProperty, true);
    return toTvr;
  }

  public void mergeTvr(VolcanoPlanner planner, TvrMetaSet other) {
    assert this != other;
    assert !isObsolete() && !other.isObsolete();
    assert tvrType.equals(other.tvrType);
    LOGGER.debug("tvr: Merge TvrMetaSet {} into TvrMetaSet {}", other.tvrId,
        tvrId);
    Set<Pair<RelSet, RelSet>> setsToMerge = new LinkedHashSet<>();

    other.tvrSets.forEach((tvrKey, otherSet) -> {
      boolean removed = otherSet.tvrLinks.remove(tvrKey, other);
      assert removed;
      RelSet mySet = getRelSet(tvrKey);
      if (mySet == null) {
        equivRoot(otherSet).addTvrLink(planner, null, tvrKey, this);
      } else {
        setsToMerge.add(new Pair<>(mySet, otherSet));
      }
    });
    // wipe out the stale TvrMetaSet other, and ensure tvr link consistency
    other.tvrSets.clear();

    Set<Pair<TvrMetaSet, TvrMetaSet>> tvrsToMerge = new LinkedHashSet<>();
    other.tvrPropertyLinks.forEach((tvrProperty, toTvr) -> {
      // Remove the old link
      toTvr.reverseTvrPropertyLinks.remove(tvrProperty, other);
      // Add the new link
      if (toTvr == other) {   // handle the self loop case
        toTvr = this;
      }
      TvrMetaSet myToTvr = tvrPropertyLinks.get(tvrProperty);
      if (myToTvr == null) {
        tvrPropertyLinks.put(tvrProperty, toTvr);
        toTvr.reverseTvrPropertyLinks.put(tvrProperty, this);
      } else if (myToTvr != toTvr) {
        tvrsToMerge.add(new Pair<>(myToTvr, toTvr));
      }
    });

    other.reverseTvrPropertyLinks.entries().forEach(entry -> {
      TvrProperty tvrProperty = entry.getKey();
      TvrMetaSet tvr = entry.getValue();
      assert tvr != other;  // self loop should have been handled
      TvrMetaSet existing = tvr.tvrPropertyLinks.put(tvrProperty, this);
      assert existing == other;
      reverseTvrPropertyLinks.put(tvrProperty, tvr);
    });
    other.tvrPropertyLinks.clear();
    other.reverseTvrPropertyLinks.clear();

    // clear delayed tvr converters
    if (!other.delayedTvrConverters.isEmpty()) {
      delayedTvrConverters.addAll(other.delayedTvrConverters);
      other.delayedTvrConverters.clear();

      planner.untrackMetaSetWithDelayedTvrConverters(other);
      planner.trackMetaSetWithDelayedTvrConverters(this);
    }

    other.equivTvr = this;

    // Trigger the potentially deep recursive merge
    for (Pair<RelSet, RelSet> toMerge : setsToMerge) {
      if (toMerge.left == toMerge.right) {
        continue;
      }
      LOGGER.debug("tvr: setsToMerge set#{} and set#{}", toMerge.left.id,
          toMerge.right.id);
      planner.merge(toMerge.left, toMerge.right);
    }

    tvrsToMerge.forEach(pair -> {
      TvrMetaSet left = (pair.left == other) ? this : pair.left;
      TvrMetaSet right = (pair.right == other) ? this : pair.right;
      if (left != right && !left.isObsolete() && !right.isObsolete()) {
        LOGGER.debug("tvr: tvrsToMerge tvr#{} and tvr#{}", left.getTvrId(),
            right.getTvrId());
        left.mergeTvr(planner, right);
      }
    });

    planner.fireRules(this);
  }

  /**
   * Gets the subset of this tvrSemantics and traits, null if not exists
   */
  public RelSubset getSubset(TvrSemantics tvrSemantics, RelTraitSet traits) {
    RelSet set = getRelSet(tvrSemantics);
    if (set == null) {
      return null;
    }
    return set.getSubset(traits);
  }

  /**
   * Gets all subsets of this TVR that satisfy the TvrSemantics predicate
   */
  public Map<TvrSemantics, RelSubset> getSubsets(
      Predicate<TvrSemantics> tvrPredicate, RelTraitSet traits) {
    return tvrSets.entrySet().stream()
        .filter(e -> tvrPredicate.test(e.getKey()))
        .map(e -> Pair.of(e.getKey(), e.getValue().getSubset(traits)))
        .filter(p -> p.getValue() != null)
        .collect(toMap(Pair::getKey, Pair::getValue));
  }

  @Override
  public String toString() {
    return "tvr#" + getTvrId() + ":" + getTvrType();
  }

  /**
   * Add tvr converters between RelSets belong to a TvrMetaSet.
   */
  void addTvrConverters(TvrSemantics newTvrTrait, VolcanoPlanner planner,
      RelOptCluster cluster) {
    for (TvrConvertMatchPattern pattern : planner
        .getTvrConvertMatchPatterns(newTvrTrait)) {
      for (TvrConvertMatch match : pattern.match(this, newTvrTrait, cluster)) {
        if (match.delayRegister) {
          if (delayedTvrConverters.isEmpty()) {
            planner.trackMetaSetWithDelayedTvrConverters(this);
          }
          delayedTvrConverters.add(match);
          continue;
        }
        fireTvrConvertMatch(match, planner);
      }
    }
  }

  boolean createDelayedTvrConverter(VolcanoPlanner planner) {
    while (!delayedTvrConverters.isEmpty()) {
      TvrConvertMatch match = delayedTvrConverters.poll();
      if (!match.satisfied.test(this)) {
        LOGGER.debug("Create delayed converter {} for tvr#{}", match.tvrKey, getTvrId());
        fireTvrConvertMatch(match, planner);
        return true;
      }
      LOGGER.debug("Skip delayed converter {} for tvr#{}", match.tvrKey, getTvrId());
    }
    return false;
  }

  void fireTvrConvertMatch(TvrConvertMatch match, VolcanoPlanner planner) {
    RelNode previous = null;
    for (RelNode newRel : match.newRels) {
      RelSubset subset = planner.ensureRegistered(newRel, previous);
      subset.set.addTvrLinkWithSetMerge(planner, match.tvrKey, this);
      if (previous == null) {
        previous = newRel;
      }
    }
  }

  public void setTvrType(TvrMetaSetType tvrType) {
    this.tvrType = tvrType;
  }

  @Override
  public int compareTo(TvrMetaSet other) {
    return this.getTvrId() - other.getTvrId();
  }

  public void accept(TvrVisitor tvrVisitor) {
    tvrPropertyLinks.forEach((edge, toTvr) -> {
      tvrVisitor.visit(toTvr, edge, this);
    });
  }

  /**
   * Store related information about a TvrConverterMatch created by
   * RelOptRule.getTvrConvertPattern()
   */
  public static class TvrConvertMatch {
    TvrSemantics tvrKey;
    List<RelNode> newRels;

    boolean delayRegister;
    Predicate<TvrMetaSet> satisfied;

    public TvrConvertMatch(TvrSemantics tvrKey, List<RelNode> newRels) {
      this(tvrKey, newRels, false, null);
    }

    public TvrConvertMatch(TvrSemantics tvrKey, List<RelNode> newRels,
        boolean delayRegister, Predicate<TvrMetaSet> satisfied) {
      assert !delayRegister || satisfied != null;
      this.tvrKey = tvrKey;
      this.newRels = newRels;
      this.delayRegister = delayRegister;
      this.satisfied = satisfied;
    }
  }

  /**
   * Describe a match pattern for a Tvr converter. Related tvr rules should
   * provide its match pattern if needed.
   */
  public interface TvrConvertMatchPattern {
    // Return the list of related Tvr classes whose addition might trigger
    // this pattern to match
    List<Class<? extends TvrSemantics>> getRelatedTvrClasses();

    // Match this pattern against the given tvr and the newly added tvr trait
    List<TvrConvertMatch> match(TvrMetaSet tvr, TvrSemantics newTrait,
        RelOptCluster cluster);
  }

}
