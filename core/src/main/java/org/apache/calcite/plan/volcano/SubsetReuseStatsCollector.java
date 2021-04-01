package org.apache.calcite.plan.volcano;


import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AdhocSink;
import org.apache.calcite.rel.core.TableSink;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.tvr.rels.TvrVirtualSpool;
import org.apache.calcite.rel.tvr.rels.VirtualRoot;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.util.VersionInterval;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static org.apache.calcite.rel.tvr.utils.TvrUtils.progressiveMatAcrossTvrEnabled;

public class SubsetReuseStatsCollector {
  private static final Log LOG = LogFactory.getLog(VolcanoPlanner.class);

  private final Map<RelSubset, Map<RelSubset, Integer>> visited;
  private final List<Predicate<? super Map.Entry<RelSubset, Integer>>> predicates;
  private final Map<RelSubset, Integer> allCand;

  public SubsetReuseStatsCollector(RelSubset root) {
    visited = new HashMap<>();
    predicates = new ArrayList<>();
    allCand = visit(root);
  }

  public SubsetReuseStatsCollector addPruningPredicate(Predicate<? super Map.Entry<RelSubset, Integer>> predicate) {
    predicates.add(predicate);
    return this;
  }

  private Map<RelSubset, Integer> getReuseCandidates() {
    Stream<Map.Entry<RelSubset, Integer>> stream = allCand.entrySet().stream();
    for (Predicate<? super Map.Entry<RelSubset, Integer>> predicate : predicates) {
      stream = stream.filter(predicate);
    }
    return stream.collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Detects shareability for equivalence nodes.
   * Section 4.1 in "Efficient and Extensible Algorithms for Multi Query Optimization".
   */
  private Map<RelSubset, Integer> visit(RelSubset subset) {
    Map<RelSubset, Integer> degreeOfShare = visited.get(subset);
    if (degreeOfShare != null) {
      return degreeOfShare;
    }
    visited.put(subset, Collections.emptyMap());

    if (subset.best == null || subset.bestCost.isInfinite()) {
      degreeOfShare = Collections.emptyMap();
    } else {
      degreeOfShare = new HashMap<>();
      if (!(subset.best instanceof TableSink || subset.best instanceof AdhocSink
          || subset.best instanceof VirtualRoot
          || subset.best instanceof TvrVirtualSpool
          || subset.best instanceof Values)) {
        degreeOfShare.put(subset, 1);
      }

      for (RelNode rel : subset.getRels()) {
        if (TvrUtils.isOdpsLogicalOperator(rel) || !TvrUtils.notConverter(rel)) {
          continue;
        }

        Map<RelSubset, Integer> local = new HashMap<>();
        for (RelNode input : rel.getInputs()) {
          Map<RelSubset, Integer> lcl = new HashMap<>();
          ((RelSubset) input).getSubsetsSatisfingThis().forEach(s -> {
            Map<RelSubset, Integer> ret = visit(s);
            mergeByMax(lcl, ret);
          });

          mergeBySum(local, lcl);
        }

        mergeByMax(degreeOfShare, local);
      }
    }

    visited.put(subset, degreeOfShare);
    return degreeOfShare;
  }

  private void mergeByMax(Map<RelSubset, Integer> target, Map<RelSubset, Integer> delta) {
    if (target.isEmpty()) {
      target.putAll(delta);
      return;
    }
    for (Map.Entry<RelSubset, Integer> p : delta.entrySet()) {
      target.merge(p.getKey(), p.getValue(), Integer::max);
    }
  }

  private void mergeBySum(Map<RelSubset, Integer> target, Map<RelSubset, Integer> delta) {
    if (target.isEmpty()) {
      target.putAll(delta);
      return;
    }
    for (Map.Entry<RelSubset, Integer> p : delta.entrySet()) {
      target.merge(p.getKey(), p.getValue(), Integer::sum);
    }
  }

  public Set<ReuseCandidate> getReuseCandidates(
          TvrContext ctx, MultiQueryCostOptimizer opt) {
    final boolean matAcrossTvr = progressiveMatAcrossTvrEnabled(ctx);
    TvrMetaSetType defaultTvrType = ctx.getDefaultTvrType();
    TvrVersion[] allSnapshots = defaultTvrType.getSnapshots();
    VersionInterval[] allDeltas = defaultTvrType.getDeltas();
    Set<VersionInterval> allDeltaSet = allDeltas == null ? null : ImmutableSet.copyOf(allDeltas);

    Set<RelSubset> computed = new HashSet<>();
    Set<ReuseCandidate> reuseCandidates = new HashSet<>();
    getReuseCandidates().forEach((k, v) -> {
      RelOptCost benefit = opt.potentialBenefit(k, v);
      assert benefit != null;
      if (!matAcrossTvr) {
        reuseCandidates.add(new ReuseCandidate(ImmutableSet.of(k), benefit));
        return;
      }
      // matAcrossTvr == true
      Set<RelSubset> toMat = getAllSubsetsAcrossTvr(k, allSnapshots, allDeltaSet);
      if (computed.contains(k) || toMat == null || toMat.isEmpty()) {
        return;
      }
      for (RelSubset s : toMat) {
        if (s == k) {
          continue;
        }
        Integer reuseCount = allCand.get(s);
        if (reuseCount == null) {
          LOG.warn(s + " not connected to root, skipping candidate " + k);
          return;
        }
        RelOptCost relatedBenefit = opt.potentialBenefit(s, reuseCount);
        assert relatedBenefit != null;
        benefit = benefit.plus(relatedBenefit);
      }
      computed.addAll(toMat);
      reuseCandidates.add(new ReuseCandidate(toMat, benefit));
    });

    return reuseCandidates;
  }

  /**
   * Given a subset s, find all subsets with the same trait, which are connected
   * to a same tvr as s with similar tvrTrait.
   *
   * Similar means that the tvrTrait are exactly the same except for time (from
   * and to).
   */
  private Set<RelSubset> getAllSubsetsAcrossTvr(RelSubset s,
      TvrVersion[] allSnapshots, Set<VersionInterval> allDeltas) {
    // 'allSnapshots' and 'allDeltas' are used to determine
    //  whether a semantics is a delta or a snapshot.
    //  For example, there are two time points named t1 and t2.  (MIN - t1 - t2 - MAX)
    //  we think Semantics@(MIN, t1) can be either a snapshot or a delta,
    //  but Semantics@(MIN, t2) is just a snapshot.
    Set<RelSubset> result = new LinkedHashSet<>();
    if (allDeltas == null || allDeltas.size() == 0) {
      result.add(s);
      return result;
    }

    for (Map.Entry<TvrSemantics, Collection<TvrMetaSet>> entry : s.getTvrLinks()
        .asMap().entrySet()) {
      TvrSemantics tvrTrait = entry.getKey();
      for (TvrMetaSet tvr : entry.getValue()) {
        for (Map.Entry<TvrSemantics, RelSet> e : tvr.tvrSets().entrySet()) {
          TvrSemantics trait = e.getKey();
          RelSet set = e.getValue();
          if (trait.getClass() == tvrTrait.getClass() && trait
              .copy(tvrTrait.fromVersion, tvrTrait.toVersion).equals(tvrTrait)) {

            if (TvrUtils.isSnapshotTime.test(tvrTrait)) {
              // FIXME: hack for range query.
              //  do not choose the candidate set that all the subsets are snapshots.
              // FIXME: add this condition to this 'if' to rollback the hack.
              // && !isSnapshotTime.test(trait)

              // tvrTrait.fromVersion is MIN
              // require snapshot
              continue;
            } else if (!TvrUtils.isSnapshotTime.test(tvrTrait)) {
              // tvrTrait.fromVersion is not MIN
              // require delta
              if (TvrUtils.isSnapshotTime.test(trait) && trait.toVersion != allSnapshots[0]) {
                // if trait is the first snapshot, it is also a delta.
                continue;
              } else if (!TvrUtils.isSnapshotTime.test(trait)
                  && !allDeltas.contains(VersionInterval.of(trait.fromVersion, trait.toVersion))) {
                // require the basic delta
                continue;
              }
            }

            RelSubset subset = set.getSubset(s.getTraitSet());
            if (subset == null || !allCand.containsKey(subset)) {
              // If this subset is not in allCand,
              // it means that this subset can not be connected to the root.
              continue;
            }
            result.add(subset);
          }
        }
      }
    }
    return result;
  }
}
