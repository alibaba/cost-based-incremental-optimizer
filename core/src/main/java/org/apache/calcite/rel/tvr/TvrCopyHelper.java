package org.apache.calcite.rel.tvr;

import com.google.common.collect.*;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.tvr.utils.ProgressiveMetrics;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.VersionInterval;

import java.util.PriorityQueue;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.calcite.plan.tvr.TvrVersion.MAX;
import static org.apache.calcite.plan.tvr.TvrVersion.MIN;

public class TvrCopyHelper {

  private final TvrVolcanoPlanner planner;
  private final ProgressiveMetrics metrics;

  private final Set<RelNode> rels;
  private final IdentityHashMap<RelNode, Integer> topoOrder;
  private final DedupedPriorityQueue<CopyEvent> toCopy;
  private final Map<RelSet, SetMultimap<TvrSemantics, TvrMetaSet>> noTvrs;
  private final Map<RelNode, SetMultimap<TvrMetaSetType, ImmutableList<TvrEquivSemanticsSet>>> patterns;

  // the useless nodes that do not need to be copied
  private static final Set<Class> blackList = ImmutableSet.of(MultiJoin.class);

  TvrCopyHelper(TvrVolcanoPlanner planner, ProgressiveMetrics metrics) {
    this.planner = planner;
    this.metrics = metrics;

    // find all rels to copy from
    this.rels = Sets.newIdentityHashSet();
    planner.getRelList().stream()
        .filter(rel -> !(rel instanceof AbstractConverter))
        .forEach(this.rels::add);

    // build a topological order of all the rels
    this.topoOrder = topoOrder();
    this.toCopy = new DedupedPriorityQueue<>(Comparator.comparingInt(e -> topoOrder.get(e.parent)));

    // infer missing tvrs
    this.noTvrs = Maps.newIdentityHashMap();
    metrics.startInferMissingTvr();
    initLocalMemo();
    metrics.endInferMissingTvr();

    // analyze all patterns
    this.patterns = Maps.newIdentityHashMap();
    metrics.startAnalyzePatterns();
    analyzePatterns();
    metrics.endAnalyzePatterns();
  }

  private IdentityHashMap<RelNode, Integer> topoOrder() {
    SetMultimap<RelNode, RelNode> deps = Multimaps
        .newSetMultimap(Maps.newIdentityHashMap(), Sets::newIdentityHashSet);
    for (RelNode rel : rels) {
      for (RelNode parent : planner.getSubset(rel).getParentRels()) {
        deps.put(parent, rel);
      }
    }

    TopoOrderQueue<RelNode> queue = new TopoOrderQueue<>(
        Comparator.comparingInt((RelNode o) -> deps.get(o).size())
            .thenComparingInt(RelNode::getId));
    rels.forEach(queue::offer);

    IdentityHashMap<RelNode, Integer> order = Maps.newIdentityHashMap();
    while (!queue.isEmpty()) {
      RelNode rel = queue.poll();
      order.put(rel, order.size());
      for (RelNode parent : planner.getSubset(rel).getParentRels()) {
        deps.remove(parent, rel);
        queue.heapify(parent);
      }
    }

    return order;
  }

  private void initLocalMemo() {
    Set<RelNode> withoutTvrs = Sets.newIdentityHashSet();
    for (RelNode rel : rels) {
      RelSet set = planner.getSet(rel);
      if (set.hasTvrLink()) {
        continue;
      }
      noTvrs.putIfAbsent(set, HashMultimap.create());
      withoutTvrs.add(rel);
    }

    PriorityQueue<RelNode> heap = new PriorityQueue<>(Comparator.comparing(this.topoOrder::get));

    boolean changed;
    do {
      changed = false;
      withoutTvrs.forEach(heap::offer);
      while (!heap.isEmpty()) {
        RelNode rel = heap.poll();
        InferHelper inferHelper = new InferHelper(rel);
        if (inferHelper.canInfer()) {
          if (inferHelper.infer()) {
            changed = true;
          }
          withoutTvrs.remove(rel);
        }
      }
    } while (changed);
  }

  class InferHelper {
    private final RelNode parent;
    private final RelSet set;
    private final SetMultimap<TvrSemantics, TvrMetaSet> pTvrLinks;
    private final List<SetMultimap<TvrSemantics, TvrMetaSet>> cTvrLinks;

    private final LinkedList<TvrDummySemantics> candidates;
    private boolean changed;

    InferHelper(RelNode parent) {
      this.parent = parent;
      RelSubset subset = planner.getSubset(this.parent);
      this.set = planner.getSet(subset);
      pTvrLinks = noTvrs.get(this.set);
      cTvrLinks = parent.getInputs().stream()
          .map(s -> getTvrLinks(planner.getSet(s)))
          .collect(Collectors.toList());

      candidates = new LinkedList<>();
    }

    boolean canInfer() {
      return cTvrLinks.stream().noneMatch(Multimap::isEmpty);
    }

    public boolean infer() {
      for (RelNode input : parent.getInputs()) {
        if (planner.getSet(input) == set) {
          return false;
        }
      }
      changed = false;
      match(0, null);
      return changed;
    }

    private void match(int i, TvrMetaSetType tvrType) {
      if (i == cTvrLinks.size()) {
        List<TvrDummySemantics> dedup = candidates.size() == 1 ?
            candidates :
            candidates.stream().distinct().sorted(
                Comparator.<TvrDummySemantics, TvrVersion>comparing(
                    s -> s.fromVersion).thenComparing(s -> s.toVersion))
                .collect(Collectors.toList());
        switch (dedup.size()) {
        case 0:
          // for values
          break;
        case 1:
          if (pTvrLinks.put(dedup.get(0), new TvrDummyMetaSet(tvrType))) {
            changed = true;
          }
          break;
        case 2:
          if (parent instanceof MultiJoin) {
            TvrDummySemantics s1 = dedup.get(0);
            TvrDummySemantics s2 = dedup.get(1);
            TvrSemantics newSemantics;
            if (s2.fromVersion.compareTo(s1.fromVersion) <= 0
                && s2.toVersion.compareTo(s1.toVersion) >= 0) {
              // the first time interval is included in the second time interval
              newSemantics = s1;
            } else {
              newSemantics = s2;
            }

            if (pTvrLinks.put(new TvrDummySemantics(newSemantics.fromVersion,
                newSemantics.toVersion), new TvrDummyMetaSet(tvrType))) {
              changed = true;
            }
            break;
          } else if (parent instanceof Union) {
            TvrDummySemantics newSemantics = inferUnionSemantics(dedup.get(0), dedup.get(1));
            if (newSemantics == null) {
              throw new UnsupportedOperationException("Unknown pattern: " + parent.getDigest());
            }

            if (pTvrLinks.put(newSemantics, new TvrDummyMetaSet(tvrType))) {
              changed = true;
            }
            break;
          } else {
            throw new UnsupportedOperationException("Unknown pattern: " + parent.getDigest());
          }
        default:
          throw new UnsupportedOperationException("Unknown pattern: " + parent.getDigest());
        }
        return;
      }

      for (Map.Entry<TvrSemantics, TvrMetaSet> e : cTvrLinks.get(i).entries()) {
        TvrMetaSet tvr = e.getValue();
        if (i == 0) {
          tvrType = tvr.getTvrType();
        } else if (!tvrType.equals(tvr.getTvrType())) {
          continue;
        }
        TvrSemantics semantics = e.getKey();
        if (!candidates.stream().allMatch(semantics::timeRangeOverlaps)) {
          continue;
        }
        candidates.addLast(new TvrDummySemantics(semantics.fromVersion, semantics.toVersion));
        match(i + 1, tvrType);
        candidates.removeLast();
      }
    }

    private TvrDummySemantics inferUnionSemantics(TvrDummySemantics t1, TvrDummySemantics t2) {
      // case 1: delta diff
      if (t1.fromVersion.equals(t2.fromVersion)) {
        assert (t2.toVersion.compareTo(t1.toVersion) > 0);
        return new TvrDummySemantics(t1.toVersion, t2.toVersion);
      }

      // case 2: delta union
      if (t1.toVersion.equals(t2.fromVersion)) {
        return new TvrDummySemantics(t1.fromVersion, t2.toVersion);
      }

      return null;
    }
  }

  private void analyzePatterns() {
    for (RelNode rel : rels) {
      AnalyzeHelper helper = new AnalyzeHelper(rel);
      helper.analyze();
    }
  }

  class AnalyzeHelper {
    private final RelNode parent;
    private final SetMultimap<TvrSemantics, TvrMetaSet> pTvrLinks;
    private final List<SetMultimap<TvrSemantics, TvrMetaSet>> cTvrLinks;

    private final LinkedList<TvrEquivSemanticsSet> candidates;

    private final LinkedList<SetMultimap<TvrEquivSemanticsSet, TvrMetaSet>>
        preprocessingTvrs;

    AnalyzeHelper(RelNode parent) {
      this.parent = parent;
      pTvrLinks = getTvrLinks(planner.getSet(this.parent));
      cTvrLinks = parent.getInputs().stream()
          .map(s -> getTvrLinks(planner.getSet(s)))
          .collect(Collectors.toList());

      candidates = new LinkedList<>();
      preprocessingTvrs = new LinkedList<>();
    }

    /**
     * Integrate tvr links (group by TvrMetaSet and TvrVersions)
     * to reduce the number of tvr matching,
     * and all the equivalent tvr links will be saved in a TvrEquivSemanticsSet.
     *
     * For example, there is a multi-join with N inputs and
     * it may take 2^(N+1) attempts to traverse all valid matches in this case.
     *
     *      MultiJoin         MultiJoin.links = [Set Snapshot MIN-8, Set Delta+ MIN-8]
     *      /\ \ \ \
     *    R1 R2 ... RN        Ri.links = [Set Snapshot MIN-8, Set Delta+ MIN-8]
     *
     * But if the tvr links have been integrated in the beginning,
     * it only takes one attempt to complete the match.
     *
     *      MultiJoin         MultiJoin.links = [TvrEquivSemanticsSet MIN-8]
     *      /\ \ \ \
     *    R1 R2 ... RN        Ri.links = [TvrEquivSemanticsSet MIN-8]
     */
    private void preprocessing() {
      Stream.concat(cTvrLinks.stream(), Stream.of(pTvrLinks)).forEach(links -> {
        SetMultimap<TvrEquivSemanticsSet, TvrMetaSet> setMultimap = Multimaps
            .newSetMultimap(Maps.newIdentityHashMap(), Sets::newIdentityHashSet);

        Map<TvrMetaSet, Map<Pair<TvrVersion, TvrVersion>, Set<TvrSemantics>>>
            m = new HashMap<>();
        links.entries().forEach(e -> {
          Map<Pair<TvrVersion, TvrVersion>, Set<TvrSemantics>> innerMap =
              m.computeIfAbsent(e.getValue(), k -> new HashMap<>());

          TvrSemantics semantics = e.getKey();
          Set<TvrSemantics> s = innerMap.computeIfAbsent(
              Pair.of(semantics.fromVersion, semantics.toVersion),
              k -> new HashSet<>());
          s.add(semantics);
        });

        m.forEach((metaSet, map) -> map.forEach((versionPair, tvrs) -> {
          setMultimap.put(
              new TvrEquivSemanticsSet(versionPair.left, versionPair.right, tvrs,
                  metaSet), metaSet);
        }));

        preprocessingTvrs.add(setMultimap);
      });
    }

    void analyze() {
      preprocessing();
      match(0, null);
    }

    private void match(int i, TvrMetaSetType tvrType) {
      if (i == preprocessingTvrs.size() - 1) {
        Set<TvrVersion> versions = Sets.newIdentityHashSet();
        for (TvrEquivSemanticsSet t : candidates) {
          versions.add(t.fromVersion);
          versions.add(t.toVersion);
        }

        SetMultimap<TvrMetaSetType, ImmutableList<TvrEquivSemanticsSet>> map =
            patterns.get(parent);
        if (map == null) {
          map = Multimaps.newSetMultimap(Maps.newHashMap(), Sets::newHashSet);
          patterns.put(parent, map);
        }

        for (Map.Entry<TvrEquivSemanticsSet, TvrMetaSet> e : preprocessingTvrs.get(i).entries()) {
          TvrEquivSemanticsSet semantics = e.getKey();
          if (i != 0) {
            if (!e.getValue().getTvrType().equals(tvrType)) {
              continue;
            }
            if (!versions.contains(semantics.fromVersion)
                || !versions.contains(semantics.toVersion)) {
              continue;
            }
          } else {
            tvrType = e.getValue().getTvrType();
          }
          candidates.addLast(semantics);
          map.put(tvrType, ImmutableList.copyOf(candidates));
          candidates.removeLast();
        }
        return;
      }

      for (Map.Entry<TvrEquivSemanticsSet, TvrMetaSet> e : preprocessingTvrs.get(i).entries()) {
        TvrMetaSet tvr = e.getValue();
        if (i == 0) {
          tvrType = tvr.getTvrType();
        } else if (!tvrType.equals(tvr.getTvrType())) {
          continue;
        }
        TvrEquivSemanticsSet semantics = e.getKey();
        if (semantics.toVersion == MAX
            || !candidates.stream().allMatch(semantics::timeRangeOverlaps)) {
          continue;
        }
        candidates.addLast(semantics);
        match(i + 1, tvrType);
        candidates.removeLast();
      }
    }
  }

  public void seed(RelSet seed, TvrMetaSetType tvrType) {
    for (RelNode parent : seed.getParentRels()) {
      if (patterns.containsKey(parent)) {
        toCopy.offer(CopyEvent.of(tvrType, parent));
      }
    }
  }

  public void kickOff() {
    metrics.startCopy();
    while (!toCopy.isEmpty()) {
      CopyEvent event = toCopy.poll();
      if (blackList.contains(event.parent.getClass())) {
        continue;
      }

      RelNodeCopyHelper helper =
          new RelNodeCopyHelper(event.tvrMetaSetType, event.parent);
      helper.copy();
    }
    metrics.endCopy();
  }

  private SetMultimap<TvrSemantics, TvrMetaSet> getTvrLinks(RelSet set) {
    SetMultimap<TvrSemantics, TvrMetaSet> tvrLinks = set.getTvrLinks();
    if (tvrLinks.isEmpty()) {
      tvrLinks = noTvrs.get(set);
    }
    return tvrLinks;
  }

  class RelNodeCopyHelper {
    private final TvrMetaSetType type;
    private final RelNode parent;
    private final RelSubset subset;
    private final Set<ImmutableList<TvrEquivSemanticsSet>> pats;

    private ImmutableList<TvrEquivSemanticsSet> pattern;
    private final Map<TvrVersion, TvrVersion> mapping;
    private final LinkedList<RelNode> newInputs;

    private RelNodeCopyHelper(TvrMetaSetType tvrType, RelNode parent) {
      type = tvrType;
      this.parent = parent;
      subset = planner.getSubset(this.parent);
      pats = patterns.get(parent).get(tvrType);

      mapping = new IdentityHashMap<>();
      mapping.put(MIN, MIN);
      newInputs = new LinkedList<>();
    }

    public void copy() {
      for (ImmutableList<TvrEquivSemanticsSet> p : pats) {
        pattern = p;
        if (pattern.stream().anyMatch(t -> !t.tvrMetaSet.getTvrType().equals(type))) {
          continue;
        }

        match(0);
      }
    }

    private void match(int i) {
      TvrEquivSemanticsSet oSemantics = pattern.get(i);
      if (i == pattern.size() - 1) {
        RelNode nParent = parent.copy(parent.getTraitSet(), newInputs);
        TvrMetaSet tvr = oSemantics.tvrMetaSet;
        TvrVersion from = mapping.get(oSemantics.fromVersion);
        TvrVersion to = mapping.get(oSemantics.toVersion);
        for (TvrSemantics nSemantics : oSemantics.copy(from, to).equivTvrs) {
          RelSubset equivSubset =
              tvr.getActiveTvr().getSubset(nSemantics, parent.getTraitSet());
          final RelSubset nSubset = Objects.requireNonNull(planner
              .ensureRegistered(nParent, equivSubset));

          // create all subsets satisfied by tgt to resemble
          // the satisfy-relationship on src
          subset.getSatisfyingSubsets().filter(o -> o != subset)
              .forEach(o -> planner.changeTraits(nSubset, o.getTraitSet()));

          Collection<RelNode> triggerCandidates;
          if (registerTvrLink(nSubset, nSemantics, tvr.getActiveTvr())) {
            // If there is a new tvr link added to a RelSet,
            // it is necessary to ensure that all the parents of the RelSubsets in this RelSet
            // have tried to do the translation symmetry.
            triggerCandidates = planner.getSet(subset).getSubsets().stream()
                .flatMap(s -> s.getParentRels().stream())
                .collect(Collectors.toSet());
          } else if (equivSubset != nSubset) {
            // 'equivSubset != nSubset' means that this RelSubset is newly created.
            triggerCandidates = subset.getParentRels();
          } else {
            triggerCandidates = Collections.emptySet();
          }
          for (RelNode p : triggerCandidates) {
            if (patterns.containsKey(p)) {
              toCopy.offer(CopyEvent.of(type, p));
            }
          }
        }
        return;
      }

      RelNode oInput = parent.getInput(i);
      if (oSemantics.fromVersion == MIN) {
        TvrVersion to = mapping.get(oSemantics.toVersion);
        if (to != null) {
          TvrEquivSemanticsSet nSemantics = oSemantics.copy(MIN, to);
          RelSubset nInput = nSemantics.getRelSubset(oInput.getTraitSet());
          if (nInput != null) {
            newInputs.addLast(nInput);
            match(i + 1);
            newInputs.removeLast();
          }
        } else {
          for (TvrVersion s : type.getSnapshots()) {
            TvrEquivSemanticsSet nSemantics = oSemantics.copy(MIN, s);
            RelSubset nInput = nSemantics.getRelSubset(oInput.getTraitSet());
            if (nInput != null) {
              mapping.put(oSemantics.toVersion, s);
              newInputs.addLast(nInput);
              match(i + 1);
              newInputs.removeLast();
              mapping.remove(oSemantics.toVersion);
            }
          }
        }
      } else {
        for (VersionInterval d : type.getDeltas()) {
          TvrVersion from = mapping.get(oSemantics.fromVersion);
          TvrVersion to = mapping.get(oSemantics.toVersion);
          if (from == null) {
            if (to == null) {
              mapping.put(oSemantics.fromVersion, d.from);
              mapping.put(oSemantics.toVersion, d.to);
            } else {
              if (to != d.to) {
                continue;
              }
              mapping.put(oSemantics.fromVersion, d.from);
            }
          } else {
            if (from != d.from) {
              continue;
            }
            if (to == null) {
              mapping.put(oSemantics.toVersion, d.to);
            } else if (to != d.to) {
              continue;
            }
          }

          TvrEquivSemanticsSet nSemantics = oSemantics.copy(d.from, d.to);
          RelSubset nInput = nSemantics.getRelSubset(oInput.getTraitSet());
          if (nInput != null) {
            newInputs.addLast(nInput);
            match(i + 1);
            newInputs.removeLast();
          }

          if (from == null) {
            mapping.remove(oSemantics.fromVersion);
          }
          if (to == null) {
            mapping.remove(oSemantics.toVersion);
          }
        }
      }
    }

    private boolean registerTvrLink(RelSubset subset, TvrSemantics semantics, TvrMetaSet tvr) {
      RelSet set = planner.getSet(subset);
      if (!(tvr instanceof TvrDummyMetaSet)) {
        if (semantics instanceof TvrDummySemantics) {
          return false;
        }

        return set.addTvrLinkWithSetMerge(planner, semantics, tvr);
      }

      assert semantics instanceof TvrDummySemantics;
      RelSet existing = tvr.getRelSet(semantics);
      if (existing == null) {
        tvr.addTvrLink(semantics, set);
        return true;
      }
      planner.ensureRegistered(subset, existing.getSubsets().get(0));
      return false;
    }

  }
}

class CopyEvent {
  final TvrMetaSetType tvrMetaSetType;
  public final RelNode parent;

  private CopyEvent(TvrMetaSetType tvrMetaSetType, RelNode parent) {
    this.tvrMetaSetType = tvrMetaSetType;
    this.parent = parent;
  }

  @Override
  public int hashCode() {
    return parent.hashCode() * 31 + tvrMetaSetType.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CopyEvent)) {
      return false;
    }
    CopyEvent other = (CopyEvent) obj;
    return parent == other.parent && tvrMetaSetType.equals(other.tvrMetaSetType);
  }

  @Override
  public String toString() {
    return "COPY(" + tvrMetaSetType + ":" + parent + ")";
  }

  public static CopyEvent of(TvrMetaSetType tvrMetaSetType, RelNode parent) {
    return new CopyEvent(tvrMetaSetType, parent);
  }
}

class TvrEquivSemanticsSet {

  public final TvrVersion fromVersion;
  public final TvrVersion toVersion;

  final Set<TvrSemantics> equivTvrs;
  TvrMetaSet tvrMetaSet;

  TvrEquivSemanticsSet(TvrVersion fromVersion, TvrVersion toVersion,
      Set<TvrSemantics> equivTvrs, TvrMetaSet tvrMetaSet) {
    assert equivTvrs != null && !equivTvrs.isEmpty() && tvrMetaSet != null;
    this.fromVersion = fromVersion;
    this.toVersion = toVersion;
    this.equivTvrs = equivTvrs;
    this.tvrMetaSet = tvrMetaSet;
  }

  RelSubset getRelSubset(RelTraitSet traitSet) {
    // update the tvr meta set because it may be merged by other meta sets
    // when registering new tvr links
    tvrMetaSet = tvrMetaSet.getActiveTvr();

    List<RelSubset> subsets = equivTvrs.stream()
        .map(t -> tvrMetaSet.getSubset(t, traitSet))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    RelSubset s = subsets.isEmpty() ? null : subsets.get(0);
    if (subsets.stream().skip(1).anyMatch(subset -> s != subset)) {
      throw new IllegalArgumentException(
          "all the equivalent tvr semantics should indicate the same RelSubset");
    }

    return s;
  }

  public TvrEquivSemanticsSet copy(TvrVersion from, TvrVersion to) {
    Set<TvrSemantics> newEquivTvrs =
        equivTvrs.stream().map(t -> t.copy(from, to))
            .collect(Collectors.toSet());
    return new TvrEquivSemanticsSet(from, to, newEquivTvrs, tvrMetaSet);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TvrEquivSemanticsSet that = (TvrEquivSemanticsSet) o;
    return Objects.equals(fromVersion, that.fromVersion) && Objects
        .equals(toVersion, that.toVersion) && Objects
        .equals(equivTvrs, that.equivTvrs) && Objects
        .equals(tvrMetaSet, that.tvrMetaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fromVersion, toVersion, equivTvrs, tvrMetaSet);
  }

  boolean timeRangeOverlaps(TvrEquivSemanticsSet other) {
    return fromVersion.compareTo(other.toVersion) <= 0
        && toVersion.compareTo(other.fromVersion) >= 0;
  }
}

class TvrDummySemantics extends TvrSemantics {

  TvrDummySemantics(TvrVersion fromVersion, TvrVersion toVersion) {
    super(fromVersion, toVersion);
  }

  @Override
  public TvrSemantics copy(TvrVersion fromVersion, TvrVersion toVersion) {
    return new TvrDummySemantics(fromVersion, toVersion);
  }
}

class TvrDummyMetaSet extends TvrMetaSet {

  TvrDummyMetaSet(TvrMetaSetType type) {
    super(type);
  }

  @Override
  public int hashCode() {
    return getTvrType().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TvrDummyMetaSet)) {
      return false;
    }
    TvrDummyMetaSet other = (TvrDummyMetaSet) obj;
    return getTvrType().equals(other.getTvrType());
  }
}

class DedupedPriorityQueue<E> extends PriorityQueue<E> {
  private final Set<E> set = Sets.newHashSet();

  DedupedPriorityQueue(Comparator<E> comparator) {
    super(comparator);
  }

  @Override
  public boolean offer(E e) {
    if (set.add(e)) {
      return super.offer(e);
    }
    return false;
  }

  @Override
  public E poll() {
    E e = super.poll();
    set.remove(e);
    return e;
  }

  @Override
  public void clear() {
    super.clear();
    set.clear();
  }
}
