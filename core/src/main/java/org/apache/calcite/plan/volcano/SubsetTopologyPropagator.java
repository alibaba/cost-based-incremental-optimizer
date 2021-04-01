package org.apache.calcite.plan.volcano;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.volcano.SubsetTopologyGraph.Clique;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SubsetTopologyPropagator implements Iterator<RelSubset> {

  private static final Log LOG =
      LogFactory.getLog(SubsetTopologyPropagator.class);

  // ----------- Constant variables -----------
  private SubsetTopologyGraph graph;
  private Set<RelSubset> knownSubsets;
  private List<Clique> topologicalOrder;
  // An optional custom function that picks the next dirtySubset within a clique
  private Function<LinkedHashSet<RelSubset>, RelSubset> chooseSubsetInClique;

  // ----------- For consistency check only -----------
  private Map<Clique, Set<RelSubset>> downStreamSubsets;

  // ----------- Working variables -----------
  private RelSubset lastSubset;
  private Set<RelSubset> dirtySubsets;
  private List<Clique> topologicalOrderCopy;

  public SubsetTopologyPropagator(SubsetTopologyGraph graph,
      Function<LinkedHashSet<RelSubset>, RelSubset> chooseSubsetInClique) {
    this.graph = graph;
    this.knownSubsets = new HashSet<>(graph.membership.keySet());
    this.topologicalOrder = graph.deriveTopologicalOrder();
    this.chooseSubsetInClique = chooseSubsetInClique;
    assert knownSubsets.size() > 0;

    // For consistency check only
    downStreamSubsets = new HashMap<>();
    topologicalOrder.forEach(cli -> {
      Set<RelSubset> downStreamSubset = new HashSet<>();
      cli.downStream.forEach(c -> downStreamSubset.addAll(c.subsets));
      downStreamSubset.addAll(cli.subsets);
      downStreamSubsets.put(cli, downStreamSubset);
    });

    // Initialize working variables
    dirtySubsets = new LinkedHashSet<>();
    reset(false);
  }

  public synchronized void reset(boolean force) {
    if (!force && hasNext()) {
      throw new RuntimeException(
          "Should not reset while there's still more dirty data");
    }
    dirtySubsets.clear();
    lastSubset = null;
    topologicalOrderCopy = new ArrayList<>(topologicalOrder);
  }

  @Override
  public synchronized boolean hasNext() {
    return !dirtySubsets.isEmpty();
  }

  @Override
  public synchronized RelSubset next() {
    // Use LinkedHashSet to preserve subset order within clique
    LinkedHashSet<RelSubset> subsets = new LinkedHashSet<>();
    Iterator<Clique> iter = topologicalOrderCopy.iterator();
    while (iter.hasNext()) {
      Clique cli = iter.next();
      subsets.addAll(cli.subsets);
      subsets.retainAll(dirtySubsets);
      if (subsets.size() > 0) {
        break;
      }
      // We don't need this any more since dirty subsets only move backwards
      // in the topological list
      iter.remove();
    }
    assert !subsets.isEmpty();

    RelSubset newSubset;
    if (chooseSubsetInClique != null) {
      newSubset = chooseSubsetInClique.apply(subsets);
    } else {
      newSubset = subsets.iterator().next();
    }

    if (lastSubset == newSubset) {
      LOG.debug(
          "Picked same RelSubset " + lastSubset + ", a potential dead loop");
    }
    lastSubset = newSubset;
    dirtySubsets.remove(lastSubset);
    return lastSubset;
  }

  public synchronized void addDirtySubset(RelSubset newDirty) {
    addDirtySubsets(ImmutableList.of(newDirty));
  }

  public synchronized void addDirtySubsets(Collection<RelSubset> newDirty) {
    Set<RelSubset> dirty = filterUnknown(newDirty, false);
    // consistency check
    if (lastSubset != null && !downStreamSubsets.get(getClique(lastSubset))
        .containsAll(dirty)) {
      LOG.error(
          "lastSubset " + lastSubset + " in Clique " + getClique(lastSubset)
              .toString(true));
      LOG.error("dirty: " + dirty.stream().map(s -> s.toString() + " ")
          .collect(Collectors.joining()));
      LOG.error(
          "possible dirty set: " + downStreamSubsets.get(getClique(lastSubset))
              .stream().map(s -> s.toString() + " ")
              .collect(Collectors.joining()));
      assert false : "consistency check failed";
    }
    dirtySubsets.addAll(dirty);
  }

  private Set<RelSubset> filterUnknown(Collection<RelSubset> subsets,
      boolean warn) {
    Set<RelSubset> ret = new HashSet<>(subsets);
    ret.retainAll(knownSubsets);
    int unknown = subsets.size() - ret.size();
    if (warn && unknown > 0) {
      LOG.warn(unknown + " subsets ignored");
    }
    return ret;
  }

  private Clique getClique(RelSubset subset) {
    return graph.membership.get(subset);
  }
}
