package org.apache.calcite.plan.volcano;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.stream.Collectors;

public class SubsetTopologyGraph {

  private static final Log LOG = LogFactory.getLog(SubsetTopologyGraph.class);

  private static int NEXT_CLIQUE_ID = 0;

  /**
   * A node in the topology graph. Subsets in a clique are ones within subset
   * cycles.
   */
  public static class Clique {
    int id;
    Set<Clique> downStream = new LinkedHashSet<>();
    Set<Clique> upStream = new LinkedHashSet<>();

    // Cycle order is preserved here, so that dirty data drains faster within
    // the cycles.
    LinkedHashSet<RelSubset> subsets = new LinkedHashSet<>();

    Clique() {
      this.id = NEXT_CLIQUE_ID++;
    }

    void addSubset(RelSubset subset) {
      this.subsets.add(subset);
    }

    void addEdgeTo(Clique down) {
      downStream.add(down);
      down.upStream.add(this);
    }

    @Override
    public String toString() {
      return toString(false);
    }

    public String toString(boolean selfOnly) {
      StringBuilder sb = new StringBuilder(id + ": ");
      sb.append(subsets.stream()
          .map(subset -> subset.toString().split("[.]")[0] + ", ")
          .collect(Collectors.joining()));
      if (!selfOnly) {
        sb.append(" upstream: ").append(
            upStream.stream().map(up -> up.id + ", ")
                .collect(Collectors.joining()));
        sb.append(" downstream: ").append(
            downStream.stream().map(down -> down.id + ", ")
                .collect(Collectors.joining()));
      }
      return sb.toString();
    }
  }

  // All known subsets
  Map<RelSubset, Clique> membership = new LinkedHashMap<>();

  // Nodes without parents
  private Set<Clique> sources = new LinkedHashSet<>();

  private List<Clique> topologicalOrder = null;

  public SubsetTopologyGraph() {
  }

  public void expandGraphFromSubset(RelSubset subset) {
    if (membership.containsKey(subset)) {
      return;
    }
    Set<LinkedHashSet<Clique>> cliquesToMerge = new LinkedHashSet<>();
    expandInternal(subset, null, new LinkedHashSet<>(), cliquesToMerge);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Original subset graph before clique merge");
      printGraph();
    }

    // Consistency check
    HashSet<Clique> toMerge = new HashSet<>();
    cliquesToMerge.forEach(mergeSet -> mergeSet.forEach(cli -> {
      boolean ret = toMerge.add(cli);
      assert ret;
    }));

    // Do all the clique merges collected
    cliquesToMerge.forEach(this::mergeCliques);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Subset clique graph after clique merge");
      printGraph();
    }
    checkGraphConsistency();
  }

  private void handleKnownCliqueReached(Clique cur,
      LinkedHashSet<Clique> pathFromSource,
      Set<LinkedHashSet<Clique>> cliquesToMerge) {
    // Reached a known subset, see whether it is a cycle
    Clique cycleFrom;
    if (pathFromSource.contains(cur)) {
      cycleFrom = cur;
    } else {
      /*
       * cur is not in current search path. Check if it is already in a
       * detected clique-to-merge. If so, check to see if any node in the
       * clique-to-merge is in the current path. If so, it is still a new
       * cycle to remember.
       */
      boolean curHasClique = false;
      Set<Clique> pathCopy = new LinkedHashSet<>(pathFromSource);
      for (Set<Clique> mergeSet : cliquesToMerge) {
        if (mergeSet.contains(cur)) {
          curHasClique = true;
          pathCopy.retainAll(mergeSet); // see if any node are in both
          break;
        }
      }
      if (!curHasClique || pathCopy.size() == 0) {
        return;  // No new cycle found
      }
      // This is a new cycle!
      cycleFrom = pathCopy.iterator().next();
    }

    // Cycle detected, save Cliques to merge
    List<Clique> newCycle = new ArrayList<>();
    boolean found = false;
    for (Clique cli : pathFromSource) {
      if (cli.equals(cycleFrom)) {
        found = true;
      }
      if (found) {
        newCycle.add(cli);
      }
    }
    // Cliques edge direction is reverse of subset exploration direction
    // This subset order will be preserved in the final merged clique
    Collections.reverse(newCycle);

    LinkedHashSet<Clique> newMergeSet = new LinkedHashSet<>(newCycle);
    newCycle.forEach(cli -> {
      // Merge existing mergeSets to me if needed
      Iterator<LinkedHashSet<Clique>> iter = cliquesToMerge.iterator();
      while (iter.hasNext()) {
        Set<Clique> mergeSet = iter.next();
        if (mergeSet.contains(cli)) {
          iter.remove();
          newMergeSet.addAll(mergeSet);
        }
      }
    });
    cliquesToMerge.add(newMergeSet);
  }

  private void expandInternal(RelSubset subsetToExpand, Clique parentSubsetCli,
      LinkedHashSet<Clique> pathFromSource,
      Set<LinkedHashSet<Clique>> cliquesToMerge) {
    Set<RelSubset> relatedSubsets = new LinkedHashSet<>();
    relatedSubsets.add(subsetToExpand);
    // Some sibling subsets are also considered parentSubset's children
    relatedSubsets.addAll(
        subsetToExpand.getSubsetsSatisfingThis().collect(Collectors.toList()));
    relatedSubsets.forEach(subset -> {
      // Ignore useless subsets
      if (subset.best == null || subset.bestCost.isInfinite()) {
        return;
      }

      Clique cur = membership.get(subset);
      if (cur != null) {
        if (parentSubsetCli != null) {
          cur.addEdgeTo(parentSubsetCli);
        }
        // A known subset, see if it is a new cycle to remember
        handleKnownCliqueReached(cur, pathFromSource, cliquesToMerge);
        return;
      }

      // A new subset
      cur = new Clique();
      cur.addSubset(subset);
      membership.put(subset, cur);
      if (parentSubsetCli != null) {
        cur.addEdgeTo(parentSubsetCli);
      }

      boolean isLeafSubset = true;
      pathFromSource.add(cur);
      for (RelNode rel : subset.getRelList()) {
        if (TvrUtils.isOdpsLogicalOperator(rel) || !TvrUtils
            .notConverter(rel)) {
          continue;
        }
        for (RelNode input : rel.getInputs()) {
          RelSubset s = (RelSubset) input;
          expandInternal(s, cur, pathFromSource, cliquesToMerge);
          isLeafSubset = false;
        }
      }
      pathFromSource.remove(cur);

      if (isLeafSubset) {
        sources.add(cur);
      }
    });
  }

  private void mergeCliques(Set<Clique> mergeSet) {
    Clique main = new Clique();

    for (Clique cur : mergeSet) {
      cur.upStream.forEach(up -> {
        boolean ret = up.downStream.remove(cur);
        assert ret;
        if (!mergeSet.contains(up)) {
          up.addEdgeTo(main);
        }
      });
      cur.upStream.clear();
      cur.downStream.forEach(down -> {
        boolean ret = down.upStream.remove(cur);
        assert ret;
        if (!mergeSet.contains(down)) {
          main.addEdgeTo(down);
        }
      });
      cur.downStream.clear();

      cur.subsets.forEach(subset -> {
        main.addSubset(subset);
        Clique ret = membership.put(subset, main);
        assert ret != null;
      });
      cur.subsets.clear();
    }

    boolean isSource = sources.removeAll(mergeSet);
    assert !isSource;

    if (main.upStream.isEmpty()) {
      sources.add(main);
    }
  }

  public List<Clique> deriveTopologicalOrder() {
    if (topologicalOrder != null) {
      return topologicalOrder;
    }
    assert !membership.isEmpty();

    Map<Clique, Set<Clique>> upStreamMap = new HashMap<>();
    membership.values().stream().distinct()
        .forEach(cli -> upStreamMap.put(cli, new HashSet<>(cli.upStream)));

    int count = upStreamMap.size();
    List<Clique> result = new ArrayList<>();

    TopoOrderQueue<Clique> frontLine = new TopoOrderQueue<>(
        Comparator.comparingInt(c -> upStreamMap.get(c).size()));
    frontLine.addAll(sources);
    while (!frontLine.isEmpty()) {
      Clique c = frontLine.poll();
      assert upStreamMap.get(c).isEmpty();

      result.add(c);
      c.downStream.forEach(cli -> {
        upStreamMap.get(cli).remove(c);
        frontLine.offer(cli);
      });
    }
    assert
        result.size() == count && result.stream().distinct().count() == count;

    topologicalOrder = ImmutableList.copyOf(result);
    if (LOG.isDebugEnabled()) {
      LOG.debug("topology order of " + topologicalOrder.size() + " cliques:\n"
          + topologicalOrder.stream().map(cli -> cli.toString(true) + "\n")
          .collect(Collectors.joining()));
    }

    return topologicalOrder;
  }

  /**
   * ------------ Debugging methods --------------
   */

  public void printGraph() {
    LOG.debug(
        membership.size() + " relevant subsets within " + membership.values()
            .stream().distinct().count()
            + " cliques (topological graph nodes)");
    LOG.debug("sources: ");
    sources.forEach(cli -> LOG.debug(cli.toString(true)));

    LOG.debug("digraph {");
    membership.values().stream().distinct().forEach(cli -> {
      cli.downStream.forEach(down -> {
        LOG.debug(
            "  \"" + cli.toString(true) + "\" -> \"" + down.toString(true)
                + "\"");
      });
    });
    LOG.debug("}");
  }

  public void checkGraphConsistency() {
    Set<Clique> path = new HashSet<>();
    Set<Clique> visited = new HashSet<>();
    sources.forEach(cli -> {
      assert cli.upStream.isEmpty();
      consistencyCheckVisit(cli, visited, path);
    });

    long cliqueCount = membership.values().stream().distinct().count();
    assert
        visited.size() == cliqueCount : visited.size() + " vs " + cliqueCount;
  }

  private void consistencyCheckVisit(Clique cur, Set<Clique> visited,
      Set<Clique> path) {
    assert !path.contains(cur) :
        "Cycle found! " + path.stream().map(cli -> cli.toString(true) + " ")
            .collect(Collectors.joining());
    if (visited.contains(cur)) {
      return;
    }
    visited.add(cur);
    path.add(cur);
    cur.upStream.forEach(cli -> {
      assert cli.downStream.contains(cur);
    });
    cur.downStream.forEach(cli -> {
      assert cli.upStream.contains(cur);
      consistencyCheckVisit(cli, visited, path);
    });
    path.remove(cur);
  }

}
