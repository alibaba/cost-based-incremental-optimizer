package org.apache.calcite.plan.volcano;


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import org.apache.calcite.adapter.enumerable.EnumerableValues;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.rel.temp.PhysicalTableSink;
import org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrJsonUtils;
import org.apache.calcite.rel.tvr.utils.TvrTableUtils;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.calcite.rel.tvr.utils.TvrJsonUtils.createTvrGson;
import static org.apache.calcite.rel.tvr.utils.TvrSinkUtils.buildSinkForDownStream;
import static org.apache.calcite.rel.tvr.utils.TvrTableUtils.getProjectName;
import static org.apache.calcite.rel.tvr.utils.TvrTableUtils.getStateTableName;


/**
 * Abstraction of an TvrMetaSet for de/serialization. Used to capture the
 * structural graph of related tvrs and materialized tables related to the
 * original query output table. Downstream query can then deserialize and
 * reconstruct the graph for its tableScan.
 */
public class TvrNode implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  public static class TvrGraphConstructor extends TvrVisitor {

    // Input tvr - table Sink can be 1 to n mapping, this constructor is per
    // <inputTvr, Sink> pair
    TvrMetaSet rootTvr;
    PhysicalTableSink oriSink;

    Map<RelSubset, Pair<List<PhysicalTableSink>, MatType>> targetSubsets;
    Map<PhysicalTableSink, RelSubset> sinkMapping;

    RelDataTypeFactory typeFactory;
    TvrContext ctx;

    // --------- output variables ------------
    Map<TvrMetaSet, TvrNode> visited = new HashMap<>();
    Set<TvrNode> tvrsToKeep = new HashSet<>();
    Set<RelSubset> targetSubsetsFound = new HashSet<>();

    Map<RelSubset, Multimap<TvrSemantics, TvrNode>> subsetToTvrNodes = new IdentityHashMap<>();

    // <subset, <new sink, linked sink>>
    Map<RelSubset, Pair<PhysicalTableSink, PhysicalTableSink>> newSinks = new IdentityHashMap<>();

    TvrGraphConstructor(TvrMetaSet rootTvr, PhysicalTableSink oriSink,
        Map<RelSubset, Pair<List<PhysicalTableSink>, MatType>> targetSubsets,
        Map<PhysicalTableSink, RelSubset> sinkMapping) {
      this.rootTvr = rootTvr;
      this.oriSink = oriSink;
      this.sinkMapping = sinkMapping;
      this.targetSubsets = targetSubsets;
      RelOptCluster cluster = oriSink.getCluster();
      this.typeFactory = cluster.getTypeFactory();
      this.ctx = TvrContext.getInstance(cluster);
    }

    TvrNode buildTvrGraph() {
      // Walk and build the full tvr relationship graph
      go(rootTvr);

      // Generate all (newSink, tvrRelation) for all tvrNodes
      buildAllSinks();

      // Reverse propagate tvrsToKeep to find all tvrNodes to keep
      for (TvrNode tvrNode : new HashSet<>(tvrsToKeep)) {
        reversePropTvrsToKeep(tvrNode);
      }
      LOGGER.info("{} out of {} subsets reached from root tvr {}",
          targetSubsetsFound.size(), targetSubsets.size(), rootTvr.getTvrId());

      TvrNode ret = visited.get(rootTvr);
      assert tvrsToKeep.contains(ret) : "nothing useful found for down stream";

      // Remove useless tvrNodes
      trimGraph();
      return ret;
    }

    @Override
    public void visit(TvrMetaSet tvr, TvrProperty propertyEdge,
        TvrMetaSet parent) {
      if (visited.containsKey(tvr)) {
        visited.get(parent)
            .addPropertyLink(TvrPropertyUtil.toString(propertyEdge),
                visited.get(tvr));
        return;
      }

      TvrNode tvrNode = new TvrNode(tvr.getTvrId(), tvr.getTvrType());
      visited.put(tvr, tvrNode);
      if (parent != null) {
        visited.get(parent)
            .addPropertyLink(TvrPropertyUtil.toString(propertyEdge), tvrNode);
      }

      // If it is the empty values tvr, safe this info
      if (tvr.getRelSet(TvrSemantics.SET_SNAPSHOT_MAX).getRelsFromAllSubsets()
          .stream().anyMatch(TvrUtils::isEmpty)) {
        tvrNode.isEmptyValuesTvr = true;
        tvrsToKeep.add(tvrNode);
      }

      tvr.tvrSets().forEach((tvrKey, set) -> set.getSubsets().forEach(subset -> {
        if (targetSubsets.containsKey(subset)) {
          subsetToTvrNodes
              .computeIfAbsent(subset, x -> ArrayListMultimap.create()).
              put(tvrKey, tvrNode);
          // Keep this tvr node because it contains target table
          tvrsToKeep.add(tvrNode);
          targetSubsetsFound.add(subset);
        }
      }));

      // Visit children of tvr
      super.visit(tvr, propertyEdge, parent);
    }

    private void buildAllSinks() {
      subsetToTvrNodes.forEach((subset, tvrLinks) -> {
        // <linked table sinks of this subset, MatType>
        Pair<List<PhysicalTableSink>, MatType> p =
            Objects.requireNonNull(targetSubsets.get(subset));

        List<TvrTableUtils.TvrTableRelation> relations = p.left.stream().map(sink -> {
          RelSubset srcSubset = Objects.requireNonNull(sinkMapping.get(sink));

          // <new table sink, linked table sink>
          Pair<PhysicalTableSink, PhysicalTableSink> sinkPair = newSinks.get(srcSubset);
          PhysicalTableSink inputSink = sinkPair == null ? null : sinkPair.left;

          // When the upstream query decides to materialize a RelSubset,
          // a table A without partition will be created for it.
          // If the downstream query wants to use the result of this RelSubset,
          // it needs a new table B, which probably has partition column.
          // Thus, table B needs to be created here (table A has been created by the CheapestPlanBuilder),
          // and then A will be linked to B.
          if (inputSink == null) {
            EnumerableValues values = EnumerableValues
                .create(srcSubset.getCluster(), srcSubset.getRowType(),
                    ImmutableList.of());

            String tableName =
                getStateTableName(ctx, srcSubset.getId(), p.right) + "_d";
            String projectName = getProjectName(ctx);
            inputSink = buildSinkForDownStream(values, oriSink, p.right,
                srcSubset.getTraitSet(), projectName, tableName, Objects.requireNonNull(
                    TvrTableUtils.getEstimatedRowCount(sink.getTable())));
            newSinks.put(srcSubset, Pair.of(inputSink, sink));
          }
          return TvrTableUtils.TvrTableRelationUtil.of(inputSink);
        }).collect(Collectors.toList());
        assert !relations.isEmpty();

        RelDataType tableSchema = TvrJsonUtils.deserializeRowType(typeFactory, relations.get(0).tableRowType);
        Gson gson = createTvrGson(tableSchema, typeFactory);
        tvrLinks.asMap().forEach((tvrKey, tvrNodes) -> {
          if (tvrKey.equals(TvrSemantics.SET_SNAPSHOT_MAX)) {
            tvrNodes.forEach(n -> n.setSnapshotMaxTable = relations);
          } else {
            String tvrKeyStr = gson.toJson(tvrKey, TvrSemantics.class);
            tvrNodes.forEach(n -> n.addTable(tvrKeyStr, relations));
          }
        });
      });
    }

    private void reversePropTvrsToKeep(TvrNode node) {
      assert tvrsToKeep.contains(node);
      node.reverseTvrPropertyLinks.values().stream()
          .flatMap(Collection::stream).distinct()
          .filter(x -> !tvrsToKeep.contains(x)).forEach(x -> {
        tvrsToKeep.add(x);
        reversePropTvrsToKeep(x);
      });
    }

    private void trimGraph() {
      visited.values().stream().filter(node -> tvrsToKeep.contains(node))
          .forEach(node -> {
            node.tvrPropertyLinks.entrySet()
                .removeIf(entry -> !tvrsToKeep.contains(entry.getValue()));
            Iterator<Map.Entry<String, Set<TvrNode>>> iter2 =
                node.reverseTvrPropertyLinks.entrySet().iterator();
            while (iter2.hasNext()) {
              Map.Entry<String, Set<TvrNode>> entry = iter2.next();
              entry.getValue().retainAll(tvrsToKeep);
              if (entry.getValue().isEmpty()) {
                iter2.remove();
              }
            }
          });
    }
  }

  private final int id;
  private boolean isEmptyValuesTvr = false;

  private final TvrMetaSetType tvrType;
  private final Map<String, TvrNode> tvrPropertyLinks = new HashMap<>();
  private final Map<String, Set<TvrNode>> reverseTvrPropertyLinks = new HashMap<>();

  // TvrSemantics -> TvrTableRelation
  private final Multimap<String, List<TvrTableUtils.TvrTableRelation>> tables =
      ArrayListMultimap.create();

  // Whether SetSnapshotMax table is computed
  private List<TvrTableUtils.TvrTableRelation> setSnapshotMaxTable = null;

  public TvrNode(int tvrId, TvrMetaSetType tvrType) {
    id = tvrId;
    this.tvrType = tvrType;
  }

  public TvrMetaSetType getTvrType() {
    return tvrType;
  }

  public List<TvrTableUtils.TvrTableRelation> getSetSnapshotMaxTable() {
    return this.setSnapshotMaxTable;
  }

  public boolean isEmptyValuesTvr() {
    return this.isEmptyValuesTvr;
  }

  public Map<String, Set<TvrNode>> getReverseTvrPropertyLinks() {
    return this.reverseTvrPropertyLinks;
  }

  public Multimap<String, List<TvrTableUtils.TvrTableRelation>> getTables() {
    return this.tables;
  }

  private void addPropertyLink(String propertyStr, TvrNode toTvr) {
    tvrPropertyLinks.put(propertyStr, toTvr);
    toTvr.reverseTvrPropertyLinks
        .computeIfAbsent(propertyStr, x -> new HashSet<>()).add(this);
  }

  private void addTable(String tvrKey, List<TvrTableUtils.TvrTableRelation> tables) {
    this.tables.put(tvrKey, tables);
  }

  public void printGraph(String table) {
    System.out.println("Out tvr related table map for " + table);
    printGraph(new HashSet<>());
  }

  private void printGraph(Set<TvrNode> visited) {
    if (visited.contains(this)) {
      return;
    }
    visited.add(this);
    if (isEmptyValuesTvr) {
      System.out.println("empty values tvr" + id);
    } else {
      System.out.println("tvr" + id);
    }
    if (setSnapshotMaxTable != null) {
      System.out.println(
          "  " + TvrSemantics.SET_SNAPSHOT_MAX + " -> " + setSnapshotMaxTable
              .stream().map(t -> t.tableName).reduce((s1, s2) -> s1 + "," + s2)
              .orElse("null"));
    }
    tables.entries().forEach(e -> System.out.println(
        "  " + e.getKey() + " -> " + e.getValue().stream().map(t -> t.tableName)
            .reduce((s1, s2) -> s1 + "," + s2).orElse("null")));
    tvrPropertyLinks.forEach((property, node) -> System.out
        .println("  " + property + " -> tvr" + node.id));
    tvrPropertyLinks.forEach((property, node) -> node.printGraph(visited));
  }

  public Set<TvrNode> getAllTvrNodes() {
    Set<TvrNode> visited = new HashSet<>();
    getAllTvrNodes(visited);
    return visited;
  }

  private void getAllTvrNodes(Set<TvrNode> visited) {
    if (visited.contains(this)) {
      return;
    }
    visited.add(this);
    tvrPropertyLinks.values().forEach(node -> node.getAllTvrNodes(visited));
  }
}
