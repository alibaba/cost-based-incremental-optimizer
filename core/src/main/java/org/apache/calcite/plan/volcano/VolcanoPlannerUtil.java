package org.apache.calcite.plan.volcano;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrSetSemantics;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.tvr.utils.ProgressiveMetrics;
import org.apache.calcite.util.Pair;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by tedxu on 14/04/2017.
 */
public class VolcanoPlannerUtil {
  public static RelNode buildCheapestPlan(VolcanoPlanner planner,
      RelSubset rel) {
    return rel.buildCheapestPlan(planner);
  }

  public static void printAllJoins(VolcanoPlanner planner) {
    throw new UnsupportedOperationException("printAllJoins unsupported");
//    RelOptCluster cluster = planner.getRoot().getCluster();
//    RelTraitSet defaultPhysical = cluster.traitSet().replace(OdpsRel.CONVENTION);
//
//    Map<Integer, String> logicalJoins = new HashMap<>();
//    Map<Integer, String> physicalJoins = new HashMap<>();
//    List<Integer> maxLogicalJoins = new ArrayList<>();
//    List<Integer> maxPhysicalJoins = new ArrayList<>();
//
//    int streamingTableCountThre = 100;
//    List<Integer> disableForStrictStreaming = new ArrayList<>();
//    List<Integer> snapshotInnerJoinOnlyJoins = new ArrayList<>();
//
//    for (RelSet set : planner.allSets) {
//      for (RelNode rel : set.getRelsFromAllSubsets()) {
//        if (rel instanceof OdpsMultiJoin) {
//          Set<Pair<String, TvrSetSemantics>> tablesAndTvrs =
//              TvrUtils.collectTableNamesAndTvrs(rel);
//          boolean maxJoin = TvrUtils.isSnapshotMaxJoin((OdpsMultiJoin) rel);
//          if (maxJoin)  {
//            if (rel.getConvention() == Convention.NONE) {
//              maxLogicalJoins.add(rel.getId());
//            } else {
//              maxPhysicalJoins.add(rel.getId());
//
//              if (!joinChildrenHasLeftAntiJoin(rel) && tablesAndTvrs.size()
//                  <= streamingTableCountThre) {
//                disableForStrictStreaming.add(rel.getId());
//              }
//            }
//          }
//          String str = "rel#" + rel.getId() + "-" + rel.getRelTypeName() + ", "
//              + tablesAndTvrs.size() + " tables, "
//              + TvrUtils.getJoinStrDebug(rel) + ", in set#"
//              + planner.getSet(rel).getId() + ", subset#" + planner.getSubset(rel)
//              .getId();
//          RelSubset defaultPhysicalSubset =
//              planner.getSet(rel).getSubset(defaultPhysical);
//          str += ", defaultPhysicalSubset#" + (defaultPhysicalSubset == null ?
//              "null" :
//              defaultPhysicalSubset.getId());
//          if (rel.getConvention() == Convention.NONE) {
//            logicalJoins.put(rel.getId(), str);
//          } else {
//            physicalJoins.put(rel.getId(), str);
//
//            // If streaming only, disable all non inner and left anti joins
//            JoinType joinType = TvrJoinUtils.getLogicalJoinType(rel);
//            if (!joinType.equals(JoinType.INNER) && !joinType
//                .equals(JoinType.LEFT_ANTI)
//                && tablesAndTvrs.size() <= streamingTableCountThre) {
//              disableForStrictStreaming.add(rel.getId());
//            }
//            if (maxJoin && joinChildrenInnerJoinOnly(rel)) {
//              snapshotInnerJoinOnlyJoins.add(rel.getId());
//            }
//          }
//        }
//      }
//    }
//    System.out.println("all joins:\n");
//    logicalJoins.keySet().stream().sorted().forEach(k -> {
//      System.out.println(logicalJoins.get(k));
//    });
//    physicalJoins.keySet().stream().sorted().forEach(k -> {
//      System.out.println(physicalJoins.get(k));
//    });
//    System.out.println();
//
//    System.out.println("joins to disable for strict streaming:");
//    System.out.println(disableForStrictStreaming.stream().sorted()
//        .collect(Collectors.toList()));
//
//    System.out.println("snapshot max joins: ");
//    System.out.println("physical: " + maxPhysicalJoins.stream().sorted().collect(Collectors.toList()));
//    maxPhysicalJoins.stream().sorted().forEach(maxJoin -> {
//      System.out.println(physicalJoins.get(maxJoin));
//    });
//    System.out.println();
//
//    System.out.println("snapshotInnerJoinOnlyJoins:");
//    snapshotInnerJoinOnlyJoins.stream().sorted().forEach(maxJoin -> {
//      System.out.println(physicalJoins.get(maxJoin));
//    });
//    System.out.println();
//
//    System.out.println("not all max snapshot not delta joins: ");
//    physicalJoins.keySet().stream().sorted().forEach(k -> {
//      if (maxPhysicalJoins.contains(k)) {
//        return;
//      }
//      if (physicalJoins.get(k).contains("11, 22")) {
//        return;
//      }
//      System.out.println(physicalJoins.get(k));
//    });
//
//    System.out.println();
  }

  public static void printAllTvrInfo(VolcanoPlanner volcanoPlanner) {
    System.out.println("all tvr info:");

    // add tvr info
    List<TvrMetaSet> tvrs = volcanoPlanner.allSets.stream()
        .flatMap(set -> set.tvrLinks.values().stream()).distinct()
        .sorted(Comparator.comparing(t -> t.getTvrId()))
        .collect(Collectors.toList());

    for (TvrMetaSet tvr : tvrs) {
      HashMap<String, Integer> tvrSets = new HashMap<>();
      tvr.tvrSets.forEach((tvrSemantics, set) -> {
        // find the most recent set
        if (set.rel == null) {
          return;
        }
        set = volcanoPlanner.getSet(set.rel);
        tvrSets.put(tvrSemantics.toString(), set.id);
      });

      ImmutableMap.Builder<String, TvrMetaSet> tvrPropertyLinks = ImmutableMap.builder();
      tvr.tvrPropertyLinks.forEach((tvrProperty, toTvr) -> {
        tvrPropertyLinks.put(tvrProperty.toString(), toTvr);
      });

      StringBuilder sb = new StringBuilder();
      sb.append("tvr#").append(tvr.getTvrId()).append(" - ")
          .append(tvr.getTvrType());
      sb.append("\n");

      sb.append("tvr sets: \n");
      for (Map.Entry<TvrSemantics, RelSet> entry : tvr.tvrSets.entrySet()) {
        TvrSemantics tvrSemantics = entry.getKey();
        RelSet set = entry.getValue();
        sb.append("\t");
        sb.append(tvrSemantics.toString()).append(" - ").append("set#")
            .append(set.getId())
            .append("\n");
      }

      sb.append("tvr property links: \n");
      if (tvr.tvrPropertyLinks.isEmpty()) {
        sb.append("\t").append("empty").append("\n");
      }
      for (Map.Entry<TvrProperty, TvrMetaSet> entry : tvr.tvrPropertyLinks.entrySet()) {
        sb.append("\t");
        TvrProperty tvrProperty = entry.getKey();
        TvrMetaSet toTvr = entry.getValue();
        sb.append(tvrProperty.toString()).append(" - ")
            .append("tvr#").append(toTvr.tvrId)
            .append("\n");
      }

      System.out.println(sb.toString());

    }

  }

//  public static boolean joinChildrenHasLeftAntiJoin(RelNode root) {
//    boolean[] ret = new boolean[] { false };
//    Set<RelNode> visited = new LinkedHashSet<>();
//    new RelVisitor() {
//      @Override
//      public void visit(RelNode node, int ordinal, RelNode parent) {
//        if (node == null) {
//          return;
//        }
//        if (visited.contains(node)) {
//          return;
//        }
//        visited.add(node);
//        super.visit(node, ordinal, parent);
//        if (node instanceof RelSubset) {
//          RelNode original = ((RelSubset) node).getOriginal();
//          if (original != null) {
//            visit(original, ordinal, parent);
//          }
//          return;
//        }
//
//        if (node instanceof OdpsMultiJoin && TvrJoinUtils
//            .getLogicalJoinType(node).equals(JoinType.LEFT_ANTI)) {
//          ret[0] = true;
//        }
//      }
//    }.go(root);
//    return ret[0];
//  }

//  public static boolean joinChildrenInnerJoinOnly(RelNode root) {
//    boolean[] ret = new boolean[] { true };
//    Set<RelNode> visited = new LinkedHashSet<>();
//    new RelVisitor() {
//      @Override
//      public void visit(RelNode node, int ordinal, RelNode parent) {
//        if (node == null) {
//          return;
//        }
//        if (visited.contains(node)) {
//          return;
//        }
//        visited.add(node);
//        super.visit(node, ordinal, parent);
//        if (node instanceof RelSubset) {
//          RelNode original = ((RelSubset) node).getOriginal();
//          if (original != null) {
//            visit(original, ordinal, parent);
//          }
//          return;
//        }
//
//        if (node instanceof OdpsMultiJoin && !TvrJoinUtils
//            .getLogicalJoinType(node).equals(JoinType.INNER)) {
//          ret[0] = false;
//        }
//      }
//    }.go(root);
//    return ret[0];
//  }

  public static Map<RelNode, ?> provenance(VolcanoPlanner planner) {
    return planner.provenanceMap;
  }



  public static void recordMemoMetrics(VolcanoPlanner planner,
      ProgressiveMetrics pm) {
    pm.setRelSetCountTotal(planner.allSets.size());
    planner.allSets.forEach(set -> {
      set.getRelsFromAllSubsets().forEach(rel -> {
        Integer n = pm.relNodeCount
            .computeIfAbsent(rel.getClass(), x -> new Integer(0));
        pm.relNodeCount.put(rel.getClass(), n + 1);
      });
    });
    pm.setRelNodeCountTotal(
        pm.relNodeCount.entrySet().stream().mapToInt(entry -> entry.getValue())
            .sum());
  }

  public static String printRelSets(List<RelSet> allSets, Set<Integer> setIds) {
    StringBuilder str = new StringBuilder();
    allSets.stream()
        .filter(set -> setIds.isEmpty() || setIds.contains(set.getId()))
        .forEach(set -> {
          str.append("Set " + set.getId() + ": " + (set.rel == null ? "": set.rel.toString()) + "\n");

          str.append("  ");
          set.getSubsets().forEach(subset -> str.append(subset.toString() + ", "));
          str.append("\n\n");

          set.tvrLinks.asMap().forEach((tvrKey, tvrMetaSets) -> {
            str.append("  " + (tvrKey.equals(TvrSemantics.SET_SNAPSHOT_MAX) ? "SSMax": tvrKey.toString()) + " --> ");
            tvrMetaSets.forEach(tvr -> str.append("tvr" + tvr.getTvrId() + ", "));
            str.append("\n");
          });
          str.append("\n");
        });
    return str.toString();
  }
}

