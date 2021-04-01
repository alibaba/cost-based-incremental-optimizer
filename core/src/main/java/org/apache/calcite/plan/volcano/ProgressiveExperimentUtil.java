package org.apache.calcite.plan.volcano;

import com.google.common.collect.Sets;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.tvr.rels.TvrVirtualSpool;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


public class ProgressiveExperimentUtil {
  private static final String ODPS_PROGRESSIVE_ROW_COUNT_SPLIT_SIZE =
      "odps.optimizer.row.count.split.size";

  // "MIN_1:MAX_1,MIN_2:MAX_2, ..."
  private static final String ODPS_PROGRESSIVE_ROW_COUNT_ID_RANGE =
      "odps.optimizer.row.count.id.range";

  public static RelNode accelerateDataInsertion(RelNode rel) {
    throw new UnsupportedOperationException("accelerateDataInsertion not supported");
  }

  public static RelNode getRelSetRowCountPlan(VolcanoPlanner planner, Set<RelSubset> subsets) {
    throw new UnsupportedOperationException("getRelSetRowCountPlan not supported");
  }

  public static Set<Integer> getRelSetsForRowCount(Set<RelSubset> subsets) {
    // Returns a sorted set
    return subsets.stream().map(subset -> subset.set.id)
        .sorted(Integer::compareTo)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  public static Set<RelSubset> getRowCountSubsets(VolcanoPlanner planner,
      Set<RelSubset> subsets) {
    throw new UnsupportedOperationException("getRowCountSubsets not supported");
  }

  public static Set<RelSet> getReuseRelSet(VolcanoPlanner planner) {
    Set<RelSet> relSets = new HashSet<>();
    Set<RelSet> joinSets = new HashSet<>();
    RelVisitor visitor = new RelVisitor() {
      private final Set<RelNode> visited = Sets.newIdentityHashSet();

      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (visited.contains(node)) {
          return;
        }
        visited.add(node);

        if (node instanceof RelSubset) {
          for (RelNode rel : ((RelSubset) node).getRelList()) {
            visit(rel, ordinal, parent);
          }
          return;
        }

        RelSet relSet = planner.getSet(node);
        if (!(node instanceof TableScan
                || node instanceof TableSink
//            || node instanceof AdhocSink
                || node instanceof Project
            || node instanceof Filter || node instanceof AbstractConverter
//            || node instanceof VirtualRoot
            || node instanceof TvrVirtualSpool)) {
          relSets.add(relSet);
        }

        // HACK: remove joinSets
        // way better performance with little cost loss
        if (node instanceof Join
//                || node instanceof OdpsMultiJoin
            || node instanceof MultiJoin) {
          joinSets.add(relSet);
        }

        RelSet parentSet = parent == null ? null : planner.getSet(parent);
        if (parentSet != null && joinSets.contains(parentSet)) {
          relSets.add(relSet);
        }
        super.visit(node, ordinal, parent);
      }
    };
    visitor.go(planner.root);

    return relSets;
  }

  static Map<Integer, Double> loadFakeRowCount(TvrContext ctx) {
    String fakeRowCountStr = ctx.getConfig()
        .get(TvrUtils.PROGRESSIVE_FAKE_ROW_COUNT_FILE, null);
    boolean isFilePath = fakeRowCountStr != null;
    fakeRowCountStr = fakeRowCountStr == null ?
        ctx.getConfig()
            .get(TvrUtils.PROGRESSIVE_FAKE_ROW_COUNT_STR, null) :
        fakeRowCountStr;
    if (fakeRowCountStr != null) {
      return parseRelSetRowCount(fakeRowCountStr, isFilePath);
    }
    return null;
  }

  private static Map<Integer, Double> parseRelSetRowCount(
      String fakeRowCountStr, boolean isFilePath) {
    if (fakeRowCountStr == null) {
      return Collections.emptyMap();
    }

    Map<Integer, Double> fakeRowCountMapping = new TreeMap<>();
    StringBuilder str = new StringBuilder();
    if (isFilePath) {
      try {
        for (String s : Files.readAllLines(Paths.get(fakeRowCountStr))) {
          str.append(s.replace("\n", ""));
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      str = new StringBuilder(fakeRowCountStr);
    }

    for (String line : str.toString().split(",")) {
      String[] data = line.split(":");
      assert data.length == 3;
      fakeRowCountMapping
          .put(Integer.parseInt(data[0]), Double.valueOf(data[1]));
    }
    return fakeRowCountMapping;
  }

}
