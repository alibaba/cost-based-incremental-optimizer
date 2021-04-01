package org.apache.calcite.rel.tvr.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.RelSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.tools.visualizer.InputExcludedRelWriter;

import java.util.List;
import java.util.Map;
import java.util.Set;


public class DotPrinter extends RelVisitor {
  List<String> edges = Lists.newArrayList();
  Set<RelNode> visitedSet = Sets.newHashSet();

  private String getId(RelNode node) {
    String id = node.getRelTypeName() + "_" + node.getId();
    return id;
  }

  private String getLabel(RelNode node) {
    StringBuilder label = new StringBuilder();
    label.append("\"");

    label.append(getId(node));

    if (node instanceof TableScan) {
      RelOptTableImpl table = (RelOptTableImpl) node.getTable();
      label.append("  ");
      label.append(table.getQualifiedName().toString());
      TimeInterval interval = table.interval;
      if (interval != null) {
        label.append("@").append(TvrUtils.formatTimeInstant(interval.from))
                .append("-").append(TvrUtils.formatTimeInstant(interval.to));
      }
    }

    label.append("\\n");

    TvrContext ctx = TvrContext.getInstance(node.getCluster());
    if (TvrUtils.progressiveEnabled(ctx) && node.getCluster().getPlanner() instanceof VolcanoPlanner) {
      VolcanoPlanner planner = (VolcanoPlanner) node.getCluster().getPlanner();
      Map<RelNode, RelSubset> mqcBest = ctx.mqcBestPlanMapping;
      if (mqcBest != null) {
        RelSubset subset = mqcBest.get(node);
        RelSet set = subset == null ? null : planner.getSet(subset);
        label.append("subset#");
        label.append((subset == null ? "null" : subset.getId()));
        label.append(", ");
        label.append("set#");
        label.append((set == null ? "null" : set.getId()));

        String costString = ctx.mqcMemoCostString.get(subset);
        if (costString != null) {
          label.append(" -- " + costString);
        }
      }
    }

    label.append("\\n");

    InputExcludedRelWriter relWriter = new InputExcludedRelWriter();
    node.explain(relWriter);
    label.append(splitIntoLines(relWriter.toString(), 80, 10));

    label.append("\"");

    return label.toString();
  }

  private String splitIntoLines(String str, int maxNumCharsPerLine, int minNumCharsPerLine) {
    StringBuilder sb = new StringBuilder();
    int strLen = str.length();
    for (int i = 0; i < strLen; i++) {
      if (i > 0 && (i % maxNumCharsPerLine == 0) && (strLen - i > minNumCharsPerLine)) {
        sb.append("\\n");
      }
      sb.append(str.charAt(i));
    }
    return sb.toString();
  }

  @Override public void visit(RelNode node, int ordinal, RelNode parent) {
    if (visitedSet.contains(node)) {
      return;
    }
    for (RelNode in : node.getInputs()) {
      String line = String
          .format("%s -> %s", getId(in), getId(node));
      edges.add(line);
    }
    visitedSet.add(node);
    super.visit(node, ordinal, parent);
  }

  public static String getDot(RelNode node) {
    DotPrinter printer = new DotPrinter();
    printer.go(node);
    StringBuilder sb = new StringBuilder();
    sb.append("digraph {");
    sb.append(System.lineSeparator());
    sb.append("node [fontsize=18];");
    sb.append(System.lineSeparator());
    for (RelNode rel : printer.visitedSet) {
      sb.append(printer.getId(rel));
      sb.append("[" + "label=").append(printer.getLabel(rel)).append("]");
      sb.append(";");
      sb.append(System.lineSeparator());
    }

    for (String l : printer.edges) {
      sb.append(l);
      sb.append(";");
      sb.append(System.lineSeparator());
    }
    sb.append("}");
    sb.append(System.lineSeparator());
    return sb.toString();
  }
}
