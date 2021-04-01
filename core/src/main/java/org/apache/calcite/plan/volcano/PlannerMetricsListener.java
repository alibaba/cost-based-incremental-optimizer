package org.apache.calcite.plan.volcano;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.VolcanoPlanner.DirectProvenance;
import org.apache.calcite.plan.volcano.VolcanoPlanner.RuleProvenance;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.utils.TvrContext;

import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.*;

/**
 * Created by ted on 2/3/2016.
 */
public class PlannerMetricsListener implements RelOptListener {

  static class AttemptMetrics {
    private int numAttempts = 0;
    private long timeMicros = 0;
    private boolean isCritical = false;

    public void incr(long t) {
      numAttempts++;
      timeMicros+= t;
    }

    public void setCritical(boolean isCritical) {
      this.isCritical = isCritical;
    }

    public int getNumAttempts() {
      return numAttempts;
    }

    public long getTimeMicros() {
      return timeMicros;
    }

    public AttemptMetrics copy() {
      AttemptMetrics ret = new AttemptMetrics();
      ret.numAttempts = this.numAttempts;
      ret.timeMicros = this.timeMicros;
      ret.isCritical = this.isCritical;
      return ret;
    }

    @Override
    public String toString() {
      return "(" + numAttempts + "," + timeMicros + "," + isCritical + ")";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof AttemptMetrics))
        return false;
      AttemptMetrics that = (AttemptMetrics) o;
      return numAttempts == that.numAttempts && timeMicros == that.timeMicros
          && isCritical == that.isCritical;
    }

    @Override
    public int hashCode() {
      return Objects.hash(numAttempts, timeMicros, isCritical);
    }
  }

  private final TvrContext ctx;
  private final Map<String, AttemptMetrics> attempts = Maps.newHashMap();
  private Map<String, AttemptMetrics> lastAttempts = Maps.newHashMap();
  private final Set<String> visitedPath = Sets.newHashSet();

  private long start;
  private int numAttempts = 0;

  public PlannerMetricsListener(TvrContext ctx) {
    this.ctx = ctx;
  }

  /**
   * Resets all metrics.
   */
  public void reset() {
    numAttempts = 0;
    attempts.clear();
    visitedPath.clear();
  }

  @Override public void relEquivalenceFound(
      RelEquivalenceEvent event) {

  }

  private void validateRuleAttemptedCounter() {
    numAttempts++;
//    if (numAttempts > ctx.getConfig().getMaxRuleAttemptCount()) {
//      throw new RetryableException(
//          "Rule apply exceed limit:" + ctx.getConfig().getMaxRuleAttemptCount());
//    }
  }

  @Override public void ruleAttempted(RuleAttemptedEvent event) {
    if (event.isBefore()) {
      // Record the start of the rule attempt
      start = System.currentTimeMillis();
    } else {
      // validate the attempt counter
      validateRuleAttemptedCounter();

      long l = System.currentTimeMillis() - start;
      RelOptRule rule = event.getRuleCall().getRule();
      AttemptMetrics m = attempts
          .computeIfAbsent(rule.toString(), k -> new AttemptMetrics());
      m.incr(l);
    }
  }

  @Override public void ruleProductionSucceeded(
      RuleProductionEvent event) {
  }

  @Override public void relDiscarded(RelDiscardedEvent event) {

  }

  @Override public void relChosen(RelChosenEvent event) {
    String sig = event.getRel() == null ? null : event.getRel().toString();
    if (event.getSource() instanceof VolcanoPlanner
        && !visitedPath.contains(sig)) {
      markRecurse((VolcanoPlanner) event.getSource(), event.getRel());
    }
  }

  /**
   * Mark provenance set from chosen RelNode.
   *
   * @param planner Volcano planner
   * @param r RelNode
   *
   */
  private void markRecurse(VolcanoPlanner planner, RelNode r) {
    Object p = planner.provenanceMap.get(r);
    if (r != null) {
      visitedPath.add(r.toString());
    }
    if (p instanceof DirectProvenance) {
      RelNode rel = ((DirectProvenance) p).source;
      markRecurse(planner, rel);
    } else if (p instanceof RuleProvenance) {
      RelOptRule rule = ((RuleProvenance) p).rule;
      AttemptMetrics m = attempts
          .computeIfAbsent(rule.toString(), k -> new AttemptMetrics());
      m.setCritical(true);
      for (RelNode rel : ((RuleProvenance) p).rels) {
        markRecurse(planner, rel);
      }
    } else if (p == null && r instanceof RelSubset) {
      final RelSubset subset = (RelSubset) r;
      markRecurse(planner, subset.getRelList().get(0));
    }
  }

  public String report() {
//    MetricsMonitor monitor = ctx.getMonitor();
//    if (monitor != null) {
//      monitor.reset(PlanMetrics.PLAN_RULE_MATCH_COUNT.name(), numAttempts);
//    }
//    ThreadCounter.replace(ThreadCounter.RULE_APPLY, numAttempts);

    StringBuilder sb = new StringBuilder();
    sb.append("Total rule attempts: " + numAttempts);
    sb.append(System.lineSeparator());
    sb.append(String
        .format("%-40s%20s%20s%n", "Rule attempts (*: critical):", "Attempts",
            "Time(us)"));
    attempts.forEach((rule, m) -> {
      sb.append(String
          .format(
              "%-40s%20d%20s%n",
              (m.isCritical ? "* " : "") + rule,
              m.getNumAttempts(),
              NumberFormat.getNumberInstance(Locale.US).format(m.getTimeMicros())));
//      ThreadCounter.add(ThreadCounter.RULE_APPLY_TOTAL, rule, m.getNumAttempts(), m.getTimeMicros());
//      if (monitor != null) {
//        monitor.reset(String.format("PLAN_RULE_COUNT_%s", rule), m.getNumAttempts());
//        monitor.reset(String.format("PLAN_RULE_LATENCY_%s", rule), m.getTimeMicros());
//        monitor.reset(String.format("PLAN_RULE_CRITICAL_%s", rule), m.isCritical ? 1 : 0);
//      }
    });

    if (! lastAttempts.isEmpty()) {
      sb.append(System.lineSeparator())
          .append("delta:")
          .append(System.lineSeparator());
      sb.append(String
          .format("%-40s%20s%20s%n", "Rule attempts (*: critical):", "Attempts",
              "Time(us)"));
      sb.append(System.lineSeparator());
      attempts.forEach((rule, m) -> {
        if (lastAttempts.containsKey(rule) && lastAttempts.get(rule).equals(m)) {
          return;
        }
        int numLastAttempts = lastAttempts.containsKey(rule) ?
            lastAttempts.get(rule).numAttempts :
            0;
        long timeLastAttempts = lastAttempts.containsKey(rule) ?
            lastAttempts.get(rule).timeMicros :
            0L;
        sb.append(String
            .format("%-40s%20d%20s%n", (m.isCritical ? "* " : "") + rule,
                m.getNumAttempts() - numLastAttempts,
                NumberFormat.getNumberInstance(Locale.US)
                    .format(m.getTimeMicros() - timeLastAttempts)));
      });
      sb.append(System.lineSeparator());
    }
    lastAttempts = new HashMap<>();
    attempts.forEach((rule, m) -> lastAttempts.put(rule, m.copy()));

    return sb.toString();
  }

  public int numAttempts() {
    return numAttempts;
  }

  public boolean isCritical(String rule) {
    AttemptMetrics m = attempts.get(rule);
    return m != null && m.isCritical;
  }
}
