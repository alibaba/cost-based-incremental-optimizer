package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

public class VolcanoRuleMatchLogger implements RelOptListener {
  private final ArrayList<String> log = new ArrayList<>();
  @Override
  public void relEquivalenceFound(RelEquivalenceEvent event) {
  }

  @Override
  public void ruleAttempted(RuleAttemptedEvent event) {
  }

  @Override
  public void ruleProductionSucceeded(RuleProductionEvent event) {
    if (event.isBefore()) {
      return;
    }

    RelOptRuleCall ruleCall = event.getRuleCall();
    StringBuilder builder = new StringBuilder();
    builder.append(ruleCall.getRule().toString());
    builder.append(" [");
    for (RelNode rel : ruleCall.rels) {
      builder.append(rel.getId());
      builder.append(",");
    }
    builder.append("]");
    log.add(builder.toString());
  }

  @Override
  public void relDiscarded(RelDiscardedEvent event) {
  }

  @Override
  public void relChosen(RelChosenEvent event) {
  }

  public List<String> getLog() {
    return log;
  }
}
