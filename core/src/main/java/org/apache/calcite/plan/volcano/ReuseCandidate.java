package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.AbstractRelNode;

import java.util.Comparator;
import java.util.Set;

public class ReuseCandidate {
  public static final Comparator<ReuseCandidate> PLAN_COST_COMPARATOR = (o1, o2) -> {
    RelOptCost c1 = o1.benefit;
    RelOptCost c2 = o2.benefit;
    if (c1.isLt(c2)) {
      return 1;
    }

    if (c2.isLt(c1)) {
      return -1;
    }

    int sum1 = o1.getCandidates().stream().mapToInt(AbstractRelNode::getId).min().orElse(0);
    int sum2 = o2.getCandidates().stream().mapToInt(AbstractRelNode::getId).min().orElse(0);
    return sum1 - sum2;
  };

  // these subsets need to be materialized at the same time
  private Set<RelSubset> candidates;
  // the possible benefits of persisting these subsets
  private RelOptCost benefit;

  public ReuseCandidate(Set<RelSubset> candidates, RelOptCost benefit) {
    this.candidates = candidates;
    this.benefit = benefit;
  }

  public Set<RelSubset> getCandidates() {
    return candidates;
  }

  public RelOptCost getBenefit() {
    return benefit;
  }
}
