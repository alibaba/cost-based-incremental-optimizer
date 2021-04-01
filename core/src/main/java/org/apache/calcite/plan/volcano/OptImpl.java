package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelOptNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.stream.Collectors;

class OptImpl {
  public final TRelOptCost bestCost;
  public final RelNode best;
  public final ImmutableBitSet matOption;

  private OptImpl(TRelOptCost bestCost, RelNode best, ImmutableBitSet matOption) {
    this.bestCost = bestCost;
    this.best = best;
    this.matOption = matOption;
  }

  public static OptImpl of(TRelOptCost bestCost, RelNode best, ImmutableBitSet matOption) {
    return new OptImpl(bestCost, best, matOption);
  }

  public boolean inputMaterialized(int i) {
    return matOption.get(i);
  }

  public int time() {
    return bestCost.earliest;
  }

  @Override
  public String toString() {
    return "<" + best.getId() + best.getInputs().stream().map(RelOptNode::getId)
        .collect(Collectors.toList()) + " : " + matOption + " : " + bestCost
        + ">";
  }
}
