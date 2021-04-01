package org.apache.calcite.plan.volcano;

import java.util.EnumSet;

class ReuseImpl {
  public final EnumSet<MatType> matTypes;
  public final TRelOptCost reuseCost;

  ReuseImpl(EnumSet<MatType> matTypes, TRelOptCost reuseCost) {
    this.matTypes = matTypes;
    this.reuseCost = reuseCost;
  }

  int time() {
    return reuseCost.earliest;
  }
}
