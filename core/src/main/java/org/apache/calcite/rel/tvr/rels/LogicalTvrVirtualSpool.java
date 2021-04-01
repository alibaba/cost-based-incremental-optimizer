package org.apache.calcite.rel.tvr.rels;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

public class LogicalTvrVirtualSpool extends TvrVirtualSpool {

  protected LogicalTvrVirtualSpool(RelTraitSet traits, RelNode input) {
    super(traits, input);
  }

  public static LogicalTvrVirtualSpool create(RelNode input) {
    return new LogicalTvrVirtualSpool(input.getTraitSet(), input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, RelNode input) {
    return new LogicalTvrVirtualSpool(traitSet, input);
  }
}
