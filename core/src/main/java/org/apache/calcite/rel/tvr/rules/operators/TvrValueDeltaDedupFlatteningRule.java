package org.apache.calcite.rel.tvr.rules.operators;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.tvr.rels.LogicalTvrDeduper;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Matches:
 * <p>
 * -                        LogicalTvrDeduper
 * -                               |
 * -                              Union
 * -                          /          \
 * -               LogicalTvrDeduper    Rel
 * -                        |
 * -                      Union
 * -                        |
 * -                      Inputs
 * <p>
 * Converts to:
 * <p>
 * -                        LogicalTvrDeduper
 * -                               |
 * -                              Union
 * -                          /          \
 * -                       Inputs       Rel
 */
public class TvrValueDeltaDedupFlatteningRule extends RelOptRule {

  public static final TvrValueDeltaDedupFlatteningRule INSTANCE = new TvrValueDeltaDedupFlatteningRule();

  private TvrValueDeltaDedupFlatteningRule() {
    super(operand(LogicalTvrDeduper.class,
        operand(LogicalUnion.class,
            operand(LogicalTvrDeduper.class,
                operand(LogicalUnion.class, any())),
            operand(RelSubset.class, any()))),
        "TvrValueDeltaDedupFlatteningRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalTvrDeduper topDeduper = call.rel(0);
    LogicalTvrDeduper leftDeduper = call.rel(2);

    // topDeduper != leftDeduper : the same node cannot be matched more than once because some deduper are idempotent.
    // e.g.,                                                    ----- deduper D (order by c1 limit 10)
    //                                                          |      |
    //         Rel A -> deduper B (order by c1 limit 10) -> Rel C -----|---------> ....
    // the deduper D can be matched any number of times without affecting the final result of execution.
    return topDeduper != leftDeduper && topDeduper.valueEquals(leftDeduper);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalTvrDeduper topDeduper = call.rel(0);
    LogicalUnion topUnion = call.rel(1);
    LogicalUnion leftUnion = call.rel(3);
    RelNode rightInput = call.rel(4);

    List<RelNode> newInputs = new ArrayList<>();
    newInputs.add(leftUnion);
    newInputs.add(rightInput);

    RelNode newUnion = topUnion.copy(topUnion.getTraitSet(), newInputs);
    call.transformTo(topDeduper.copy(topDeduper.getTraitSet(), ImmutableList.of(newUnion)));
  }
}
