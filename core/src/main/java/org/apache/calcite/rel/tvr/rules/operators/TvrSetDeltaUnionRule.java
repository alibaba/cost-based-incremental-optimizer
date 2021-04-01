package org.apache.calcite.rel.tvr.rules.operators;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.utils.ProjectBuilder;
import org.apache.calcite.rel.type.RelDataType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.calcite.rel.tvr.utils.TvrUtils.notSetSnapshot;

public class TvrSetDeltaUnionRule extends RelOptTvrRule {

  /**
   * Matches:
   * <p>
   *   LogicalUnion --SET_SNAPSHOT_MAX-- Tvr0
   *          |
   *          |                                                  (*SetDelta)
   *    ManyInputs  --SET_SNAPSHOT_MAX-- Tvr(1, 2 ...)  ----------------------  ManyInputs
   * <p>
   * Converts to:
   * <p>
   *                                                     (SetDelta+/SetDelta+-)
   *     LogicalUnion --SET_SNAPSHOT_MAX-- Tvr0       --------------------------   newUnion
   *          |                                                                       |
   *          |                                            (SetDelta+/SetDelta+-)      |
   *      ManyInputs  --SET_SNAPSHOT_MAX-- Tvr(1, 2 ...)  ----------------------  ManyInputs
   *
   * <p>
   */
  private static RelOptRuleOperand operandOf(int numInputs) {
    Supplier<RelOptRuleOperand> leaf = () -> operand(RelSubset.class,
        tvrEdgeSSMax(
            tvr(tvrEdge(TvrSetDelta.class, notSetSnapshot, logicalSubset())),
            IDENTICAL_TIME), any());
    return operand(LogicalUnion.class, r -> r.getInputs().size() == numInputs,
        tvrEdgeSSMax(tvr()),
        some(Stream.generate(leaf).limit(numInputs).collect(toList())));
  }

  public TvrSetDeltaUnionRule(int numInputs) {
    super(operandOf(numInputs), "TvrSetDeltaUnionRule-" + numInputs);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RelMatch root = getRoot(call);

    // check whether all input nodes have the same input trait.
    // For example, set delta unites set delta, value delta unites value delta.
    return root.inputs().stream().map(i -> i.tvrSibling().tvrSemantics.fromVersion)
        .distinct().count() == 1 && root.inputs().stream().map(i -> i.tvrSibling().tvrSemantics.toVersion)
        .distinct().count() == 1;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    LogicalUnion union = root.get();

    boolean isPositiveOnly = root.inputs().stream()
        .allMatch(i -> ((TvrSetDelta) i.tvrSibling().tvrSemantics).isPositiveOnly());

    List<RelNode> newInputs = new ArrayList<>();
    for (RelMatch input : root.inputs()) {
      TvrSetDelta setDelta = (TvrSetDelta) input.tvrSibling().tvrSemantics;
      if (!isPositiveOnly && setDelta.isPositiveOnly()) {
        newInputs.add(ProjectBuilder.anchor(input.tvrSibling().rel.get())
            .addAll().addConstMultiplicity(BigDecimal.ONE).build());
      } else {
        newInputs.add(input.tvrSibling().rel.get());
      }
    }

    TvrSetDelta inputSetDelta = (TvrSetDelta) root.input(0).tvrSibling().tvrSemantics;
    TvrSetDelta newTrait = new TvrSetDelta(inputSetDelta.fromVersion,
        inputSetDelta.toVersion, isPositiveOnly);

    // different inputs might have different nullable properties
    // we need derive row type first
    RelNode newUnion = union.copy(union.getTraitSet(), newInputs);
    RelDataType rowType = newUnion.getRowType();
    newUnion = call.builder().push(newUnion).convert(rowType, true).build();

    transformToRootTvr(call, newUnion, newTrait);
  }

}


