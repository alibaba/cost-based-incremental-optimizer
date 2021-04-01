package org.apache.calcite.rel.tvr.rels;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.util.BuiltInMethod;

import java.util.List;

public class TvrExecPlansOp extends AbstractRelNode implements EnumerableRel {

  private final ImmutableList<RelNode> inputs;

  private final int execTime;
  /**
   * Creates a <code>SingleRel</code>.
   *
   * @param cluster Cluster this relational expression belongs to
   */
  private TvrExecPlansOp(RelOptCluster cluster, RelTraitSet traitSet,
                         List<RelNode> inputs, int execTime) {
    super(cluster, traitSet);
    this.inputs = ImmutableList.copyOf(inputs);
    this.execTime = execTime;
  }

  @Override
  public List<RelNode> getInputs() {
    return inputs;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    for (Ord<RelNode> ord : Ord.zip(inputs)) {
      pw.input("input#" + ord.i, ord.e);
    }
    return pw;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new TvrExecPlansOp(getCluster(), traitSet, inputs, execTime);
  }

  public static TvrExecPlansOp create(RelOptCluster cluster,
      List<RelNode> inputs, int execTime) {
    return new TvrExecPlansOp(cluster, cluster.traitSet(), inputs, execTime);
  }

  public int getExecTime() {
    return execTime;
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    Expression unionExp = null;
    for (Ord<RelNode> ord : Ord.zip(inputs)) {
      EnumerableRel input = (EnumerableRel) ord.e;
      final Result result = implementor.visitChild(this, ord.i, input, pref);
      Expression childExp = builder.append("child" + ord.i, result.block);
      if (unionExp == null) {
        unionExp = childExp;
      } else {
        unionExp = Expressions.call(unionExp, BuiltInMethod.CONCAT.method, childExp);
      }
    }

    builder.add(unionExp);
    final PhysType physType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.prefer(JavaRowFormat.CUSTOM));
    return implementor.result(physType, builder.toBlock());
  }
}
