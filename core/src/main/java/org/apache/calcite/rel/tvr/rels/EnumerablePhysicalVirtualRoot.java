package org.apache.calcite.rel.tvr.rels;

import com.google.common.collect.Lists;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.util.BuiltInMethod;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class EnumerablePhysicalVirtualRoot extends VirtualRoot implements EnumerableRel {

    /**
     * Creates an <code>VirtualRoot</code>.
     */
    protected EnumerablePhysicalVirtualRoot(RelOptCluster cluster, RelTraitSet traitSet,
                                            List<RelNode> inputs) {
        super(cluster, traitSet, inputs);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new EnumerablePhysicalVirtualRoot(inputs.get(0).getCluster(), traitSet, inputs);
    }

    /**
     * Create a virtual root based on given inputs. If there is only 1 input,
     * return the input itself.
     *
     * @param inputs inputs
     * @return virtual root, or input if given single input.
     */
    public static RelNode create(List<RelNode> inputs) {
        if (inputs == null || inputs.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Input of virtual root is null or empty.");
        }
        if (inputs.size() == 1) {
            return inputs.get(0);
        }
        RelNode root = inputs.get(0);
        RelOptCluster cluster = root.getCluster();
        return new EnumerablePhysicalVirtualRoot(cluster, cluster.traitSet().replace(EnumerableConvention.INSTANCE), inputs);
    }

    public static final RelOptRule ENUMERABLE_PHYSICAL_VIRTUALROOT_RULE = new EnumerablePhysicalVirtualRootRule();

    // copy pasted from EnumerableUnion
    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        Expression unionExp = null;
        for (Ord<RelNode> ord : Ord.zip(inputs)) {
            EnumerableRel input = (EnumerableRel) ord.e;
            final Result result = implementor.visitChild(this, ord.i, input, pref);
            Expression childExp = builder.append("child" + ord.i, result.block);

            String msg = "--- generating result of time point " + ord.i + " ---";
            childExp = Expressions.call(BuiltInMethod.TVR_PRINT_MESSAGE.method, childExp, Expressions.constant(msg));

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

    private static class EnumerablePhysicalVirtualRootRule extends ConverterRule {
        private EnumerablePhysicalVirtualRootRule() {
            super(LogicalVirtualRoot.class, Convention.NONE, EnumerableConvention.INSTANCE,
                    "EnumerablePhysicalVirtualRootRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            RelTraitSet outTraits = rel.getTraitSet().replace(EnumerableConvention.INSTANCE);
            return new EnumerablePhysicalVirtualRoot(
                    rel.getCluster(),
                    rel.getTraitSet().replace(EnumerableConvention.INSTANCE),
                    rel.getInputs().stream().map(input -> rel.getCluster().getPlanner().changeTraits(input, outTraits))
                            .collect(Collectors.toList()));
        }
    }

}
