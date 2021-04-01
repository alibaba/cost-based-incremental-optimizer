package org.apache.calcite.rel.tvr.rels;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.util.BuiltInMethod;

public class EnumerablePhysicalTvrVirtualSpool extends TvrVirtualSpool implements EnumerableRel  {

    /**
     * Creates an <code>VirtualRoot</code>.
     */
    protected EnumerablePhysicalTvrVirtualSpool(RelTraitSet traitSet,
                                                RelNode input) {
        super(traitSet, input);
    }

    @Override public RelNode copy(RelTraitSet traitSet, RelNode input) {
        return new EnumerablePhysicalTvrVirtualSpool(traitSet, input);
    }

    public static RelNode create(RelNode input) {
        if (input == null) {
            throw new UnsupportedOperationException(
                    "Input of virtual root is null or empty.");
        }
        return new EnumerablePhysicalTvrVirtualSpool(input.getTraitSet().replace(EnumerableConvention.INSTANCE), input);
    }

    public static final RelOptRule ENUMERABLE_PHYSICAL_TVR_VIRTUAL_SPOOL_RULE = new EnumerablePhysicalTvrVirtualSpoolRule();

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        // pass through and invoke input
        final BlockBuilder builder = new BlockBuilder();
        final Result result = implementor.visitChild(this, 0, (EnumerableRel) getInput(), pref);
        Expression childExp = builder.append("child" + 0, result.block);

        builder.add(childExp);
        final PhysType physType = PhysTypeImpl.of(
                implementor.getTypeFactory(),
                getRowType(),
                pref.prefer(JavaRowFormat.CUSTOM));
        return implementor.result(physType, builder.toBlock());
    }

    private static class EnumerablePhysicalTvrVirtualSpoolRule extends ConverterRule {
        private EnumerablePhysicalTvrVirtualSpoolRule() {
            super(LogicalTvrVirtualSpool.class, Convention.NONE, EnumerableConvention.INSTANCE,
                    "EnumerablePhysicalTvrVirtualSpoolRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            RelTraitSet outTraits = rel.getTraitSet().replace(EnumerableConvention.INSTANCE);
            return new EnumerablePhysicalTvrVirtualSpool(
                    rel.getTraitSet().replace(EnumerableConvention.INSTANCE),
                    rel.getCluster().getPlanner().changeTraits(rel.getInput(0), outTraits));
        }
    }

}
