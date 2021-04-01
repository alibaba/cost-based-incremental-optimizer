package org.apache.calcite.rel.tvr.rels;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AdhocSink;

public class LogicalAdhocSink extends AdhocSink {

    protected LogicalAdhocSink(RelTraitSet traits, RelNode input) {
        super(traits, input);
    }

    public static LogicalAdhocSink create(RelNode input) {
        return new LogicalAdhocSink(input.getTraitSet(), input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, RelNode input) {
        return new LogicalAdhocSink(traitSet, input);
    }
}
