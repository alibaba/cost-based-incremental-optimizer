package org.apache.calcite.rel.core;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public abstract class AdhocSink extends SingleRel {
    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param traits
     * @param input   Input relational expression
     */
    protected AdhocSink(RelTraitSet traits, RelNode input) {
        super(input.getCluster(), traits, input);
    }

    @Override
    protected RelDataType deriveRowType() {
        return input.getRowType();
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return mq.getRowCount(input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 1;
        return copy(traitSet, inputs.get(0));
    }

    public abstract RelNode copy(RelTraitSet traitSet, RelNode input);

}
