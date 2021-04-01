package org.apache.calcite.rel.tvr.rels;

import com.google.common.collect.Lists;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public class VirtualRoot extends AbstractRelNode {
    protected final List<RelNode> inputs;

    /**
     * Creates an <code>VirtualRoot</code>.
     */
    protected VirtualRoot(RelOptCluster cluster, RelTraitSet traitSet,
                          List<RelNode> inputs) {
        super(cluster, traitSet);
        this.inputs = Lists.newArrayList(inputs);
    }

    @Override public List<RelNode> getInputs() {
        return inputs;
    }

    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        for (Ord<RelNode> ord : Ord.zip(inputs)) {
            pw.input("input#" + ord.i, ord.e);
        }
        return pw;
    }

    @Override public void replaceInput(int ordinalInParent, RelNode p) {
        inputs.set(ordinalInParent, p);
    }

    @Override protected RelDataType deriveRowType() {
        return inputs.get(0).getRowType();
    }
}

