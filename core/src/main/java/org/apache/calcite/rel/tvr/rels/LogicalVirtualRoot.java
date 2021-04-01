package org.apache.calcite.rel.tvr.rels;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import java.util.List;

public class LogicalVirtualRoot extends VirtualRoot {

    protected LogicalVirtualRoot(RelOptCluster cluster, RelTraitSet traitSet,
                                 List<RelNode> inputs) {
        super(cluster, traitSet, inputs);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalVirtualRoot(getCluster(), traitSet, inputs);
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
        return new LogicalVirtualRoot(cluster, cluster.traitSet(), inputs);
    }

}