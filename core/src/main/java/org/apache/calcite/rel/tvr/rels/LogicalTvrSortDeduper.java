package org.apache.calcite.rel.tvr.rels;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

public class LogicalTvrSortDeduper extends LogicalTvrDeduper {

  private final RelCollation collation;
  private final RelDistribution distribution;

  private final BigDecimal limit;
  private final BigDecimal offset;

  private LogicalTvrSortDeduper(RelOptCluster cluster, RelTraitSet traits,
      RelNode input, RelCollation collation, RelDistribution distribution,
      BigDecimal limit, BigDecimal offset) {
    super(cluster, traits, input);
    this.collation = collation;
    this.distribution = distribution;
    this.limit = limit;
    this.offset = offset;
  }

  public static LogicalTvrSortDeduper create(RelNode input,
      RelCollation collation, RelDistribution distribution, BigDecimal limit,
      BigDecimal offset) {
    return new LogicalTvrSortDeduper(input.getCluster(), input.getTraitSet(),
        input, collation, distribution, limit, offset);
  }

  @Override
  protected RelDataType deriveRowType() {
    // sort deduper will not modify the row type of the input
    return input.getRowType();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    if (inputs.size() != 1) {
      throw new IllegalArgumentException(
          "LogicalTvrSortDeduper should have only one input, but receive "
              + inputs.size());
    }
    RelNode input = inputs.get(0);
    return new LogicalTvrSortDeduper(input.getCluster(), input.getTraitSet(),
        input, collation, distribution, limit, offset);
  }


  @Override
  public RelNode apply() {
    RelOptCluster cluster = getCluster();
    final RexLiteral limit =
        cluster.getRexBuilder().makeBigintLiteral(this.limit);
    final RexLiteral offset = this.offset == null ?
        null :
        cluster.getRexBuilder().makeBigintLiteral(this.offset);
    return LogicalSort
        .create(input, this.collation, offset, limit);
  }

  @Override
  public boolean valueEquals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LogicalTvrSortDeduper that = (LogicalTvrSortDeduper) o;
    return Objects.equals(collation, that.collation) && Objects
        .equals(distribution, that.distribution) && Objects
        .equals(limit, that.limit) && Objects.equals(offset, that.offset);
  }
}
