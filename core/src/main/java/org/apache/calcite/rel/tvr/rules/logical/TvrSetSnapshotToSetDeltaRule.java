package org.apache.calcite.rel.tvr.rules.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrSetSnapshot;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.TvrConverterRule;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrMetaSet.TvrConvertMatch;
import org.apache.calcite.plan.volcano.TvrMetaSetType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.trait.TvrValueDelta;
import org.apache.calcite.rel.tvr.utils.ProjectBuilder;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.VersionInterval;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This rule is to calculate the differences between the previous set snapshot
 * and current set snapshot to output set delta (set +/-).
 */
public class TvrSetSnapshotToSetDeltaRule extends TvrConverterRule {

  public static TvrSetSnapshotToSetDeltaRule INSTANCE =
      new TvrSetSnapshotToSetDeltaRule();

  private TvrSetSnapshotToSetDeltaRule() {
    super(ImmutableList.of(TvrSetSnapshot.class));
  }

  private RelNode diffUsingAggregation(RelNode leftNode, RelNode rightNode,
                                       RexBuilder rexBuilder, TvrSetDelta setDelta, boolean consolidate) {

    // 1. projection
    // with last snapshot added with "-1" and current snapshot added as "+1"
    LogicalProject prevProject = ProjectBuilder.anchor(leftNode).addAll()
        .addConstMultiplicity(BigDecimal.ONE.negate()).build();
    LogicalProject curProject = ProjectBuilder.anchor(rightNode).addAll()
        .addConstMultiplicity(BigDecimal.ONE).build();

    // 2. union
    RelNode union = LogicalUnion
        .create(Lists.newArrayList(prevProject, curProject), true);

    if (!consolidate) {
      return union;
    }

    // 3. Consolidate the setDelta
    return TvrSetDelta.consolidate(union, rexBuilder, setDelta, false);
  }

  /**
   * when input has an (explicit or implicit) primary key:
   * For deletion, each deletion only corresponds to one single previous tuple,
   * therefore, we can use a LEFT ANTI JOIN to filter out the tuples to be deleted.
   * For insertion, we can use a RIGHT ANTI JOIN to filter out the tuples to be added.
   * Then use a project node to add the multiplicity column.
   *
   * @param leftInput   is the input with previous snapshot
   * @param rightInput  is the input with current snapshot
   */
  private void diffUsingAntiJoin(
      RelOptRuleCall call, RelNode leftInput,
      RelNode rightInput, RexBuilder rexBuilder) {
    int columnNum = rightInput.getRowType().getFieldCount();
    ImmutableIntList joinKeys =
        ImmutableIntList.copyOf(ImmutableIntList.range(0, columnNum));

    // 1. left anti join and right anti join
    List<RelDataTypeField> leftFields = leftInput.getRowType().getFieldList();
    List<RelDataTypeField> rightFields = rightInput.getRowType().getFieldList();
    // 't1 LEFT ANTI JOIN t2 ON t1.a = t2.a AND t1.b = t2.b AND ...'
    List<RexNode> rexList = IntStream.range(0, columnNum).mapToObj(i -> {
      RelDataTypeField rightField = rightFields.get(i);
      RelDataTypeField leftField = leftFields.get(i);
      return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ImmutableList
          .of(new RexInputRef(rightField.getIndex(), rightField.getType()),
              new RexInputRef(leftField.getIndex(), leftField.getType())));
    }).collect(Collectors.toList());
    RexNode joinCondition;
    if (rexList.size() > 1) {
      // more than one condition, use "AND" to combine.
      joinCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, rexList);
    } else {
      joinCondition = rexList.get(0);
    }

    RelNode leftAntiJoin = SemiJoin
        .create(leftInput, rightInput, joinCondition, joinKeys, joinKeys,
            SemiJoinType.ANTI); // SemiJoinType.ANTILEFT

    RelNode rightAntiJoin = SemiJoin
        .create(rightInput, leftInput, joinCondition, joinKeys, joinKeys,
                SemiJoinType.ANTI); // SemiJoinType.ANTILEFT

    // 2. project
    RelNode posProject = ProjectBuilder.anchor(rightAntiJoin).addAll()
        .addConstMultiplicity(BigDecimal.ONE).build();
    RelNode negProject = ProjectBuilder.anchor(leftAntiJoin).addAll()
        .addConstMultiplicity(BigDecimal.ONE.negate()).build();

    RelNode union =
        LogicalUnion.create(Arrays.asList(posProject, negProject), true);
    call.transformTo(union);
  }

  @Override
  public List<TvrConvertMatch> match(TvrMetaSet tvr, TvrSemantics newTrait,
      RelOptCluster cluster) {
    TvrMetaSetType tvrType = tvr.getTvrType();
    ImmutableList.Builder<TvrConvertMatch> builder = ImmutableList.builder();
    TvrVersion[] snapshots = tvrType.getSnapshots();
    int index = Arrays.binarySearch(snapshots, newTrait.toVersion);
    if (index < 0) {
      return Collections.emptyList();
    }

    TvrSemantics prev = null;
    if (index > 0) {
      TvrVersion from = snapshots[index - 1];
      prev = tvr.allTvrSemantics().stream()
          .filter(x -> x instanceof TvrSetSnapshot && x.toVersion.equals(from))
          .findAny().orElse(null);
    }
    if (prev != null && tvrType
        .contains(VersionInterval.of(prev.toVersion, newTrait.toVersion))) {
      builder.add(create(prev, newTrait, cluster, tvr));
    }

    TvrSemantics next = null;
    if (index < snapshots.length - 1) {
      TvrVersion to = snapshots[index + 1];
      next = tvr.allTvrSemantics().stream()
          .filter(x -> x instanceof TvrSetSnapshot && x.toVersion.equals(to))
          .findAny().orElse(null);
    }
    if (next != null && tvrType
        .contains(VersionInterval.of(newTrait.toVersion, next.toVersion))) {
      builder.add(create(newTrait, next, cluster, tvr));
    }

    return builder.build();
  }

  private TvrConvertMatch create(TvrSemantics leftTrait,
      TvrSemantics rightTrait, RelOptCluster cluster, TvrMetaSet tvr) {
    TvrSetDelta setDelta =
        new TvrSetDelta(leftTrait.toVersion, rightTrait.toVersion, false);

    RelNode leftInput = getInputSubset(cluster, tvr, leftTrait);
    RelNode rightInput = getInputSubset(cluster, tvr, rightTrait);

    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelNode rel;
    if (TvrUtils.progressiveSuperDeltaEnabled(cluster)) {
      rel = diffUsingAggregation(leftInput, rightInput, rexBuilder, setDelta,
          false);
    } else {
      rel = diffUsingAggregation(leftInput, rightInput, rexBuilder, setDelta,
          true);

      //    // there is an alternative way to merge using AntiJoin when key exists
      //    // we temporarily disable it to make rule easier to manage
      //    // TODO[TVR]: decide whether aggregate or antiJoin is better when key exists
      //    if (TvrUtils.inputExistsKey(leftNode)) {
      //      diffUsingAntiJoin(call, leftNode, rightNode, rexBuilder);
      //    }
    }

    return new TvrConvertMatch(setDelta, ImmutableList.of(rel), true,
        (tvrMetaSet) -> tvrMetaSet.allTvrSemantics().stream().anyMatch(x ->
            (x instanceof TvrSetDelta
                || x instanceof TvrValueDelta && !((TvrValueDelta) x)
                .hasWindow()) && x.timeRangeEquals(setDelta)) || tvrMetaSet
            .allTvrSemantics().stream()
            .noneMatch(x -> x instanceof TvrValueDelta));
  }
}
