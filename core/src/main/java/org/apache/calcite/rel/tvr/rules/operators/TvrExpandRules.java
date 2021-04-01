//package org.apache.calcite.rel.tvr.rules.operators;
//
//import com.aliyun.odps.lot.cbo.rel.odps.OdpsLogicalExpand;
//import com.aliyun.odps.lot.cbo.rel.odps.OdpsRelOptUtils;
//import com.aliyun.odps.lot.cbo.rules.OdpsRelOptTvrRule;
//import com.aliyun.odps.lot.cbo.trait.progressive.TvrSetDelta;
//import com.aliyun.odps.lot.cbo.trait.progressive.TvrValueDelta;
//import com.aliyun.odps.lot.cbo.trait.progressive.transformer.*;
//import com.aliyun.odps.lot.cbo.trait.progressive.transformer.predicate.TransformerPredicate;
//import com.aliyun.odps.lot.cbo.utils.TvrRowTypeUtils;
//import org.apache.calcite.plan.RelOptRuleCall;
//import org.apache.calcite.plan.RelOptTvrRule;
//import org.apache.calcite.plan.tvr.TvrSemantics;
//import org.apache.calcite.plan.volcano.RelSubset;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.rel.type.RelDataTypeFactory;
//import org.apache.calcite.rel.type.RelDataTypeField;
//import org.apache.calcite.rex.RexBuilder;
//import org.apache.calcite.rex.RexInputRef;
//import org.apache.calcite.rex.RexNode;
//import org.apache.calcite.util.ImmutableIntList;
//import org.apache.calcite.util.Pair;
//
//import java.math.BigDecimal;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.List;
//import java.util.Objects;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//
//import static com.aliyun.odps.lot.cbo.rules.tvr.operators.TvrProjectRules.applyProject;
//
///**
// * Expand is equivalent to union multi projects, for example
// * --columns: a,b,c
// * --Project: a,b,null, 1L
// *
// * --Expand
// *   |--  a,b,null, 1L
// *   |__  a,null,null, 2L
// *
// * The above expand is equivalent to Project(a,b,null, 1L) union Project(a,null,null, 2L)
// *
// * <p>
// * Matches:
// * <p>
// *     LogicalExpand  --SET_SNAPSHOT_MAX-- Tvr0
// *           |                                     (SetDelta/ValueDelta)
// *         input       --SET_SNAPSHOT_MAX-- Tvr1 ------------------------     input
// * <p>
// * Converts to:
// * <p>
// *                                                 (SetDelta/ValueDelta)
// *     LogicalExpand  --SET_SNAPSHOT_MAX-- Tvr0 ------------------------   newExpand
// *           |                                     (SetDelta/ValueDelta)         |
// *         input       --SET_SNAPSHOT_MAX-- Tvr1 ------------------------     input
// * <p>
// */
//public abstract class TvrExpandRules {
//    public static final TvrSetDeltaExpandRule TVR_SET_DELTA_EXPAND_RULE =
//            new TvrSetDeltaExpandRule();
//    public static final TvrValueSemanticsExpandRule
//            TVR_VALUE_SEMANTICS_EXPAND_RULE = new TvrValueSemanticsExpandRule();
//}
//
//abstract class TvrExpandRuleBase extends RelOptTvrRule {
//    TvrExpandRuleBase(Class<? extends TvrSemantics> tvrClass) {
//        super(operand(LogicalExpand.class, tvrEdgeSSMax(tvr()),
//                operand(RelSubset.class,
//                        tvrEdgeSSMax(tvr(tvrEdge(tvrClass, logicalSubset()))), any())));
//    }
//}
//
///**
// * Tvr set delta expand rule
// */
//class TvrSetDeltaExpandRule extends TvrExpandRuleBase {
//    TvrSetDeltaExpandRule() {
//        super(TvrSetDelta.class);
//    }
//
//    @Override
//    public void onMatch(RelOptRuleCall call) {
//        RelMatch expandAny = getRoot(call);
//        OdpsLogicalExpand logicalExpand = expandAny.get();
//        RelOfTvr inputSibling = expandAny.input(0).tvrSibling();
//        RelNode leafDeltaRel = inputSibling.rel.get();
//        TvrSetDelta inputTrait = (TvrSetDelta) inputSibling.tvrSemantics;
//        RelDataTypeFactory typeFactory =
//                logicalExpand.getCluster().getTypeFactory();
//        RexBuilder rexBuilder = logicalExpand.getCluster().getRexBuilder();
//
//        List<List<RexNode>> newProjects = new ArrayList<>();
//        for (List<RexNode> projects: logicalExpand.getProjects()) {
//            newProjects.add(new ArrayList<>(projects));
//        }
//
//        RelDataTypeFactory.Builder typeBuilder = new RelDataTypeFactory.Builder(typeFactory);
//        typeBuilder.addAll(logicalExpand.getRowType().getFieldList());
//        if (!inputTrait.isPositiveOnly()) {
//            int indexOfMultiplicity = inputTrait.getIndexOfMultiplicity(leafDeltaRel);
//            newProjects.stream().forEach(p -> p.add(rexBuilder.makeInputRef(leafDeltaRel, indexOfMultiplicity)));
//            typeBuilder.add(leafDeltaRel.getRowType().getFieldList().get(indexOfMultiplicity));
//        }
//
//        RelNode newLogicalExpand =
//                OdpsLogicalExpand.create(leafDeltaRel, typeBuilder.build(), newProjects, logicalExpand.getExpandIdIndex());
//
//        transformToRootTvr(call, newLogicalExpand, inputTrait);
//    }
//}
//
///**
// * Tvr value semantics expand rule
// */
//class TvrValueSemanticsExpandRule extends TvrExpandRuleBase {
//
//    /**
//     * Expand is equivalent to union multi project, so predicate should be same with project rule.
//     */
//    private static final TransformerPredicate inputRefPredicate =
//        new TransformerPredicate() {
//            @Override
//            public boolean test(TvrAggregateTransformer transformer, ImmutableIntList indices) {
//                return indices.size() == 1;
//            }
//
//            @Override
//            public boolean test(TvrProjectTransformer transformer, ImmutableIntList indices) {
//                return indices.size() == 1;
//            }
//
//            @Override
//            public boolean test(TvrSortTransformer transformer, ImmutableIntList indices) {
//                return indices.size() == 1;
//            }
//        };
//
//    private static final TransformerPredicate nonInputRefPredicate =
//        new TransformerPredicate() {
//            @Override
//            public boolean test(TvrAggregateTransformer transformer, ImmutableIntList indices) {
//                if (indices.size() != 1) {
//                    return false;
//                }
//                return indices.stream().noneMatch(transformer::referToPartialResult);
//            }
//
//            @Override
//            public boolean test(TvrProjectTransformer transformer, ImmutableIntList indices) {
//                return indices.size() == 1;
//            }
//
//            @Override
//            public boolean test(TvrSortTransformer transformer, ImmutableIntList indices) {
//                // can not do the computation on the row number column (i.e., ROW_NUMBER + 1)
//                // if the 'limit' is not equal to 1.
//                Integer rowNumberColIndex = transformer.getRowNumberColIndex();
//                // 'rowNumberColIndex == null' means that row number column is not kept in the current schema,
//                // and it has been removed by the previous projects.
//                return indices.size() == 1 && rowNumberColIndex == null || (
//                    indices.stream().noneMatch(i -> i.equals(rowNumberColIndex))
//                        || transformer.getLimit().equals(BigDecimal.ONE));
//            }
//        };
//
//    TvrValueSemanticsExpandRule() {
//        super(TvrValueDelta.class);
//    }
//
//    @Override
//    public boolean matches(RelOptRuleCall call) {
//        RelMatch expandAny = getRoot(call);
//        OdpsLogicalExpand logicalExpand = expandAny.get();
//        TvrValueDelta inputTrait =
//                (TvrValueDelta) expandAny.input(0).tvrSibling().tvrSemantics;
//        // Partial aggregate columns cannot participate in complex expressions.
//        TvrSemanticsTransformer transformer = inputTrait.getTransformer();
//
//        return logicalExpand.getProjects().stream().flatMap(Collection::stream).allMatch(project -> {
//            TransformerPredicate predicate = project instanceof RexInputRef ?
//                    inputRefPredicate :
//                    nonInputRefPredicate;
//            return OdpsRelOptUtils.findUniqueInputRef(project)
//                    .stream().allMatch(p -> transformer.isCompatible(predicate, ImmutableIntList.of(p.getIndex())));
//        });
//    }
//
//    @Override
//    public void onMatch(RelOptRuleCall call) {
//        RelMatch expandAny = getRoot(call);
//        OdpsLogicalExpand logicalExpand = expandAny.get();
//        RelOfTvr inputSibling = expandAny.input(0).tvrSibling();
//        RelNode leafSnapshotRel = inputSibling.rel.get();
//        TvrValueDelta inputTrait = (TvrValueDelta) inputSibling.tvrSemantics;
//        List<RelDataTypeField> dataTypeFields = leafSnapshotRel.getRowType().getFieldList();
//        List<String> fieldNames = logicalExpand.getRowType().getFieldNames();
//
//        List<List<RexNode>> projectList = logicalExpand.getProjects();
//        List<List<RexNode>> newProjectList = new ArrayList<>();
//
//        TvrValueDelta newSemantics = null;
//        List<String> newNames = new ArrayList<>();
//        List<RelDataType> newDataTypes = new ArrayList<>();
//
//        for (int i = 0; i < projectList.size(); i++) {
//            List<Pair<RexNode, String>> namedProject =
//                Pair.zip(projectList.get(i), fieldNames);
//
//            Pair<TvrValueDelta, List<Pair<RexNode, String>>> pair =
//                applyProject(dataTypeFields, inputTrait, namedProject,
//                    leafSnapshotRel.getCluster());
//            List<RexNode> newProjects = Pair.left(pair.right);
//            newProjectList.add(newProjects);
//
//            if (i == 0) {
//                // use the schema of the first project to build the new expand
//                newSemantics = pair.left;
//                newNames.addAll(Pair.right(pair.right));
//                newDataTypes.addAll(newProjects.stream().map(RexNode::getType).collect(
//                    Collectors.toList()));
//                continue;
//            }
//
//            if (!newSemantics.equals(pair.left)) {
//                // can not build a valid expand node
//                // because the number of RexNodes in these projects is different
//                // or the tvrs are inconsistent
//                return;
//            }
//
//            if (newProjects.size() != newDataTypes.size()) {
//                // can not build a valid expand node
//                // because the number of RexNodes in these projects is different
//                // or the tvrs are inconsistent
//                return;
//            }
//            for (int k = 0; k < newProjects.size(); k++) {
//                RelDataType type = newProjects.get(k).getType();
//                RelDataType other = newDataTypes.get(k);
//                if (!TvrRowTypeUtils.areRowTypesEqual(type, other, false, false)) {
//                    // can not build a valid expand node
//                    // because the types of RexNodes in these projects are different
//                    return;
//                }
//                if (!other.isNullable() && type.isNullable()) {
//                    // always use the least constraints to build the schema of this new expand
//                    newDataTypes.set(k, type);
//                }
//            }
//        }
//
//        final int newExpandIndex = Objects
//            .requireNonNull(newSemantics.getTransformer().backwardMapping())
//            .get(logicalExpand.getExpandIdIndex()).get(0);
//
//        RelDataTypeFactory typeFactory = logicalExpand.getCluster().getTypeFactory();
//        RelDataTypeFactory.Builder typeBuilder =
//            new RelDataTypeFactory.Builder(typeFactory);
//        assert newNames.size() == newDataTypes.size();
//        IntStream.range(0, newNames.size()).forEach(
//            i -> typeBuilder.add(newNames.get(i), newDataTypes.get(i)));
//
//        RelNode newLogicalExpand = OdpsLogicalExpand
//            .create(leafSnapshotRel, typeBuilder.build(), newProjectList,
//                newExpandIndex);
//        transformToRootTvr(call, newLogicalExpand, newSemantics);
//    }
//}
