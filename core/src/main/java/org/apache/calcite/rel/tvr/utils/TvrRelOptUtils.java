package org.apache.calcite.rel.tvr.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.AbstractList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.rel.rules.ProjectRemoveRule.isTrivial;

public class TvrRelOptUtils {

    /**
     * Find all InputRefs in the RexNode, and put them in the specified container.
     * To collect unique inputRef, then use a {@link Set}.
     * To collect all inputRef, then use a {@link List}
     */
    private static class InputRefFinder extends RexVisitorImpl<Void> {
        private final Collection<RexInputRef> inputRefContainer;

        public InputRefFinder(Collection<RexInputRef> inputRefContainer) {
            super(true);
            if (inputRefContainer == null) {
                throw new RuntimeException("Please instantiate the container.");
            }
            this.inputRefContainer = inputRefContainer;
        }

        public Void visitInputRef(RexInputRef inputRef) {
            inputRefContainer.add(inputRef);
            return null;
        }
    }

    public static Set<RexInputRef> findUniqueInputRef(RexNode node) {
        Set<RexInputRef> uniqueInputRefs = Sets.newHashSet();
        node.accept(new InputRefFinder(uniqueInputRefs));
        return uniqueInputRefs;
    }

    /**
     * Derives the type of a join relational expression.
     *
     * @param leftType        Row type of left input to join
     * @param rightType       Row type of right input to join
     * @param joinType        Type of join
     * @param typeFactory     Type factory
     * @param systemFieldList List of system fields that will be prefixed to
     *                        output row type; typically empty but must not be
     *                        null
     * @return join type
     */
    public static RelDataType deriveJoinRowType(
            RelDataType leftType,
            RelDataType rightType,
            JoinRelType joinType,
            RelDataTypeFactory typeFactory,
            List<RelDataTypeField> systemFieldList) {
        assert systemFieldList != null;
        switch (joinType) {
            case LEFT:
                rightType = createFieldRowType((RelRecordType) rightType, typeFactory, true);
                break;
            case RIGHT:
                leftType = createFieldRowType((RelRecordType) leftType, typeFactory, true);
                break;
            case FULL:
                leftType = createFieldRowType((RelRecordType) leftType, typeFactory, true);
                rightType = createFieldRowType((RelRecordType) rightType, typeFactory, true);
                break;
            default:
                break;
        }
        return SqlValidatorUtil
                .createJoinType(typeFactory, leftType, rightType, null,
                        systemFieldList);
    }

    private static RelDataType createFieldRowType(final RelRecordType type,
                                                  final RelDataTypeFactory factory, boolean nullable) {
        return factory.createStructType(type.getStructKind(), new AbstractList<RelDataType>() {
            @Override public RelDataType get(int index) {
                RelDataType fieldType = type.getFieldList().get(index).getType();
                return factory.createTypeWithNullability(fieldType, nullable);
            }

            @Override public int size() {
                return type.getFieldCount();
            }
        }, type.getFieldNames());
    }

    public static RelNode strip(RelNode input) {
        if (input instanceof Project) {
            Project project = (Project) input;
            return isTrivial(project) ? project.getInput() : project;
        } else {
            return input;
        }
    }

    public static RexNode makePredicate(RexBuilder rexBuilder, List<? extends RexNode> exprs) {
        int size = exprs.size();
        if (size == 0) {
            return null;
        } else if (size == 1) {
            return exprs.get(0);
        } else {
            return rexBuilder.makeCall(SqlStdOperatorTable.AND, exprs);
        }
    }

    public static class EqualExprs {
        private List<RexNode> ops;
        private boolean comparingNulls;

        public EqualExprs(RexNode leftReference, RexNode rightReference, boolean comparingNulls) {
            ops = ImmutableList.of(leftReference, rightReference);
            this.comparingNulls = comparingNulls;
        }

        public static EqualExprs of(RexNode leftReference, RexNode rightReference, boolean comparingNulls) {
            return new EqualExprs(leftReference, rightReference, comparingNulls);
        }

        public RexNode getLeftReference() {
            return ops.get(0);
        }

        public RexNode getRightReference() {
            return ops.get(1);
        }

        public boolean isComparingNulls() {
            return comparingNulls;
        }

        public String toString() {
            return "(" + getLeftReference().toString() + (comparingNulls ? "<=>" : ",") + getRightReference().toString() + ")";
        }

        public RexNode composeEqualExpr(RexBuilder rexBuilder) {
            return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ops);
        }
    }

    /**
     * Resolve the join condition according the inputs' row type.
     */
    public static class JoinConditionSplitRes {
        List<EqualExprs> equalExprs;
        List<RexNode> leftFilters;
        List<RexNode> rightFilters;
        List<RexNode> nonEqualExprs;

        public JoinConditionSplitRes(List<EqualExprs> equalExprs,
                                     List<RexNode> leftFilters, List<RexNode> rightFilters,
                                     List<RexNode> nonEqualExprs) {
            this.equalExprs = equalExprs;
            this.leftFilters = leftFilters;
            this.rightFilters = rightFilters;
            this.nonEqualExprs = nonEqualExprs;
        }

        public static JoinConditionSplitRes of(List<EqualExprs> equalExprs,
                                               List<RexNode> leftFilters, List<RexNode> rightFilters,
                                               List<RexNode> nonEqualExprs) {
            return new JoinConditionSplitRes(equalExprs, leftFilters, rightFilters,
                    nonEqualExprs);
        }

        public List<EqualExprs> getEqualExprs() {
            return equalExprs;
        }

        public List<RexNode> getLeftFilter() {
            return leftFilters;
        }

        public List<RexNode> getRightFilter() {
            return rightFilters;
        }

        public List<RexNode> getNonEqualExprs() {
            return nonEqualExprs;
        }

        public static JoinConditionSplitRes merge(
                List<JoinConditionSplitRes> resolvedConditions) {
            List<EqualExprs> equalExprs = Lists.newArrayList();
            List<RexNode> leftFilters = Lists.newArrayList();
            List<RexNode> rightFilters = Lists.newArrayList();
            List<RexNode> nonEqualExprs = Lists.newArrayList();
            for (JoinConditionSplitRes resolvedCondition : resolvedConditions) {
                equalExprs.addAll(resolvedCondition.getEqualExprs());
                leftFilters.addAll(resolvedCondition.getLeftFilter());
                rightFilters.addAll(resolvedCondition.getRightFilter());
                nonEqualExprs.addAll(resolvedCondition.getNonEqualExprs());
            }
            return JoinConditionSplitRes.of(equalExprs, leftFilters, rightFilters,
                    nonEqualExprs);
        }

        public List<RexNode> getLeftJoinKeys() {
            List<RexNode> leftJoinKeys = Lists.newArrayList();
            for (EqualExprs exprs : equalExprs) {
                leftJoinKeys.add(exprs.getLeftReference());
            }
            return leftJoinKeys;
        }

        public List<RexNode> getRightJoinKeys() {
            List<RexNode> rightJoinKeys = Lists.newArrayList();
            for (EqualExprs exprs : equalExprs) {
                rightJoinKeys.add(exprs.getRightReference());
            }
            return rightJoinKeys;
        }

        public JoinConditionSplitRes accept(RexVisitor<RexNode> shuttle) {
            List<EqualExprs> newEqualExprs = Lists.newArrayList();
            List<RexNode> newLeftFilters = Lists.newArrayList();
            List<RexNode> newRightFilters = Lists.newArrayList();
            List<RexNode> newNonEqualExprs = Lists.newArrayList();
            for (EqualExprs exprs : equalExprs) {
                EqualExprs newExprs = EqualExprs.of(
                        exprs.getLeftReference().accept(shuttle), exprs.getRightReference()
                                .accept(shuttle), exprs.isComparingNulls());
                newEqualExprs.add(newExprs);
            }
            for (RexNode leftFilter : leftFilters) {
                newLeftFilters.add(leftFilter.accept(shuttle));
            }
            for (RexNode rightFilter : rightFilters) {
                newRightFilters.add(rightFilter.accept(shuttle));
            }
            for (RexNode nonEqual : nonEqualExprs) {
                newNonEqualExprs.add(nonEqual.accept(shuttle));
            }
            return new JoinConditionSplitRes(newEqualExprs, newLeftFilters,
                    newRightFilters, newNonEqualExprs);
        }

        /**
         * swap the leftJoinKeys and the rightJoinKeys, swap the leftFilters and the
         * rightFilters
         *
         * @return
         */
        public JoinConditionSplitRes swapLeftRight() {
            List<EqualExprs> newEqualExprsList = Lists.newArrayList();
            for (EqualExprs exprs : equalExprs) {
                // swap the leftKey and the rightKey
                newEqualExprsList.add(EqualExprs.of(exprs.getRightReference(),
                        exprs.getLeftReference(), exprs.isComparingNulls()));
            }
            List<RexNode> newLeftFilters = rightFilters;
            List<RexNode> newRightFilters = leftFilters;
            return JoinConditionSplitRes.of(newEqualExprsList, newLeftFilters,
                    newRightFilters, nonEqualExprs);
        }
    }

    /**
     * Note that the right filters' column index are based on the right relNode.
     * The left filters' column index are based on the left relNode.
     *
     * @param left
     *          the left input relNode.
     * @param right
     *          the right input relNode.
     * @param condition
     *          the join's condition whose inputRefs are based on the input
     *          relNodes.
     * @param info
     * @param type
     * @param pushLeft
     *          whether push the extracted left filters to the left relNode.
     * @param pushRight
     *          whether push the extracted right filters to the right relNode.
     * @param filterOnJoin
     *          condition is from join on
     * @return
     */
    public static JoinConditionSplitRes analyzeJoinCondition(RelNode left,
                                                             RelNode right, RexNode condition, JoinInfo info, JoinRelType type,
                                                             boolean pushLeft, boolean pushRight, boolean filterOnJoin) {

        RexBuilder rexBuilder = left.getCluster().getRexBuilder();
        List<EqualExprs> equalExprs = Lists.newArrayList();
        List<RexNode> leftFilter = Lists.newArrayList();
        List<RexNode> rightFilter = Lists.newArrayList();
        List<RexNode> notEqualExprs = Lists.newArrayList();

        if (condition != null && !condition.isAlwaysTrue() /*full outer join*/) {
            throw new UnsupportedOperationException("analyzeJoinCondition: full outer join unsupported");
//            extractEqualExprsFromOnCondition(left.getCluster(), condition,
//                    left.getRowType().getFieldCount(),
//                    left.getRowType().getFieldCount() + right.getRowType().getFieldCount(),
//                    equalExprs, leftFilter, rightFilter, notEqualExprs, info, type,
//                    pushLeft, pushRight, false, filterOnJoin);
        }

        int leftCount = left.getRowType().getFieldCount();
        int rightCount = right.getRowType().getFieldCount();
        int[] adjustments = new int[leftCount + rightCount];
        for (int i = leftCount; i < leftCount + rightCount; i++) {
            adjustments[i] = -leftCount;
        }

        // If left count is 0, then it must be a map join whose join condition likes
        // 1=1 and rightTable.pt='xx'.
        for (int i = 0; i < rightFilter.size(); i++) {
            rightFilter.set(i, rightFilter.get(i).accept(
                    new RelOptUtil.RexInputConverter(
                            rexBuilder, leftCount == 0 ?
                            right.getRowType().getFieldList() :
                            left.getRowType().getFieldList(),
                            right.getRowType().getFieldList(),
                            adjustments)));
        }

        return new JoinConditionSplitRes(equalExprs, leftFilter, rightFilter,
                notEqualExprs);
    }



}
