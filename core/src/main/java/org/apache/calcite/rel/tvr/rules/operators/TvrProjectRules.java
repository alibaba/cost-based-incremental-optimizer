package org.apache.calcite.rel.tvr.rules.operators;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.trait.TvrValueDelta;
import org.apache.calcite.rel.tvr.trait.transformer.TvrAggregateTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrProjectTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrSemanticsTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrSortTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.predicate.TransformerPredicate;
import org.apache.calcite.rel.tvr.utils.RexInputRefReplacer;
import org.apache.calcite.rel.tvr.utils.TvrRelOptUtils;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.calcite.rel.tvr.rules.operators.TvrProjectRules.applyProject;

/**
 * <p>
 * Matches:
 * <p>
 *     LogicalProject  --SET_SNAPSHOT_MAX-- Tvr0
 *           |                                     (SetDelta/ValueDelta)
 *         input       --SET_SNAPSHOT_MAX-- Tvr1 ------------------------     input
 * <p>
 * Converts to:
 * <p>
 *                                                 (SetDelta/ValueDelta)
 *     LogicalProject  --SET_SNAPSHOT_MAX-- Tvr0 ------------------------   newProject
 *           |                                     (SetDelta/ValueDelta)         |
 *         input       --SET_SNAPSHOT_MAX-- Tvr1 ------------------------     input
 * <p>
 */
public abstract class TvrProjectRules {
  public static final TvrSetDeltaProjectRule TVR_SET_DELTA_PROJECT_RULE =
      new TvrSetDeltaProjectRule();
  public static final TvrValueSemanticsProjectRule
      TVR_VALUE_SEMANTICS_PROJECT_RULE = new TvrValueSemanticsProjectRule();

  /**
   * -                                ------------------- M4 ------------------
   * -     Select                    |                                         |     Select
   * -     AVG(C),      Project  <-- * <-- ProjTrans <-- * <-- Transformer <-- * <--  Project   SUM(C),COUNT(C),
   * -   1,  D,  D,        | \                           |                     |          /|     1, D, missing_key_A, COUNT()
   * -     AVG(C)          | |                            -------- M5 ---------          | |
   * -                     | M1                                                         M3 |
   * -                     | |              --------------M2---------------              | |
   * -                     |/              |                               |              \|
   * -  Group by A,D    Aggregate  <--     *    <--  Transformer  <--      *     <--  Aggregate A,D,SUM(B),SUM(C),COUNT(C),COUNT()
   * -  SUM(B), AVG(C)     |                                                             |
   * -                     |                                                             |
   * -    A,B,C,D        Input                                                         Input    A,B,C,D
   * -
   * -  M1 : [[3] -> 0, [2] -> null, [-1] -> 1, [1] -> 2, [1] -> 3, [3] -> 4, [0] -> null]
   * -  M2 : [0 -> [0], 1 -> [1], 2 -> [2], 3 -> [3, 4]]
   * -  M1, M2 => M4 : [[0, 1] -> 0, [2] -> 1, [3] -> 2, [3] -> 3, [0, 1] -> 4]
   * -            M3 : [3 -> 0, 4 -> 1, -1 -> 2, 1 -> 3, 0 -> 4, 5 -> 5]
   * -                 => Ref(3), Ref(4), Literal(1), Ref(1), Ref(0) (missing key), Ref(5)
   * -  New Transformer backward mapping M5: [[2] -> 0, [3] -> 1, [4] -> 2, [0, 1] -> 3]
   * -        M4 & M5 => [[3] -> 0, [0] -> 1, [1] -> 2, [1] -> 3, [3] -> 4]
   * -                => projTrans = [select $3, $0, $1, $1, $3]
   */
  public static Pair<TvrValueDelta, List<Pair<RexNode, String>>> applyProject(
      List<RelDataTypeField> inputFields, TvrValueDelta valueTrait,
      List<Pair<RexNode, String>> namedProjects, RelOptCluster cluster) {
    RexBuilder rexBuilder = cluster.getRexBuilder();

    // 1. use column mapping 1 and mapping 2 to create mapping 3 & 4 (see the figure above),
    //    so that we can get the new schema to create the project
    TvrSemanticsTransformer transformer = valueTrait.getTransformer();
    Map<Integer, ImmutableIntList> backwardMapping =
        transformer.backwardMapping();

    // use negative numbers to represent the new columns that added in the row type
    int addedColIndex = -1;
    Set<Integer> missingCols = new HashSet<>(transformer.getRequiredColumns());

    // this is M3 in the figure
    int newColIndex = 0;
    Multimap<Integer, Integer> newColMapping = ArrayListMultimap.create();

    // this is M4 in the figure
    Multimap<ImmutableIntList, Integer> newProjectMapping =
        ArrayListMultimap.create();

    // the InputRef in a RexNode should not point to the partial columns,
    // that is, the size of backwardMapping.get(i) must be 1.
    RexInputRefReplacer replacer =
        new RexInputRefReplacer(rexBuilder, i -> backwardMapping.get(i).get(0));
    List<RexNode> newProjects = new ArrayList<>();
    List<String> newNames = new ArrayList<>();

    for (int i = 0; i < namedProjects.size(); i++) {
      Pair<RexNode, String> pair = namedProjects.get(i);
      RexNode rexNode = pair.left;

      if (rexNode instanceof RexInputRef) {
        int originalIndex = ((RexInputRef) rexNode).getIndex();
        ImmutableIntList newInputRefs = backwardMapping.get(originalIndex);
        List<Integer> newArgs = new ArrayList<>();
        // create new projects and names
        for (int index : newInputRefs) {
          if (!newColMapping.containsKey(index)) {
            RelDataTypeField field = inputFields.get(index);
            newProjects.add(rexBuilder.makeInputRef(field.getType(), index));
            //  namedProjects might select the same column multiple times,
            //  leading to the same name appearing more than once.
            newNames.add(pair.right);
            newColMapping.put(index, newColIndex++);
            missingCols.remove(index);
          }
          newArgs.add(newColMapping.get(index).iterator().next());
        }
        newProjectMapping.put(ImmutableIntList.copyOf(newArgs), i);
      } else {
        newColMapping.put(addedColIndex--, newColIndex);
        newProjectMapping.put(ImmutableIntList.of(newColIndex++), i);

        RexNode newRexNode = replacer.apply(rexNode);
        newProjects.add(newRexNode);
        newNames.add(pair.right);
      }
    }

    for (int i : missingCols) {
      newColMapping.put(i, newColIndex++);

      RelDataTypeField field = inputFields.get(i);
      newProjects.add(rexBuilder.makeInputRef(field.getType(), i));
      String name = TvrUtils.makeUniqueName(field.getName(), newNames);
      newNames.add(name);
    }

    // 2. update original transformers in the value trait
    TvrSemanticsTransformer newOriginalTransformer =
        transformer.transform(newColMapping);

    // 3. add the new project transformer
    // this is M5 in the figure
    Multimap<ImmutableIntList, Integer> newForwardMapping =
        newOriginalTransformer.forwardMapping();

    // create new project transformer
    HashMap<Integer, Integer> newProjectRefMap = new HashMap<>();
    newProjectMapping.entries().forEach(e -> {
      Collection<Integer> inputRefs = newForwardMapping.get(e.getKey());
      assert inputRefs.size() == 1;
      newProjectRefMap.put(e.getValue(), inputRefs.iterator().next());
    });

    List<Integer> newProjectRefs =
        IntStream.range(0, newProjectRefMap.size()).boxed()
            .map(newProjectRefMap::get).collect(Collectors.toList());
    TvrProjectTransformer projectTransformer =
        new TvrProjectTransformer(ImmutableIntList.copyOf(newProjectRefs));
    TvrSemanticsTransformer newTransformer =
        newOriginalTransformer.addNewTransformer(projectTransformer);

    TvrValueDelta newTvrTrait = new TvrValueDelta(valueTrait.fromVersion, valueTrait.toVersion,
        newTransformer, valueTrait.isPositiveOnly());

    return Pair.of(newTvrTrait, Pair.zip(newProjects, newNames));
  }
}

abstract class TvrProjectRuleBase extends RelOptTvrRule {
  TvrProjectRuleBase(Class<? extends TvrSemantics> tvrClass) {
    super(operand(LogicalProject.class, tvrEdgeSSMax(tvr()),
        operand(RelSubset.class,
            tvrEdgeSSMax(tvr(tvrEdge(tvrClass, logicalSubset()))), any())));
  }
}

/**
 * Tvr set delta project rule
 */
class TvrSetDeltaProjectRule extends TvrProjectRuleBase {
  TvrSetDeltaProjectRule() {
    super(TvrSetDelta.class);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch projectAny = getRoot(call);
    LogicalProject logicalProject = projectAny.get();
    RelOfTvr inputSibling = projectAny.input(0).tvrSibling();
    RelNode leafDeltaRel = inputSibling.rel.get();
    TvrSetDelta inputTrait = (TvrSetDelta) inputSibling.tvrSemantics;
    RelDataTypeFactory typeFactory =
        logicalProject.getCluster().getTypeFactory();
    RexBuilder rexBuilder = logicalProject.getCluster().getRexBuilder();

    List<RexNode> newProjects = new ArrayList<>(logicalProject.getProjects());
    RelDataTypeFactory.Builder typeBuilder =
        new RelDataTypeFactory.Builder(typeFactory);
    typeBuilder.addAll(logicalProject.getRowType().getFieldList());

    if (!inputTrait.isPositiveOnly()) {
      int indexOfMultiplicity =
          inputTrait.getIndexOfMultiplicity(leafDeltaRel);
      newProjects
          .add(rexBuilder.makeInputRef(leafDeltaRel, indexOfMultiplicity));
      typeBuilder.add(
          leafDeltaRel.getRowType().getFieldList().get(indexOfMultiplicity));
    }

    RelNode newLogicalProject = logicalProject
        .copy(logicalProject.getTraitSet(), leafDeltaRel, newProjects,
            typeBuilder.build());

    transformToRootTvr(call, newLogicalProject, inputTrait);
  }
}

/**
 * Tvr value semantics project rule
 */
class TvrValueSemanticsProjectRule extends TvrProjectRuleBase {

  private static final TransformerPredicate inputRefPredicate =
      new TransformerPredicate() {
        @Override
        public boolean test(TvrAggregateTransformer transformer,
                            ImmutableIntList indices) {
          return indices.size() == 1;
        }

        @Override
        public boolean test(TvrProjectTransformer transformer,
            ImmutableIntList indices) {
          return indices.size() == 1;
        }

        @Override
        public boolean test(TvrSortTransformer transformer,
                            ImmutableIntList indices) {
          return indices.size() == 1;
        }
      };

  private static final TransformerPredicate nonInputRefPredicate =
      new TransformerPredicate() {
        @Override
        public boolean test(TvrAggregateTransformer transformer,
            ImmutableIntList indices) {
          if (indices.size() != 1) {
            return false;
          }
          return indices.stream().noneMatch(transformer::referToPartialResult);
        }

        @Override
        public boolean test(TvrProjectTransformer transformer,
            ImmutableIntList indices) {
          return indices.size() == 1;
        }

        @Override
        public boolean test(TvrSortTransformer transformer,
            ImmutableIntList indices) {
          // can not do the computation on the row number column (i.e., ROW_NUMBER + 1)
          // if the 'limit' is not equal to 1.
          Integer rowNumberColIndex = transformer.getRowNumberColIndex();
          // 'rowNumberColIndex == null' means that row number column is not kept in the current schema,
          // and it has been removed by the previous projects.
          return indices.size() == 1 && rowNumberColIndex == null || (
              indices.stream().noneMatch(i -> i.equals(rowNumberColIndex))
                  || transformer.getLimit().equals(BigDecimal.ONE));
        }
      };

  TvrValueSemanticsProjectRule() {
    super(TvrValueDelta.class);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RelMatch projectAny = getRoot(call);
    LogicalProject logicalProject = projectAny.get();
    TvrValueDelta inputTrait =
        (TvrValueDelta) projectAny.input(0).tvrSibling().tvrSemantics;
    // Partial aggregate columns cannot participate in complex expressions.
    TvrSemanticsTransformer transformer = inputTrait.getTransformer();

    return logicalProject.getProjects().stream().allMatch(project -> {
      TransformerPredicate predicate = project instanceof RexInputRef ?
          inputRefPredicate :
          nonInputRefPredicate;
      return TvrRelOptUtils.findUniqueInputRef(project)
          .stream().allMatch(p -> transformer.isCompatible(predicate, ImmutableIntList.of(p.getIndex())));
    });
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch projectAny = getRoot(call);
    LogicalProject logicalProject = projectAny.get();
    RelOfTvr inputSibling = projectAny.input(0).tvrSibling();
    RelNode leafSnapshotRel = inputSibling.rel.get();
    TvrValueDelta inputTrait =
        (TvrValueDelta) inputSibling.tvrSemantics;
    RelDataTypeFactory typeFactory =
        logicalProject.getCluster().getTypeFactory();

    Pair<TvrValueDelta, List<Pair<RexNode, String>>> pair =
        applyProject(leafSnapshotRel.getRowType().getFieldList(),
            inputTrait, logicalProject.getNamedProjects(),
            leafSnapshotRel.getCluster());

    List<RexNode> newProjects = Pair.left(pair.right);
    List<String> newNames = Pair.right(pair.right);
    RelDataTypeFactory.Builder typeBuilder =
        new RelDataTypeFactory.Builder(typeFactory);
    for (int i = 0; i < newProjects.size(); i++) {
      typeBuilder.add(newNames.get(i), newProjects.get(i).getType());
    }
    RelDataType rowType = typeBuilder.build();

    RelNode newLogicalProject = logicalProject
        .copy(logicalProject.getTraitSet(), leafSnapshotRel, newProjects,
            rowType);

    transformToRootTvr(call, newLogicalProject, pair.left);
  }
}
