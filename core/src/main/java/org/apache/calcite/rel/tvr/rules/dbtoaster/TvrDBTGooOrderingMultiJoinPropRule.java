package org.apache.calcite.rel.tvr.rules.dbtoaster;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrMetaSetType;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil;
import org.apache.calcite.rel.tvr.trait.property.TvrUpdateOneTableProperty;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrUtils;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.calcite.plan.tvr.TvrSemantics.SET_SNAPSHOT_MAX;
import static org.apache.calcite.rel.tvr.trait.property.TvrUpdateOneTableProperty.PropertyType.DB_TOASTER;

public class TvrDBTGooOrderingMultiJoinPropRule extends RelOptTvrRule
    implements InterTvrRule {

  public static final TvrDBTGooOrderingMultiJoinPropRule INSTANCE
      = new TvrDBTGooOrderingMultiJoinPropRule();

  private static RelOptRuleOperand getRootOperand() {
    Supplier<RelOptRuleOperand> child = () -> operand(RelSubset.class,
        tvrEdgeSSMax(tvr(tvrProperty(TvrUpdateOneTableProperty.class,
            p -> p.getType() == DB_TOASTER, tvr(false)))), any());
    return operand(MultiJoin.class, tvrEdgeSSMax(tvr()),
        unordered(child.get()));
  }

  public TvrDBTGooOrderingMultiJoinPropRule() {
    super(getRootOperand());
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    RelNode rootRel = root.get();
    TvrMetaSet rootTvr = root.tvr().get();
    TvrMetaSetType rootTvrType = rootTvr.getTvrType();

    VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();

    RelMatch matchedInput = root.input(0);
    TvrMetaSet inputTvr = matchedInput.tvr().get();
    TvrUpdateOneTableProperty tvrProp = matchedInput.tvr().property();

    // must match DB_TOASTER tvr property
    if (tvrProp.getType() != DB_TOASTER) {
      return false;
    }

    // root tvr and input tvr type must be the same
    if (!rootTvrType.equals(inputTvr.getTvrType())) {
      return false;
    }

    for (int i = 0; i < rootRel.getInputs().size(); i++) {
      RelNode input = rootRel.getInput(i);
      // find the set snapshot max tvrs of this input
      Set<TvrMetaSet> tvrMetaSets = planner.getSubset(input).getTvrLinks()
          .get(SET_SNAPSHOT_MAX);
      if (tvrMetaSets.isEmpty()) {
        return false;
      }
      // find the tvr with the same tvr type
      Optional<TvrMetaSet> tvr = tvrMetaSets.stream()
          .filter(t -> t.getTvrType().equals(rootTvrType)).findFirst();
      if (! tvr.isPresent()) {
        return false;
      }
      // find the sibling tvr with the same dbtoaster property
      Map<TvrUpdateOneTableProperty, TvrMetaSet> propLinks =
          tvr.get().getTvrPropertyLinks(p -> p instanceof TvrUpdateOneTableProperty && tvrProp.equals(p));
      if (propLinks.isEmpty()) {
        return false;
      }
      assert propLinks.size() == 1;
    }

    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    MultiJoin multiJoin = root.get();
    TvrMetaSet rootTvr = root.tvr().get();
    TvrMetaSetType rootTvrType = rootTvr.getTvrType();

    VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();
    RelOptCluster cluster = multiJoin.getCluster();
    TvrContext ctx = TvrContext.getInstance(cluster);

    TvrUpdateOneTableProperty property = root.input(0).tvr().property();
    assert property.getType() == DB_TOASTER;
    TvrMetaSetType updateOneTableTvrType = TvrPropertyUtil
        .getDbtUpdateOneTableTvrType(property.getChangingTable(), rootTvrType);
    assert updateOneTableTvrType != TvrMetaSetType.DEFAULT;

    // follow the property link to find the updateTable input relNode
    List<RelNode> newInputs = new ArrayList<>();
    for (RelNode input : multiJoin.getInputs()) {
      Set<TvrMetaSet> tvrMetaSets = planner.getSubset(input).getTvrLinks()
          .get(SET_SNAPSHOT_MAX);
      assert ! tvrMetaSets.isEmpty();
      Optional<TvrMetaSet> tvr = tvrMetaSets.stream()
          .filter(t -> t.getTvrType().equals(rootTvrType)).findFirst();
      assert tvr.isPresent();
      Map<TvrUpdateOneTableProperty, TvrMetaSet> propLinks =
          tvr.get().getTvrPropertyLinks(p -> p instanceof TvrUpdateOneTableProperty && property.equals(p));
      assert !propLinks.isEmpty();
      assert propLinks.size() == 1;
      TvrMetaSet updateTvr = propLinks.values().iterator().next();

      RelNode relNode = updateTvr.getSubset(SET_SNAPSHOT_MAX, cluster.traitSet());
      newInputs.add(relNode);
      assert relNode != null;
    }
    // construct the newMultiJoin for updateX
    MultiJoin updateMultiJoin = (MultiJoin) multiJoin.copy(multiJoin.getTraitSet(), newInputs);

    List<Set<Integer>> tableNameForEachInput =
        multiJoin.getInputs().stream().map(TvrUtils::collectUpdateTableNames)
            .map(tableNames -> tableNames.stream().map(ctx::tableOrdinal)
                .collect(Collectors.toSet())).collect(Collectors.toList());
    Multimap<Integer, Integer> revTableJoinInput = HashMultimap.create();
    for (int i = 0; i < tableNameForEachInput.size(); i++) {
      for (Integer t : tableNameForEachInput.get(i)) {
        revTableJoinInput.put(t, i);
      }
    }

    Integer changingTable = property.getChangingTable();
    List<Integer> updatedInputs =
        new ArrayList<>(revTableJoinInput.get(changingTable));

    // right now we only support one branch update one input
    // other cases will fall back to regular goo join ordering rule
    Integer updatedInput;
    if (updatedInputs.size() == 1) {
      updatedInput = updatedInputs.get(0);
    } else {
      updatedInput = null;
    }

    RelNode newJoin = new DBTGooJoinOrderingRule(false,
        null, "TvrDBToasterGooJoinOrderingRule-temp")
        .transformDBT(updateMultiJoin, call.builder(), updatedInput);

    if (newJoin == null) {
      return;
    }

    // dbtoaster part1: register new join order for updateX
    RelOptRuleCall.TransformBuilder builder = call.transformBuilder();
    builder.addPropertyLink(rootTvr, property, newJoin,
        updateOneTableTvrType);
    builder.addPropertyLink(rootTvr, property, updateMultiJoin,
        updateOneTableTvrType);
    builder.addEquiv(newJoin, updateMultiJoin);
    builder.transform();

    // dbtoaster part2: mark update-independent relNodes as view
    // this process will work from bottom to top
    List<RelNode> newRels = collectNewRels(newJoin);
    newRels.add(updateMultiJoin);

    // create mapping from the updateX relNode to snapshot max in original tvr
    Map<RelNode, RelNode> mapping = new IdentityHashMap<>();
    // add the leaf mapping
    for (int i = 0; i < multiJoin.getInputs().size(); i++) {
      mapping.put(newInputs.get(i), multiJoin.getInput(i));
    }

    for (RelNode rel : newRels) {
      markAsDbtoasterViewIfNeeded(call, rootTvrType, rel, property, mapping, ctx);
    }
  }

  // mark the relNode as a View (default tvr type) for DBToaster
  // if the RelNode does not involve the table we are updating right now
  // create a default tvr for this relNode and registers its snapshot max
  private void markAsDbtoasterViewIfNeeded(RelOptRuleCall call,
      TvrMetaSetType tvrType, RelNode rel, TvrUpdateOneTableProperty property,
      Map<RelNode, RelNode> mapping, TvrContext ctx) {

    List<RelNode> originalMaxInputs = new ArrayList<>();
    for (RelNode inputRel : rel.getInputs()) {
      RelNode inputView = mapping.get(inputRel);
      assert inputView != null;
      originalMaxInputs.add(inputView);
    }
    // register the original snapshot max relNode as a dbtoaster view
    RelNode newViewRel = rel.copy(rel.getTraitSet(), originalMaxInputs);
    mapping.put(rel, newViewRel);

    // don't register relNode as view if it is not update-independent
    Set<Integer> tables =
        TvrUtils.collectUpdateTableNames(rel).stream().map(ctx::tableOrdinal)
            .collect(Collectors.toSet());
    if (tables.contains(property.getChangingTable())) {
      return;
    }

    RelOptRuleCall.TransformBuilder viewBuilder = call.transformBuilder();
    viewBuilder.addTvrType(newViewRel, tvrType);
    viewBuilder.transform();
  }

  // collect newly created relNodes during this rule call
  // the returned list will be ordered from bottom to top
  private static List<RelNode> collectNewRels(RelNode rel) {
    LinkedHashSet<RelNode> newRels = new LinkedHashSet<>();
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node == null) {
          return;
        }
        if (newRels.contains(node)) {
          return;
        }
        // stop visiting when encountered a RelSubset
        if (node instanceof RelSubset) {
          return;
        }
        super.visit(node, ordinal, parent);
        newRels.add(node);
      }
    }.go(rel);
    return new ArrayList<>(newRels);
  }

}