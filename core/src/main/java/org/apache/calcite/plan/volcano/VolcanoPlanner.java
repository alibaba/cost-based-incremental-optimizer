/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.plan.volcano;

import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.AbstractRelOptPlanner;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.MulticastRelOptListener;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.TvrMetaSet.TvrConvertMatchPattern;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.convert.Converter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.metadata.CyclicMetadataException;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.CalcRemoveRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.JoinAssociateRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.SaffronProperties;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

/**
 * VolcanoPlanner optimizes queries by transforming expressions selectively
 * according to a dynamic programming algorithm.
 */
public class VolcanoPlanner extends AbstractRelOptPlanner {
  //~ Static fields/initializers ---------------------------------------------

  protected static final double COST_IMPROVEMENT = .5;

  protected EnumSet<VolcanoPlannerPhase> phasesToDrainTvrConverters =
      EnumSet.of(VolcanoPlannerPhase.values()[0]);

  //~ Instance fields --------------------------------------------------------

  protected RelSubset root;

  /**
   * If true, the planner keeps applying rules as long as they continue to
   * reduce the cost. If false, the planner terminates as soon as it has found
   * any implementation, no matter how expensive.
   */
  protected boolean ambitious = true;

  /**
   * If true, and if {@link #ambitious} is true, the planner waits a finite
   * number of iterations for the cost to improve.
   *
   * <p>The number of iterations K is equal to the number of iterations
   * required to get the first finite plan. After the first finite plan, it
   * continues to fire rules to try to improve it. The planner sets a target
   * cost of the current best cost multiplied by {@link #COST_IMPROVEMENT}. If
   * it does not meet that cost target within K steps, it quits, and uses the
   * current best plan. If it meets the cost, it sets a new, lower target, and
   * has another K iterations to meet it. And so forth.
   *
   * <p>If false, the planner continues to fire rules until the rule queue is
   * empty.
   */
  protected boolean impatient = false;

  /**
   * Operands that apply to a given class of {@link RelNode}.
   *
   * <p>Any operand can be an 'entry point' to a rule call, when a RelNode is
   * registered which matches the operand. This map allows us to narrow down
   * operands based on the class of the RelNode.</p>
   */
  private final Multimap<Class<? extends RelNode>, RelOptRuleOperand>
      classOperands = LinkedListMultimap.create();

  private final Set<Class<? extends TvrSemantics>> knownTvrEdgeClasses =
      new LinkedHashSet<>();
  private final Set<Class<? extends TvrProperty>> knownTvrPropertyClasses =
      new LinkedHashSet<>();

  private final Set<TvrRelOptRuleOperand> tvrOperands = new LinkedHashSet<>();
  private final Multimap<Class<? extends TvrSemantics>,
      TvrEdgeRelOptRuleOperand>
      tvrEdgeToOperands = LinkedListMultimap.create();
  private final Multimap<Class<? extends TvrProperty>,
      TvrPropertyEdgeRuleOperand>
      tvrPropertyEdgeToOperands = LinkedListMultimap.create();

  private final Multimap<Class<? extends TvrSemantics>, TvrConvertMatchPattern>
      tvrConvertMatchPatterns = LinkedListMultimap.create();

  private final Set<TvrMetaSet> hasDelayedTvrConverters = new LinkedHashSet<>();

  /**
   * List of all sets. Used only for debugging.
   */
  protected final List<RelSet> allSets = new ArrayList<>();

  /**
   * Canonical map from {@link String digest} to the unique
   * {@link RelNode relational expression} with that digest.
   *
   * <p>Row type is part of the key for the rare occasion that similar
   * expressions have different types, e.g. variants of
   * {@code Project(child=rel#1, a=null)} where a is a null INTEGER or a
   * null VARCHAR(10).
   */
  private final Map<Pair<String, RelDataType>, RelNode> mapDigestToRel =
      new HashMap<>();

  /**
   * Map each registered expression ({@link RelNode}) to its equivalence set
   * ({@link RelSubset}).
   *
   * <p>We use an {@link IdentityHashMap} to simplify the process of merging
   * {@link RelSet} objects. Most {@link RelNode} objects are identified by
   * their digest, which involves the set that their child relational
   * expressions belong to. If those children belong to the same set, we have
   * to be careful, otherwise it gets incestuous.</p>
   */
  private final IdentityHashMap<RelNode, RelSubset> mapRel2Subset =
      new IdentityHashMap<>();

  /**
   * The importance of relational expressions.
   *
   * <p>The map contains only RelNodes whose importance has been overridden
   * using {@link RelOptPlanner#setImportance(RelNode, double)}. Other
   * RelNodes are presumed to have 'normal' importance.
   *
   * <p>If a RelNode has 0 importance, all {@link RelOptRuleCall}s using it
   * are ignored, and future RelOptRuleCalls are not queued up.
   */
  final Map<RelNode, Double> relImportances = new HashMap<>();

  public static final double DEFAULT_REL_IMPORTANCE = 1.0d;

  protected final SetMultimap<RelNode, TvrMetaSetType> tvrGenericRels =
      LinkedHashMultimap.create();

  /**
   * List of all schemas which have been registered.
   */
  public final Set<RelOptSchema> registeredSchemas = new HashSet<>();

  /**
   * Holds rule calls waiting to be fired.
   */
  final RuleQueue ruleQueue = new RuleQueue(this);

  /**
   * Holds the currently registered RelTraitDefs.
   */
  private final List<RelTraitDef> traitDefs = new ArrayList<>();

  /**
   * Set of all registered rules.
   */
  protected final Set<RelOptRule> ruleSet = new HashSet<>();

  private int nextSetId = 0;

  /**
   * Incremented every time a relational expression is registered or two sets
   * are merged. Tells us whether anything is going on.
   */
  private int registerCount;

  /**
   * Dump of the root relational expression, as it was before any rules were
   * applied. For debugging.
   */
  private String originalRootString;

  private RelNode originalRoot;

  /**
   * Whether the planner can accept new rules.
   */
  private boolean locked;

  private final List<RelOptMaterialization> materializations =
      new ArrayList<>();

  /**
   * Map of lattices by the qualified name of their star table.
   */
  private final Map<List<String>, RelOptLattice> latticeByName =
      new LinkedHashMap<>();

  final Map<RelNode, Provenance> provenanceMap = new HashMap<>();;

  private final Deque<VolcanoRuleCall> ruleCallStack = new ArrayDeque<>();

  /** Zero cost, according to {@link #costFactory}. Not necessarily a
   * {@link org.apache.calcite.plan.volcano.VolcanoCost}. */
  private final RelOptCost zeroCost;

  /** Maps rule classes to their name, to ensure that the names are unique and
   * conform to rules. */
  private final SetMultimap<String, Class> ruleNames =
      LinkedHashMultimap.create();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a uninitialized <code>VolcanoPlanner</code>. To fully initialize
   * it, the caller must register the desired set of relations, rules, and
   * calling conventions.
   */
  public VolcanoPlanner() {
    this(null, null);
  }

  /**
   * Creates a uninitialized <code>VolcanoPlanner</code>. To fully initialize
   * it, the caller must register the desired set of relations, rules, and
   * calling conventions.
   */
  public VolcanoPlanner(Context externalContext) {
    this(null, externalContext);
  }

  /**
   * Creates a {@code VolcanoPlanner} with a given cost factory.
   */
  public VolcanoPlanner(RelOptCostFactory costFactory, //
      Context externalContext) {
    super(costFactory == null ? VolcanoCost.FACTORY : costFactory, //
        externalContext);
    this.zeroCost = this.costFactory.makeZeroCost();
  }

  //~ Methods ----------------------------------------------------------------

  protected VolcanoPlannerPhaseRuleMappingInitializer
      getPhaseRuleMappingInitializer() {
    return phaseRuleMap -> {
      // Disable all phases except OPTIMIZE by adding one useless rule name.
      Arrays.stream(VolcanoPlannerPhase.values()).skip(1)
          .forEach(phase -> phaseRuleMap.get(phase).add("xxx"));
    };
  }

  // implement RelOptPlanner
  public boolean isRegistered(RelNode rel) {
    return mapRel2Subset.get(rel) != null;
  }

  public void setRoot(RelNode rel) {
    // We're registered all the rules, and therefore RelNode classes,
    // we're interested in, and have not yet started calling metadata providers.
    // So now is a good time to tell the metadata layer what to expect.
    registerMetadataRels();

    if (this.originalRoot == null) {
      this.originalRoot = rel;
      this.originalRootString =
          RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES);
    }

    this.root =
        registerAsTvrGeneric(rel, ImmutableSet.of(TvrMetaSetType.DEFAULT),
            ImmutableMap.of());

    // Making a node the root changes its importance.
    this.ruleQueue.recompute(this.root);
    ensureRootConverters();
  }

  public RelNode getRoot() {
    return root;
  }

  public ImmutableList<RelOptMaterialization> getMaterializations() {
    return ImmutableList.copyOf(materializations);
  }

  @Override public void addMaterialization(
      RelOptMaterialization materialization) {
    materializations.add(materialization);
  }

  @Override public void addLattice(RelOptLattice lattice) {
    latticeByName.put(lattice.starRelOptTable.getQualifiedName(), lattice);
  }

  @Override public RelOptLattice getLattice(RelOptTable table) {
    return latticeByName.get(table.getQualifiedName());
  }

  private void registerMaterializations() {
    // Avoid using materializations while populating materializations!
    final CalciteConnectionConfig config =
        context.unwrap(CalciteConnectionConfig.class);
    if (config == null || !config.materializationsEnabled()) {
      return;
    }

    // Register rels using materialized views.
    final List<Pair<RelNode, List<RelOptMaterialization>>> materializationUses =
        RelOptMaterializations.useMaterializedViews(originalRoot, materializations);
    for (Pair<RelNode, List<RelOptMaterialization>> use : materializationUses) {
      RelNode rel = use.left;
      Hook.SUB.run(rel);
      registerImpl(rel, root.set);
    }

    // Register table rels of materialized views that cannot find a substitution
    // in root rel transformation but can potentially be useful.
    final Set<RelOptMaterialization> applicableMaterializations =
        new HashSet<>(
            RelOptMaterializations.getApplicableMaterializations(
                originalRoot, materializations));
    for (Pair<RelNode, List<RelOptMaterialization>> use : materializationUses) {
      applicableMaterializations.removeAll(use.right);
    }
    for (RelOptMaterialization materialization : applicableMaterializations) {
      RelSubset subset = registerImpl(materialization.queryRel, null);
      RelNode tableRel2 =
          RelOptUtil.createCastRel(
              materialization.tableRel,
              materialization.queryRel.getRowType(),
              true);
      registerImpl(tableRel2, subset.set);
    }

    // Register rels using lattices.
    final List<Pair<RelNode, RelOptLattice>> latticeUses =
        RelOptMaterializations.useLattices(
            originalRoot, ImmutableList.copyOf(latticeByName.values()));
    if (!latticeUses.isEmpty()) {
      RelNode rel = latticeUses.get(0).left;
      Hook.SUB.run(rel);
      registerImpl(rel, root.set);
    }
  }

  /**
   * Finds an expression's equivalence set. If the expression is not
   * registered, returns null.
   *
   * @param rel Relational expression
   * @return Equivalence set that expression belongs to, or null if it is not
   * registered
   */
  public RelSet getSet(RelNode rel) {
    assert rel != null : "pre: rel != null";
    final RelSubset subset = getSubset(rel);
    if (subset != null) {
      assert subset.set != null;
      return subset.set;
    }
    return null;
  }

  @Override public boolean addRelTraitDef(RelTraitDef relTraitDef) {
    return !traitDefs.contains(relTraitDef) && traitDefs.add(relTraitDef);
  }

  @Override public void clearRelTraitDefs() {
    traitDefs.clear();
  }

  @Override public List<RelTraitDef> getRelTraitDefs() {
    return traitDefs;
  }

  @Override public RelTraitSet emptyTraitSet() {
    RelTraitSet traitSet = super.emptyTraitSet();
    for (RelTraitDef traitDef : traitDefs) {
      if (traitDef.multiple()) {
        // TODO: restructure RelTraitSet to allow a list of entries
        //  for any given trait
      }
      traitSet = traitSet.plus(traitDef.getDefault());
    }
    return traitSet;
  }

  @Override public void clear() {
    super.clear();
    for (RelOptRule rule : ImmutableList.copyOf(ruleSet)) {
      removeRule(rule);
    }
    this.classOperands.clear();
    this.tvrOperands.clear();
    this.tvrEdgeToOperands.clear();
    this.tvrPropertyEdgeToOperands.clear();
    this.tvrConvertMatchPatterns.clear();
    this.allSets.clear();
    this.mapDigestToRel.clear();
    this.mapRel2Subset.clear();
    this.relImportances.clear();
    this.ruleQueue.clear();
    this.ruleNames.clear();
    this.materializations.clear();
    this.latticeByName.clear();
    this.root = null;
    this.originalRoot = null;
  }

  public List<RelOptRule> getRules() {
    return ImmutableList.copyOf(ruleSet);
  }

  public boolean addRule(RelOptRule rule) {
    if (locked) {
      return false;
    }
    if (ruleSet.contains(rule)) {
      // Rule already exists.
      return false;
    }
    final boolean added = ruleSet.add(rule);
    assert added;

    final String ruleName = rule.toString();
    if (ruleNames.put(ruleName, rule.getClass())) {
      Set<Class> x = ruleNames.get(ruleName);
      if (x.size() > 1) {
        throw new RuntimeException("Rule description '" + ruleName
            + "' is not unique; classes: " + x);
      }
    }

    mapRuleDescription(rule);

    // Each of this rule's operands is an 'entry point' for a rule call.
    // Register each operand against all concrete sub-classes that could match
    // it.
    for (RelOptRuleOperand operand : rule.getOperands()) {
      if (operand instanceof TvrRelOptRuleOperand) {
        tvrOperands.add((TvrRelOptRuleOperand) operand);
      } else if (operand instanceof TvrEdgeRelOptRuleOperand) {
        TvrEdgeRelOptRuleOperand op = (TvrEdgeRelOptRuleOperand) operand;
        Iterable<Class<? extends TvrSemantics>> subClasses =
            Util.filter(knownTvrEdgeClasses,
                op.getMatchedTvrClass()::isAssignableFrom);
        subClasses.forEach(c -> tvrEdgeToOperands.put(c, op));
      } else if (operand instanceof TvrPropertyEdgeRuleOperand) {
        TvrPropertyEdgeRuleOperand op = (TvrPropertyEdgeRuleOperand) operand;
        Iterable<Class<? extends TvrProperty>> subClasses =
            Util.filter(knownTvrPropertyClasses,
                op.getMatchedPropertyClass()::isAssignableFrom);
        subClasses.forEach(c -> tvrPropertyEdgeToOperands.put(c, op));
      } else {
        for (Class<? extends RelNode> subClass : subClasses(
            operand.getMatchedClass())) {
          classOperands.put(subClass, operand);
        }
      }
    }

    if (rule instanceof TvrConvertMatchPattern) {
      TvrConvertMatchPattern pattern = (TvrConvertMatchPattern) rule;
      pattern.getRelatedTvrClasses().forEach(clazz -> {
        Iterable<Class<? extends TvrSemantics>> subClasses =
            Util.filter(knownTvrEdgeClasses, clazz::isAssignableFrom);
        subClasses.forEach(c -> tvrConvertMatchPatterns.put(c, pattern));
      });
    }

    // If this is a converter rule, check that it operates on one of the
    // kinds of trait we are interested in, and if so, register the rule
    // with the trait.
    if (rule instanceof ConverterRule) {
      ConverterRule converterRule = (ConverterRule) rule;

      final RelTrait ruleTrait = converterRule.getInTrait();
      final RelTraitDef ruleTraitDef = ruleTrait.getTraitDef();
      if (traitDefs.contains(ruleTraitDef)) {
        ruleTraitDef.registerConverterRule(this, converterRule);
      }
    }

    return true;
  }

  public boolean removeRule(RelOptRule rule) {
    if (!ruleSet.remove(rule)) {
      // Rule was not present.
      return false;
    }

    // Remove description.
    unmapRuleDescription(rule);

    // Remove operands.
    for (Iterator<RelOptRuleOperand> iter = classOperands.values().iterator();
         iter.hasNext();) {
      RelOptRuleOperand entry = iter.next();
      if (entry.getRule().equals(rule)) {
        iter.remove();
      }
    }

    // Remove trait mappings. (In particular, entries from conversion
    // graph.)
    if (rule instanceof ConverterRule) {
      ConverterRule converterRule = (ConverterRule) rule;
      final RelTrait ruleTrait = converterRule.getInTrait();
      final RelTraitDef ruleTraitDef = ruleTrait.getTraitDef();
      if (traitDefs.contains(ruleTraitDef)) {
        ruleTraitDef.deregisterConverterRule(this, converterRule);
      }
    }
    return true;
  }

  @Override protected void onNewClass(RelNode node) {
    super.onNewClass(node);

    // Create mappings so that instances of this class will match existing
    // operands.
    final Class<? extends RelNode> clazz = node.getClass();
    for (RelOptRule rule : ruleSet) {
      for (RelOptRuleOperand operand : rule.getOperands()) {
        if (operand.getMatchedClass().isAssignableFrom(clazz)) {
          classOperands.put(clazz, operand);
        }
      }
    }
  }

  public void registerTvrEdgeClass(TvrSemantics tvrKey) {
    Class<? extends TvrSemantics> clazz = tvrKey.getClass();
    if (knownTvrEdgeClasses.add(clazz)) {
      // Create mappings to existing rule operands for this new class
      for (RelOptRule rule : ruleSet) {
        for (RelOptRuleOperand operand : rule.getOperands()) {
          if (!(operand instanceof TvrEdgeRelOptRuleOperand)) {
            continue;
          }
          TvrEdgeRelOptRuleOperand op = (TvrEdgeRelOptRuleOperand) operand;
          if (op.getMatchedTvrClass().isAssignableFrom(clazz)) {
            tvrEdgeToOperands.put(clazz, op);
          }
        }

        if (rule instanceof TvrConvertMatchPattern) {
          TvrConvertMatchPattern pattern = (TvrConvertMatchPattern) rule;
          if (pattern.getRelatedTvrClasses().stream()
              .anyMatch(cl -> cl.isAssignableFrom(clazz))) {
            tvrConvertMatchPatterns.put(clazz, pattern);
          }
        }
      }
    }
  }

  public void registerTvrPropertyClass(TvrProperty tvrProperty) {
    Class<? extends TvrProperty> clazz = tvrProperty.getClass();
    if (knownTvrPropertyClasses.add(clazz)) {
      // Create mappings to existing rule operands for this new class
      for (RelOptRule rule : ruleSet) {
        for (RelOptRuleOperand operand : rule.getOperands()) {
          if (!(operand instanceof TvrPropertyEdgeRuleOperand)) {
            continue;
          }
          TvrPropertyEdgeRuleOperand op = (TvrPropertyEdgeRuleOperand) operand;
          if (op.getMatchedPropertyClass().isAssignableFrom(clazz)) {
            tvrPropertyEdgeToOperands.put(clazz, op);
          }
        }
      }
    }
  }

  public RelNode changeTraits(final RelNode rel, RelTraitSet toTraits) {
    assert !rel.getTraitSet().equals(toTraits);
    assert toTraits.allSimple();

    RelSubset rel2 = ensureRegistered(rel, null);
    if (rel2.getTraitSet().equals(toTraits)) {
      return rel2;
    }

    return rel2.set.getOrCreateSubset(rel.getCluster(), toTraits.simplify());
  }

  private RelSubset registerOneLevelWithTvrTypes(RelNode newRel, RelNode equiv,
      Set<TvrMetaSetType> tvrTypes) {
    // Update the relNode with registered inputs and recompute digest
    RelNode rel = newRel.onRegister(this);
    // Remember the equiv nodes that's already registered if any. This equiv
    // node ('s input) might change as a result of set merge when we register
    // below
    RelNode oldEquivExp = mapDigestToRel.get(key(rel));

    assert rel.getInputs().stream()
        .allMatch(input -> input instanceof RelSubset);
    RelSubset subset = ensureRegistered(rel, equiv);

    if (!tvrTypes.isEmpty()) {
      if (!(newRel instanceof RelSubset)) {
        // Find the registered relNode and remember the generic tvr types
        RelNode equivExp;
        if (oldEquivExp == null) {
          equivExp = mapDigestToRel.get(key(rel));
        } else {
          // oldEquivExp's input might have changed and oldEquivExp might
          // have been discarded as a result of merge
          equivExp = mapDigestToRel.get(key(oldEquivExp));
        }
        assert equivExp != null;

        // add all new tvrTypes
        boolean setChanged = tvrGenericRels.putAll(equivExp, tvrTypes);

        // fire rules as a rel is newly registered as a Tvr generic rel
        if (setChanged) {
          fireRules(equivExp, true);
        }
      }
      // Make the RelSet a TvrRelSet of the requested types
      subset.set.makeTvrSetIfPossible(this, tvrTypes);
    }
    return subset;
  }

  /**
   * Register a relNode (and its unregistered children) and connect them to new
   * tvrs of given types. If tvrTypes is empty, no new tvr is created, meaning
   * the relNode is not generic to any tvr.
   *
   * This is similar to ensureRegistered, but with added tvrType handling.
   * We keep it a separate api so that register api remains unchanged.
   */
  public RelSubset registerAsTvrGeneric(RelNode rel,
      Set<TvrMetaSetType> tvrTypes, Map<RelNode, RelNode> equiv) {
    if (rel instanceof RelSubset || isRegistered(rel)) {
      return registerOneLevelWithTvrTypes(rel, equiv.get(rel), tvrTypes);
    }
    // Register inputs as generic recursively first
    for (RelNode input : rel.getInputs()) {
      registerAsTvrGeneric(input, tvrTypes, equiv);
    }
    return registerOneLevelWithTvrTypes(rel, equiv.get(rel), tvrTypes);
  }

  public RelNode getOrCreateSetFromTvrSet(RelNode rel, TvrMetaSetType tvrType,
      TvrSemantics toTvrKey, RelDataType newSetRowType, RelTraitSet toTraits) {
    RelSubset rel2 =
        registerAsTvrGeneric(rel, ImmutableSet.of(tvrType), ImmutableMap.of());
    TvrMetaSet tvr = getSet(rel2).getTvrForTvrSet(tvrType);
    return getOrCreateSetForTvr(rel.getCluster(), tvr, toTvrKey, newSetRowType,
        toTraits);
  }

  public RelNode getOrCreateSetForTvr(RelOptCluster cluster, TvrMetaSet tvr,
      TvrSemantics toTvrKey, RelDataType newSetRowType, RelTraitSet toTraits) {
    // Find or create the RelSet we are looking for
    RelSet set = tvr.getRelSet(toTvrKey);
    if (set == null) {
      // TvrRelSet should not be created this way
      assert toTvrKey != TvrSemantics.SET_SNAPSHOT_MAX;

      // Create a non-Tvr RelSet for the TvrMetaSet
      set = new RelSet(nextSetId++, new HashSet<>(), new HashSet<>(), cluster,
          newSetRowType);
      this.allSets.add(set);

      set.addTvrLink(this, null, toTvrKey, tvr);
      LOGGER.debug("Empty set " + set.id + " with TvrSemantic " + toTvrKey
          + " created for tvr " + tvr.getTvrId());
    }
    return set.getOrCreateSubset(cluster, toTraits);
  }

  public Collection<TvrConvertMatchPattern> getTvrConvertMatchPatterns(
      TvrSemantics newTrait) {
    return tvrConvertMatchPatterns.get(newTrait.getClass());
  }

  public RelOptPlanner chooseDelegate() {
    return this;
  }

  /**
   * Finds the most efficient expression to implement the query given via
   * {@link org.apache.calcite.plan.RelOptPlanner#setRoot(org.apache.calcite.rel.RelNode)}.
   *
   * <p>The algorithm executes repeatedly in a series of phases. In each phase
   * the exact rules that may be fired varies. The mapping of phases to rule
   * sets is maintained in the {@link #ruleQueue}.
   *
   * <p>In each phase, the planner sets the initial importance of the existing
   * RelSubSets ({@link #setInitialImportance()}). The planner then iterates
   * over the rule matches presented by the rule queue until:
   *
   * <ol>
   * <li>The rule queue becomes empty.</li>
   * <li>For ambitious planners: No improvements to the plan have been made
   * recently (specifically within a number of iterations that is 10% of the
   * number of iterations necessary to first reach an implementable plan or 25
   * iterations whichever is larger).</li>
   * <li>For non-ambitious planners: When an implementable plan is found.</li>
   * </ol>
   *
   * <p>Furthermore, after every 10 iterations without an implementable plan,
   * RelSubSets that contain only logical RelNodes are given an importance
   * boost via {@link #injectImportanceBoost()}. Once an implementable plan is
   * found, the artificially raised importance values are cleared (see
   * {@link #clearImportanceBoost()}).
   *
   * @return the most efficient RelNode tree found for implementing the given
   * query
   */
  public RelNode findBestExp() {
    ensureRootConverters();
    registerMaterializations();
    int cumulativeTicks = 0;
    for (VolcanoPlannerPhase phase : VolcanoPlannerPhase.values()) {
      prePhase(phase);
      setInitialImportance();

      RelOptCost targetCost = costFactory.makeHugeCost();
      int tick = 0;
      int firstFiniteTick = -1;
      int splitCount = 0;
      int giveUpTick = Integer.MAX_VALUE;

      while (true) {
        ++tick;
        ++cumulativeTicks;
        if (root.bestCost.isLe(targetCost)) {
          if (firstFiniteTick < 0) {
            firstFiniteTick = cumulativeTicks;

            clearImportanceBoost();
          }
          if (ambitious) {
            // Choose a slightly more ambitious target cost, and
            // try again. If it took us 1000 iterations to find our
            // first finite plan, give ourselves another 100
            // iterations to reduce the cost by 10%.
            targetCost = root.bestCost.multiplyBy(0.9);
            ++splitCount;
            if (impatient) {
              if (firstFiniteTick < 10) {
                // It's possible pre-processing can create
                // an implementable plan -- give us some time
                // to actually optimize it.
                giveUpTick = cumulativeTicks + 25;
              } else {
                giveUpTick =
                    cumulativeTicks
                        + Math.max(firstFiniteTick / 10, 25);
              }
            }
          } else {
            break;
          }
        } else if (cumulativeTicks > giveUpTick) {
          // We haven't made progress recently. Take the current best.
          break;
        } else if (root.bestCost.isInfinite() && ((tick % 10) == 0)) {
          injectImportanceBoost();
        }

        LOGGER.debug("PLANNER = {}; TICK = {}/{}; PHASE = {}; COST = {}",
            this, cumulativeTicks, tick, phase.toString(), root.bestCost);

        VolcanoRuleMatch match = ruleQueue.popMatch(phase);
        if (match == null) {
          // RuleQueue is empty, create delayed TvrTrait one at a time
          LOGGER.debug("RuleQueue drained, creating delayed tvr converters");
          if (phasesToDrainTvrConverters.contains(phase)
              && createOneDelayedTvrConverter()) {
            continue; // Continue looping
          } else {
            break;    // Exit when delayCreateTvrTraits is also drained
          }
        }

        assert match.getRule().matches(match);
        match.onMatch();

        // The root may have been merged with another
        // subset. Find the new root subset.
        root = canonize(root);
      }

      ruleQueue.phaseCompleted(phase);
      postPhase(phase);
    }
    if (LOGGER.isTraceEnabled()) {
      StringWriter sw = new StringWriter();
      final PrintWriter pw = new PrintWriter(sw);
      dump(pw);
      pw.flush();
      LOGGER.trace(sw.toString());
    }
    RelNode cheapest = root.buildCheapestPlan(this);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Cheapest plan:\n{}", RelOptUtil.toString(cheapest, SqlExplainLevel.ALL_ATTRIBUTES));

      LOGGER.debug("Provenance:\n{}", provenance(cheapest));
    }
    return cheapest;
  }

  protected void postPhase(VolcanoPlannerPhase phase) {
  }

  protected void prePhase(VolcanoPlannerPhase phase) {
  }

  private boolean createOneDelayedTvrConverter() {
    Iterator<TvrMetaSet> iter = hasDelayedTvrConverters.iterator();
    while (iter.hasNext()) {
      TvrMetaSet tvrMetaSet = iter.next();
      if (tvrMetaSet.delayedTvrConverters.isEmpty()) {
        iter.remove();
        continue;
      }
      if (tvrMetaSet.createDelayedTvrConverter(this)) {
        return true;
      }
    }
    return false;
  }

  public void trackMetaSetWithDelayedTvrConverters(TvrMetaSet tvr) {
    hasDelayedTvrConverters.add(tvr);
  }

  public void untrackMetaSetWithDelayedTvrConverters(TvrMetaSet tvr) {
    hasDelayedTvrConverters.remove(tvr);
  }

  /** Informs {@link JaninoRelMetadataProvider} about the different kinds of
   * {@link RelNode} that we will be dealing with. It will reduce the number
   * of times that we need to re-generate the provider. */
  private void registerMetadataRels() {
    JaninoRelMetadataProvider.DEFAULT.register(classOperands.keySet());
  }

  /** Ensures that the subset that is the root relational expression contains
   * converters to all other subsets in its equivalence set.
   *
   * <p>Thus the planner tries to find cheap implementations of those other
   * subsets, which can then be converted to the root. This is the only place
   * in the plan where explicit converters are required; elsewhere, a consumer
   * will be asking for the result in a particular convention, but the root has
   * no consumers. */
  void ensureRootConverters() {
    final Set<RelSubset> subsets = new HashSet<>();
    for (RelNode rel : root.getRels()) {
      if (rel instanceof AbstractConverter) {
        subsets.add((RelSubset) ((AbstractConverter) rel).getInput());
      }
    }
    for (RelSubset subset : root.set.subsets) {
      final ImmutableList<RelTrait> difference =
          root.getTraitSet().difference(subset.getTraitSet());
      if (difference.size() == 1 && subsets.add(subset)) {
        register(
            new AbstractConverter(subset.getCluster(), subset,
                difference.get(0).getTraitDef(), root.getTraitSet()),
            root);
      }
    }
  }

  /**
   * Returns a multi-line string describing the provenance of a tree of
   * relational expressions. For each node in the tree, prints the rule that
   * created the node, if any. Recursively describes the provenance of the
   * relational expressions that are the arguments to that rule.
   *
   * <p>Thus, every relational expression and rule invocation that affected
   * the final outcome is described in the provenance. This can be useful
   * when finding the root cause of "mistakes" in a query plan.</p>
   *
   * @param root Root relational expression in a tree
   * @return Multi-line string describing the rules that created the tree
   */
  private String provenance(RelNode root) {
    final StringWriter sw = new StringWriter();
    final PrintWriter pw = new PrintWriter(sw);
    final List<RelNode> nodes = new ArrayList<>();
    new RelVisitor() {
      public void visit(RelNode node, int ordinal, RelNode parent) {
        nodes.add(node);
        super.visit(node, ordinal, parent);
      }
      // CHECKSTYLE: IGNORE 1
    }.go(root);
    final Set<RelNode> visited = new HashSet<>();
    for (RelNode node : nodes) {
      provenanceRecurse(pw, node, 0, visited);
    }
    pw.flush();
    return sw.toString();
  }

  /**
   * Helper for {@link #provenance(org.apache.calcite.rel.RelNode)}.
   */
  private void provenanceRecurse(
      PrintWriter pw, RelNode node, int i, Set<RelNode> visited) {
    Spaces.append(pw, i * 2);
    if (!visited.add(node)) {
      pw.println("rel#" + node.getId() + " (see above)");
      return;
    }
    pw.println(node);
    final Provenance o = provenanceMap.get(node);
    Spaces.append(pw, i * 2 + 2);
    if (o == Provenance.EMPTY) {
      pw.println("no parent");
    } else if (o instanceof DirectProvenance) {
      RelNode rel = ((DirectProvenance) o).source;
      pw.println("direct");
      provenanceRecurse(pw, rel, i + 2, visited);
    } else if (o instanceof RuleProvenance) {
      RuleProvenance rule = (RuleProvenance) o;
      pw.println("call#" + rule.callId + " rule [" + rule.rule + "]");
      for (RelNode rel : rule.rels) {
        provenanceRecurse(pw, rel, i + 2, visited);
      }
    } else if (o == null && node instanceof RelSubset) {
      // A few operands recognize subsets, not individual rels.
      // The first rel in the subset is deemed to have created it.
      final RelSubset subset = (RelSubset) node;
      pw.println("subset " + subset);
      provenanceRecurse(pw, subset.getRelList().get(0), i + 2, visited);
    } else {
      throw new AssertionError("bad type " + o);
    }
  }

  private void setInitialImportance() {
    RelVisitor visitor =
        new RelVisitor() {
          int depth = 0;
          final Set<RelSubset> visitedSubsets = new HashSet<>();

          public void visit(
              RelNode p,
              int ordinal,
              RelNode parent) {
            if (p instanceof RelSubset) {
              RelSubset subset = (RelSubset) p;

              if (visitedSubsets.contains(subset)) {
                return;
              }

              if (subset != root) {
                Double importance = Math.pow(0.9, (double) depth);

                ruleQueue.updateImportance(subset, importance);
              }

              visitedSubsets.add(subset);

              depth++;
              for (RelNode rel : subset.getRels()) {
                visit(rel, -1, subset);
              }
              depth--;
            } else {
              super.visit(p, ordinal, parent);
            }
          }
        };

    visitor.go(root);
  }

  /**
   * Finds RelSubsets in the plan that contain only rels of
   * {@link Convention#NONE} and boosts their importance by 25%.
   */
  private void injectImportanceBoost() {
    final Set<RelSubset> requireBoost = new HashSet<>();

  SUBSET_LOOP:
    for (RelSubset subset : ruleQueue.subsetImportances.keySet()) {
      for (RelNode rel : subset.getRels()) {
        if (rel.getConvention() != Convention.NONE) {
          continue SUBSET_LOOP;
        }
      }

      requireBoost.add(subset);
    }

    ruleQueue.boostImportance(requireBoost, 1.25);
  }

  /**
   * Clear all importance boosts.
   */
  private void clearImportanceBoost() {
    Collection<RelSubset> empty = Collections.emptySet();

    ruleQueue.boostImportance(empty, 1.0);
  }

  public RelSubset register(
      RelNode rel,
      RelNode equivRel) {
    assert !isRegistered(rel) : "pre: isRegistered(rel)";
    final RelSet set;
    if (equivRel == null) {
      set = null;
    } else {
      assert RelOptUtil.equal(
          "rel rowtype",
          rel.getRowType(),
          "equivRel rowtype",
          equivRel.getRowType(),
          Litmus.THROW);
      set = getSet(equivRel);
    }
    final RelSubset subset = registerImpl(rel, set);
    return subset;
  }

  public RelSubset ensureRegistered(RelNode rel, RelNode equivRel) {
    RelSubset result;
    final RelSubset subset = getSubset(rel);
    if (subset != null) {
      if (equivRel != null) {
        final RelSubset equivSubset = getSubset(equivRel);
        if (subset.set != equivSubset.set) {
          merge(equivSubset.set, subset.set);
        }
      }
      result = subset;
    } else {
      result = register(rel, equivRel);
    }

    // Checking if tree is valid considerably slows down planning
    // Only doing it if logger level is debug or finer
    if (LOGGER.isDebugEnabled()) {
      assert isValid(Litmus.THROW);
    }

    return result;
  }

  /**
   * Checks internal consistency.
   */
  protected boolean isValid(Litmus litmus) {
    if (this.getRoot() == null) {
      return true;
    }

    RelMetadataQuery metaQuery = this.getRoot().getCluster().getMetadataQuery();
    for (RelSet set : allSets) {
      if (set.equivalentSet != null) {
        throw new AssertionError(
            "set [" + set
            + "] has been merged: it should not be in the list");
      }
      for (RelSubset subset : set.subsets) {
        if (subset.set != set) {
          throw new AssertionError(
              "subset [" + subset.getDescription()
              + "] is in wrong set [" + set + "]");
        }

        if (subset.best != null) {

          // Make sure best RelNode is valid
          if (!subset.set.rels.contains(subset.best)) {
            return litmus.fail("RelSubset [{}] does not contain its best RelNode [{}]",
                    subset.getDescription(), subset.best.getDescription());
          }

          // Make sure bestCost is up-to-date
          try {
            RelOptCost bestCost = getCost(subset.best, metaQuery);
            if (!subset.bestCost.equals(bestCost)) {
              return litmus.fail("RelSubset [" + subset.getDescription()
                      + "] has wrong best cost "
                      + subset.bestCost + ". Correct cost is " + bestCost);
            }
          } catch (CyclicMetadataException e) {
            // ignore
          }
        }

        for (RelNode rel : subset.getRels()) {
          try {
            RelOptCost relCost = getCost(rel, metaQuery);
            if (relCost.isLt(subset.bestCost)) {
              return litmus.fail("rel [{}] has lower cost {} than "
                      + "best cost {} of subset [{}]",
                      rel.getDescription(), relCost, subset.bestCost, subset.getDescription());
            }
          } catch (CyclicMetadataException e) {
            // ignore
          }
        }
      }
    }
    return litmus.succeed();
  }

  public void registerAbstractRelationalRules() {
    addRule(FilterJoinRule.FILTER_ON_JOIN);
    addRule(FilterJoinRule.JOIN);
    addRule(AbstractConverter.ExpandConversionRule.INSTANCE);
    addRule(JoinCommuteRule.INSTANCE);
    addRule(SemiJoinRule.PROJECT);
    addRule(SemiJoinRule.JOIN);
    if (CalcitePrepareImpl.COMMUTE) {
      addRule(JoinAssociateRule.INSTANCE);
    }
    addRule(AggregateRemoveRule.INSTANCE);
    addRule(UnionToDistinctRule.INSTANCE);
    addRule(ProjectRemoveRule.INSTANCE);
    addRule(AggregateJoinTransposeRule.INSTANCE);
    addRule(AggregateProjectMergeRule.INSTANCE);
    addRule(CalcRemoveRule.INSTANCE);
    addRule(SortRemoveRule.INSTANCE);

    // todo: rule which makes Project({OrdinalRef}) disappear
  }

  public void registerSchema(RelOptSchema schema) {
    if (registeredSchemas.add(schema)) {
      try {
        schema.registerRules(this);
      } catch (Exception e) {
        throw new AssertionError("While registering schema " + schema, e);
      }
    }
  }

  public RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
    assert rel != null : "pre-condition: rel != null";
    if (rel instanceof RelSubset) {
      return ((RelSubset) rel).bestCost;
    }
    if (rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE)
        == Convention.NONE) {
      return costFactory.makeInfiniteCost();
    }
    RelOptCost cost = mq.getNonCumulativeCost(rel);
    if (!zeroCost.isLt(cost)) {
      // cost must be positive, so nudge it
      cost = costFactory.makeTinyCost();
    }
    for (RelNode input : rel.getInputs()) {
      cost = cost.plus(getCost(input, mq));
    }
    return cost;
  }

  /**
   * Returns the subset that a relational expression belongs to.
   *
   * @param rel Relational expression
   * @return Subset it belongs to, or null if it is not registered
   */
  public RelSubset getSubset(RelNode rel) {
    assert rel != null : "pre: rel != null";
    if (rel instanceof RelSubset) {
      return canonize((RelSubset) rel);
    } else {
      RelSubset s = mapRel2Subset.get(rel);
      return s == null ? null : canonize(s);
    }
  }

  public RelSubset getSubset(
      RelNode rel,
      RelTraitSet traits) {
    return getSubset(rel, traits, false);
  }

  public RelSubset getSubset(
      RelNode rel,
      RelTraitSet traits,
      boolean createIfMissing) {
    if ((rel instanceof RelSubset) && (rel.getTraitSet().equals(traits))) {
      return (RelSubset) rel;
    }
    RelSet set = getSet(rel);
    if (set == null) {
      return null;
    }
    if (createIfMissing) {
      return set.getOrCreateSubset(rel.getCluster(), traits);
    }
    return set.getSubset(traits);
  }

  public List<RelNode> getRelList() {
    return mapDigestToRel.values().stream().filter(rel -> {
      Double importance = relImportances.get(rel);
      return importance == null || importance != 0d;
    }).collect(Collectors.toList());
  }

  private RelNode changeTraitsUsingConverters(
      RelNode rel,
      RelTraitSet toTraits,
      boolean allowAbstractConverters) {
    final RelTraitSet fromTraits = rel.getTraitSet();

    assert fromTraits.size() >= toTraits.size();

    final boolean allowInfiniteCostConverters =
        SaffronProperties.INSTANCE.allowInfiniteCostConverters().get();

    // Traits may build on top of another...for example a collation trait
    // would typically come after a distribution trait since distribution
    // destroys collation; so when doing the conversion below we use
    // fromTraits as the trait of the just previously converted RelNode.
    // Also, toTraits may have fewer traits than fromTraits, excess traits
    // will be left as is.  Finally, any null entries in toTraits are
    // ignored.
    RelNode converted = rel;
    for (int i = 0; (converted != null) && (i < toTraits.size()); i++) {
      RelTrait fromTrait = converted.getTraitSet().getTrait(i);
      final RelTraitDef traitDef = fromTrait.getTraitDef();
      RelTrait toTrait = toTraits.getTrait(i);

      if (toTrait == null) {
        continue;
      }

      assert traitDef == toTrait.getTraitDef();
//            if (fromTrait.subsumes(toTrait)) {
      if (fromTrait.equals(toTrait)) {
        // No need to convert; it's already correct.
        continue;
      }

      rel =
          traitDef.convert(
              this,
              converted,
              toTrait,
              allowInfiniteCostConverters);
      if (rel != null) {
        assert rel.getTraitSet().getTrait(traitDef).satisfies(toTrait);
        rel =
            completeConversion(
                rel, allowInfiniteCostConverters, toTraits,
                Expressions.list(traitDef));
        if (rel != null) {
          register(rel, converted);
        }
      }

      if ((rel == null) && allowAbstractConverters) {
        RelTraitSet stepTraits =
            converted.getTraitSet().replace(toTrait);

        rel = getSubset(converted, stepTraits);
      }

      converted = rel;
    }

    // make sure final converted traitset subsumes what was required
    if (converted != null) {
      assert converted.getTraitSet().satisfies(toTraits);
    }

    return converted;
  }

  /**
   * Converts traits using well-founded induction. We don't require that
   * each conversion preserves all traits that have previously been converted,
   * but if it changes "locked in" traits we'll try some other conversion.
   *
   * @param rel                         Relational expression
   * @param allowInfiniteCostConverters Whether to allow infinite converters
   * @param toTraits                    Target trait set
   * @param usedTraits                  Traits that have been locked in
   * @return Converted relational expression
   */
  private RelNode completeConversion(
      RelNode rel,
      boolean allowInfiniteCostConverters,
      RelTraitSet toTraits,
      Expressions.FluentList<RelTraitDef> usedTraits) {
    if (true) {
      return rel;
    }
    for (RelTrait trait : rel.getTraitSet()) {
      if (toTraits.contains(trait)) {
        // We're already a match on this trait type.
        continue;
      }
      final RelTraitDef traitDef = trait.getTraitDef();
      RelNode rel2 =
          traitDef.convert(
              this,
              rel,
              toTraits.getTrait(traitDef),
              allowInfiniteCostConverters);

      // if any of the used traits have been knocked out, we could be
      // heading for a cycle.
      for (RelTraitDef usedTrait : usedTraits) {
        if (!rel2.getTraitSet().contains(usedTrait)) {
          continue;
        }
      }
      // recursive call, to convert one more trait
      rel =
          completeConversion(
              rel2,
              allowInfiniteCostConverters,
              toTraits,
              usedTraits.append(traitDef));
      if (rel != null) {
        return rel;
      }
    }
    assert rel.getTraitSet().equals(toTraits);
    return rel;
  }

  RelNode changeTraitsUsingConverters(
      RelNode rel,
      RelTraitSet toTraits) {
    return changeTraitsUsingConverters(rel, toTraits, false);
  }

  void checkForSatisfiedConverters(
      RelSet set,
      RelNode rel) {
    int i = 0;
    while (i < set.abstractConverters.size()) {
      AbstractConverter converter = set.abstractConverters.get(i);
      RelNode converted =
          changeTraitsUsingConverters(
              rel,
              converter.getTraitSet());
      if (converted == null) {
        i++; // couldn't convert this; move on to the next
      } else {
        if (!isRegistered(converted)) {
          registerImpl(converted, set);
        }
        set.abstractConverters.remove(converter); // success
      }
    }
  }

  public void setImportance(RelNode rel, double importance) {
    assert rel != null;
    assert importance >= 0d;
    relImportances.put(rel, importance);
  }

  public Set<TvrMetaSetType> getGenericRelTvrTypes(RelNode rel) {
    return tvrGenericRels.get(rel);
  }

  /**
   * Dumps the internal state of this VolcanoPlanner to a writer.
   *
   * @param pw Print writer
   * @see #normalizePlan(String)
   */
  public void dump(PrintWriter pw) {
    pw.println("Root: " + root.getDescription());
    pw.println("Original rel:");
    pw.println(originalRootString);
    pw.println("Sets:");
    Ordering<RelSet> ordering = Ordering.from((o1, o2) -> o1.id - o2.id);
    for (RelSet set : ordering.immutableSortedCopy(allSets)) {
      pw.println(
          "Set#" + set.id + ", type: " + set.subsets.get(0).getRowType());
      pw.println("\t" + set.tvrLinks.asMap().entrySet().stream().
          map(e -> e.getKey() + "-[" + e.getValue().stream()
              .map(metaSet -> Integer.toString(metaSet.tvrId))
              .collect(joining(",")) + "]").collect(joining(", ")));

      int j = -1;
      for (RelSubset subset : set.subsets) {
        ++j;
        pw.println(
            "\t" + subset.getDescription() + ", best="
            + ((subset.best == null) ? "null"
                : ("rel#" + subset.best.getId())) + ", importance="
                + ruleQueue.getImportance(subset));
        assert subset.set == set;
        for (int k = 0; k < j; k++) {
          assert !set.subsets.get(k).getTraitSet().equals(
              subset.getTraitSet());
        }
        for (RelNode rel : subset.getRels()) {
          // "\t\trel#34:JavaProject(rel#32:JavaFilter(...), ...)"
          pw.print("\t\t" + rel.getDescription());
          for (RelNode input : rel.getInputs()) {
            RelSubset inputSubset =
                getSubset(
                    input,
                    input.getTraitSet());
            RelSet inputSet = inputSubset.set;
            if (input instanceof RelSubset) {
              final Iterator<RelNode> rels =
                  inputSubset.getRels().iterator();
              if (rels.hasNext()) {
                input = rels.next();
                assert input.getTraitSet().satisfies(inputSubset.getTraitSet());
                assert inputSet.rels.contains(input);
                assert inputSet.subsets.contains(inputSubset);
              }
            }
          }
          Double importance = relImportances.get(rel);
          if (importance != null) {
            pw.print(", importance=" + importance);
          }
          RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
          pw.print(", rowcount=" + mq.getRowCount(rel));
          pw.println(", cumulative cost=" + getCost(rel, mq));
        }
      }
    }
    pw.println();
  }

  /** Computes the key for {@link #mapDigestToRel}. */
  private static Pair<String, RelDataType> key(RelNode rel) {
    return Pair.of(rel.getDigest(), rel.getRowType());
  }

  /**
   * Re-computes the digest of a {@link RelNode}.
   *
   * <p>Since a relational expression's digest contains the identifiers of its
   * children, this method needs to be called when the child has been renamed,
   * for example if the child's set merges with another.
   *
   * @param rel Relational expression
   */
  void rename(RelNode rel) {
    final String oldDigest = rel.getDigest();
    if (fixUpInputs(rel)) {
      final Pair<String, RelDataType> oldKey =
          Pair.of(oldDigest, rel.getRowType());
      final RelNode removed = mapDigestToRel.remove(oldKey);
      assert removed == rel;
      final String newDigest = rel.recomputeDigest();
      LOGGER.trace("Rename #{} from '{}' to '{}'", rel.getId(), oldDigest, newDigest);
      final Pair<String, RelDataType> key = key(rel);
      final RelNode equivRel = mapDigestToRel.put(key, rel);
      if (equivRel != null) {
        assert equivRel != rel;

        // There's already an equivalent with the same name, and we
        // just knocked it out. Put it back, and forget about 'rel'.
        LOGGER.trace("After renaming rel#{} it is now equivalent to rel#{}",
            rel.getId(), equivRel.getId());
        mapDigestToRel.put(key, equivRel);

        RelSubset equivRelSubset = getSubset(equivRel);
        ruleQueue.recompute(equivRelSubset, true);

        // Remove back-links from children.
        for (RelNode input : rel.getInputs()) {
          ((RelSubset) input).set.parents.remove(rel);
        }

        // Remove rel from its subset. (This may leave the subset
        // empty, but if so, that will be dealt with when the sets
        // get merged.)
        final RelSubset subset = mapRel2Subset.put(rel, equivRelSubset);
        assert subset != null;
        boolean existed = subset.set.rels.remove(rel);
        assert existed : "rel was not known to its set";
        final RelSubset equivSubset = getSubset(equivRel);
        for (RelSubset s : subset.set.subsets) {
          if (s.best == rel) {
            Set<RelSubset> activeSet = new HashSet<>();
            s.best = equivRel;

            // Propagate cost improvement since this potentially would change the subset's best cost
            s.propagateCostImprovements(
                    this, equivRel.getCluster().getMetadataQuery(),
                    equivRel, activeSet);
          }
        }

        if (equivSubset != subset) {
          // The equivalent relational expression is in a different
          // subset, therefore the sets are equivalent.
          assert equivSubset.getTraitSet().equals(
              subset.getTraitSet());
          assert equivSubset.set != subset.set;
          merge(equivSubset.set, subset.set);
        }
      }
    }
  }

  /**
   * Registers a {@link RelNode}, which has already been registered, in a new
   * {@link RelSet}.
   *
   * @param set Set
   * @param rel Relational expression
   */
  void reregister(
      RelSet set,
      RelNode rel) {
    // Is there an equivalent relational expression? (This might have
    // just occurred because the relational expression's child was just
    // found to be equivalent to another set.)
    final Pair<String, RelDataType> key = key(rel);
    RelNode equivRel = mapDigestToRel.get(key);
    if (equivRel != null && equivRel != rel) {
      assert equivRel.getClass() == rel.getClass();
      assert equivRel.getTraitSet().equals(rel.getTraitSet());

      RelSubset equivRelSubset = getSubset(equivRel);
      ruleQueue.recompute(equivRelSubset, true);
      return;
    }

    // Add the relational expression into the correct set and subset.
    RelSubset subset2 = addRelToSet(rel, set);
  }

  /**
   * Update relNode related information when two identical relNodes are
   * de-duped.
   */
  void mergeIdenticalRels(RelNode to, RelNode from) {
    // Merge the generic map entries
    if (tvrGenericRels.containsKey(from)) {
      tvrGenericRels.putAll(to, tvrGenericRels.get(from));
    }

    RelSubset equivRelSubset = getSubset(to);
    ruleQueue.recompute(equivRelSubset, true);

    // update the importance of the surviving relNode to the merged
    // importance, which is the smaller value of the two.
    if (isSelfLoop(to)) {
      setImportance(to, 0d);
      return;
    }
    Double fromImportance = relImportances.get(from);
    Double toImportance = relImportances.get(to);
    if (fromImportance == null) {
      if (toImportance == null) {
        return; // both using default importance
      }
      if (toImportance > DEFAULT_REL_IMPORTANCE) {
        relImportances.remove(to);  // merge to default
      }
      return;
    }
    if (toImportance == null) {
      if (fromImportance < DEFAULT_REL_IMPORTANCE) {
        relImportances.put(to, fromImportance); // use smaller value
      }
      return;
    }
    // use the smaller value
    relImportances.put(to,
        (fromImportance < toImportance) ? fromImportance : toImportance);
  }

  /**
   * Whether the rel is one of its own inputs, if true, this relNode is
   * useless.
   */
  boolean isSelfLoop(RelNode rel) {
    RelSubset subset = getSubset(rel);
    return rel.getInputs().contains(subset);
  }

  /**
   * If a subset has one or more equivalent subsets (owing to a set having
   * merged with another), returns the subset which is the leader of the
   * equivalence class.
   *
   * @param subset Subset
   * @return Leader of subset's equivalence class
   */
  private RelSubset canonize(final RelSubset subset) {
    if (subset.set.equivalentSet == null) {
      return subset;
    }
    RelSet set = subset.set;
    do {
      set = set.equivalentSet;
    } while (set.equivalentSet != null);
    return set.getOrCreateSubset(
        subset.getCluster(), subset.getTraitSet());
  }

  /**
   * Fires all rules matched by a relational expression.
   *
   * @param rel      Relational expression which has just been created (or maybe
   *                 from the queue)
   * @param deferred If true, each time a rule matches, just add an entry to
   *                 the queue.
   */
  protected void fireRules(
      RelNode rel,
      boolean deferred) {
    for (RelOptRuleOperand operand : classOperands.get(rel.getClass())) {
      if (!ruleQueue.active(operand.getRule())) {
        continue;
      }
      if (rel instanceof RelSubset
          && operand.getMatchedClass() != RelSubset.class) {
        // Operand has to be look for exactly RelSubset.class to match a subset
        continue;
      }
      if (operand.matches(rel)) {
        final VolcanoRuleCall ruleCall;
        if (deferred) {
          ruleCall = new DeferringRuleCall(this, operand);
        } else {
          ruleCall = new VolcanoRuleCall(this, operand);
        }
        ruleCall.match(rel);
      }
    }
  }

  void fireRules(TvrMetaSet tvr) {
    for (TvrRelOptRuleOperand tvrOp : tvrOperands) {
      if (!ruleQueue.active(tvrOp.getRule())) {
        continue;
      }
      if (tvrOp.matches(tvr)) {
        final VolcanoRuleCall ruleCall;
        ruleCall = new DeferringRuleCall(this, tvrOp);
        ruleCall.match(tvr);
      }
    }
  }

  void fireRules(TvrMetaSet tvr, TvrSemantics tvrKey, RelSet set,
      boolean deferred) {
    for (TvrEdgeRelOptRuleOperand edgeOp : tvrEdgeToOperands
        .get(tvrKey.getClass())) {
      if (!ruleQueue.active(edgeOp.getRule())) {
        continue;
      }
      if (edgeOp.matches(tvrKey)) {
        final VolcanoRuleCall ruleCall;
        if (deferred) {
          ruleCall = new DeferringRuleCall(this, edgeOp);
        } else {
          ruleCall = new VolcanoRuleCall(this, edgeOp);
        }
        ruleCall.match(tvr, tvrKey, set);
      }
    }
  }

  void fireRules(TvrMetaSet fromTvr, TvrMetaSet toTvr, TvrProperty tvrProperty,
      boolean deferred) {
    for (TvrPropertyEdgeRuleOperand propertyOp : tvrPropertyEdgeToOperands
        .get(tvrProperty.getClass())) {
      if (!ruleQueue.active(propertyOp.getRule())) {
        continue;
      }
      if (propertyOp.matches(tvrProperty)) {
        final VolcanoRuleCall ruleCall;
        if (deferred) {
          ruleCall = new DeferringRuleCall(this, propertyOp);
        } else {
          ruleCall = new VolcanoRuleCall(this, propertyOp);
        }
        ruleCall.match(fromTvr, toTvr, tvrProperty);
      }
    }
  }

  private boolean fixUpInputs(RelNode rel) {
    List<RelNode> inputs = rel.getInputs();
    int i = -1;
    int changeCount = 0;
    for (RelNode input : inputs) {
      ++i;
      if (input instanceof RelSubset) {
        final RelSubset subset = (RelSubset) input;
        RelSubset newSubset = canonize(subset);
        if (newSubset != subset) {
          rel.replaceInput(i, newSubset);
          if (subset.set != newSubset.set) {
            subset.set.parents.remove(rel);
            newSubset.set.parents.add(rel);
          }
          changeCount++;
        }
      }
    }
    return changeCount > 0;
  }

  RelSet merge(RelSet set, RelSet set2) {
    assert set != set2 : "pre: set != set2";

    // Find the root of set2's equivalence tree.
    set = equivRoot(set);
    set2 = equivRoot(set2);

    // Looks like set2 was already marked as equivalent to set. Nothing
    // to do.
    if (set2 == set) {
      return set;
    }

    // If necessary, swap the sets, so we're always merging the newer set
    // into the older.
    if (set.id > set2.id) {
      RelSet t = set;
      set = set2;
      set2 = t;
    }

    // Merge.
    set.mergeWith(this, set2);

    // Was the set we merged with the root? If so, the result is the new
    // root.
    if (set2 == getSet(root)) {
      root =
          set.getOrCreateSubset(
              root.getCluster(),
              root.getTraitSet());
      ensureRootConverters();
    }

    // find the relnode self loop after set merge and set its importance to be 0
    for (RelNode rel : set.rels) {
      if (isSelfLoop(rel)) {
        setImportance(rel, 0d);
      }
    }

    return set;
  }

  public static RelSet equivRoot(RelSet s) {
    RelSet p = s; // iterates at twice the rate, to detect cycles
    while (s.equivalentSet != null) {
      p = forward2(s, p);
      s = s.equivalentSet;
    }
    return s;
  }

  /** Moves forward two links, checking for a cycle at each. */
  private static RelSet forward2(RelSet s, RelSet p) {
    p = forward1(s, p);
    p = forward1(s, p);
    return p;
  }

  /** Moves forward one link, checking for a cycle. */
  private static RelSet forward1(RelSet s, RelSet p) {
    if (p != null) {
      p = p.equivalentSet;
      if (p == s) {
        throw new AssertionError("cycle in equivalence tree");
      }
    }
    return p;
  }

  /**
   * Registers a new expression <code>exp</code> and queues up rule matches.
   * If <code>set</code> is not null, makes the expression part of that
   * equivalence set. If an identical expression is already registered, we
   * don't need to register this one and nor should we queue up rule matches.
   *
   * @param rel relational expression to register. Must be either a
   *         {@link RelSubset}, or an unregistered {@link RelNode}
   * @param set set that rel belongs to, or <code>null</code>
   * @return the equivalence-set
   */
  RelSubset registerImpl(RelNode rel, RelSet set) {
    if (rel instanceof RelSubset) {
      return registerSubset(set, (RelSubset) rel);
    }

    assert !isRegistered(rel) : "already been registered: " + rel;
    if (rel.getCluster().getPlanner() != this) {
      throw new AssertionError("Relational expression " + rel
          + " belongs to a different planner than is currently being used.");
    }

    // Now is a good time to ensure that the relational expression
    // implements the interface required by its calling convention.
    final RelTraitSet traits = rel.getTraitSet();
    final Convention convention = traits.getTrait(ConventionTraitDef.INSTANCE);
    assert convention != null;
    if (!convention.getInterface().isInstance(rel)
        && !(rel instanceof Converter)) {
      throw new AssertionError("Relational expression " + rel
          + " has calling-convention " + convention
          + " but does not implement the required interface '"
          + convention.getInterface() + "' of that convention");
    }
    if (traits.size() != traitDefs.size()) {
      throw new AssertionError("Relational expression " + rel
          + " does not have the correct number of traits: " + traits.size()
          + " != " + traitDefs.size());
    }

    // Ensure that its sub-expressions are registered.
    rel = rel.onRegister(this);

    // Record its provenance. (Rule call may be null.)
    if (ruleCallStack.isEmpty()) {
      provenanceMap.put(rel, Provenance.EMPTY);
    } else {
      final VolcanoRuleCall ruleCall = ruleCallStack.peek();
      provenanceMap.put(
          rel,
          new RuleProvenance(
              ruleCall.rule,
              ImmutableList.copyOf(ruleCall.rels),
              ruleCall.id));
    }

    // If it is equivalent to an existing expression, return the set that
    // the equivalent expression belongs to.
    Pair<String, RelDataType> key = key(rel);
    RelNode equivExp = mapDigestToRel.get(key);
    if (equivExp == null) {
      // do nothing
    } else if (equivExp == rel) {
      return getSubset(rel);
    } else {
      assert RelOptUtil.equal(
          "left", equivExp.getRowType(),
          "right", rel.getRowType(),
          Litmus.THROW);
      RelSet equivSet = getSet(equivExp);
      if (equivSet != null) {
        LOGGER.trace(
            "Register: rel#{} is equivalent to {}", rel.getId(), equivExp.getDescription());
        return registerSubset(set, getSubset(equivExp));
      }
    }

    // Converters are in the same set as their children.
    if (rel instanceof Converter) {
      final RelNode input = ((Converter) rel).getInput();
      final RelSet childSet = getSet(input);
      if ((set != null)
          && (set != childSet)
          && (set.equivalentSet == null)) {
        LOGGER.trace(
            "Register #{} {} (and merge sets, because it is a conversion)",
            rel.getId(), rel.getDigest());
        merge(set, childSet);
        registerCount++;

        // During the mergers, the child set may have changed, and since
        // we're not registered yet, we won't have been informed. So
        // check whether we are now equivalent to an existing
        // expression.
        if (fixUpInputs(rel)) {
          rel.recomputeDigest();
          key = key(rel);
          RelNode equivRel = mapDigestToRel.get(key);
          if ((equivRel != rel) && (equivRel != null)) {
            // make sure this bad rel didn't get into the
            // set in any way (fixupInputs will do this but it
            // doesn't know if it should so it does it anyway)
            set.obliterateRelNode(rel);

            // There is already an equivalent expression. Use that
            // one, and forget about this one.
            return getSubset(equivRel);
          }
        }
      } else {
        set = childSet;
      }
    }

    // Place the expression in the appropriate equivalence set.
    if (set == null) {
      set = new RelSet(nextSetId++,
          Util.minus(RelOptUtil.getVariablesSet(rel), rel.getVariablesSet()),
          RelOptUtil.getVariablesUsed(rel), rel.getCluster(), rel.getRowType());
      this.allSets.add(set);
    }

    // Chain to find 'live' equivalent set, just in case several sets are
    // merging at the same time.
    while (set.equivalentSet != null) {
      set = set.equivalentSet;
    }

    // Allow each rel to register its own rules.
    registerClass(rel);

    registerCount++;
    RelSubset subset = addRelToSet(rel, set);

    final RelNode xx = mapDigestToRel.put(key, rel);
    assert xx == null || xx == rel : rel.getDigest();

    LOGGER.trace("Register {} in {}", rel.getDescription(), subset.getDescription());

    // This relational expression may have been registered while we
    // recursively registered its children. If this is the case, we're done.
    if (xx != null) {
      return subset;
    }

    // Create back-links from its children, which makes children more
    // important.
    if (rel == this.root) {
      ruleQueue.subsetImportances.put(
          subset,
          1.0); // todo: remove
    }
    for (RelNode input : rel.getInputs()) {
      RelSubset childSubset = (RelSubset) input;
      childSubset.set.parents.add(rel);

      // Child subset is more important now a new parent uses it.
      ruleQueue.recompute(childSubset);
    }
    if (rel == this.root) {
      ruleQueue.subsetImportances.remove(subset);
    }

    // Remember abstract converters until they're satisfied
    if (rel instanceof AbstractConverter) {
      set.abstractConverters.add((AbstractConverter) rel);
    }

    // If this set has any unsatisfied converters, try to satisfy them.
    checkForSatisfiedConverters(set, rel);

    // Make sure this rel's subset importance is updated
    ruleQueue.recompute(subset, true);

    // Queue up all rules triggered by this relexp's creation.
    fireRules(rel, true);

    return subset;
  }

  private RelSubset addRelToSet(RelNode rel, RelSet set) {
    RelSubset subset = set.add(rel);
    mapRel2Subset.put(rel, subset);

    // While a tree of RelNodes is being registered, sometimes nodes' costs
    // improve and the subset doesn't hear about it. You can end up with
    // a subset with a single rel of cost 99 which thinks its best cost is
    // 100. We think this happens because the back-links to parents are
    // not established. So, give the subset another change to figure out
    // its cost.
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    try {
      subset.propagateCostImprovements(this, mq, rel, new HashSet<>());
    } catch (CyclicMetadataException e) {
      // ignore
    }

    return subset;
  }

  private RelSubset registerSubset(
      RelSet set,
      RelSubset subset) {
    if ((set != subset.set)
        && (set != null)
        && (set.equivalentSet == null)) {
      LOGGER.trace("Register #{} {}, and merge sets", subset.getId(), subset);
      merge(set, subset.set);
      registerCount++;
    }
    return subset;
  }

  // implement RelOptPlanner
  public void registerMetadataProviders(List<RelMetadataProvider> list) {
    list.add(0, new VolcanoRelMetadataProvider());
  }

  // implement RelOptPlanner
  public long getRelMetadataTimestamp(RelNode rel) {
    RelSubset subset = getSubset(rel);
    if (subset == null) {
      return 0;
    } else {
      return subset.timestamp;
    }
  }

  /**
   * Normalizes references to subsets within the string representation of a
   * plan.
   *
   * <p>This is useful when writing tests: it helps to ensure that tests don't
   * break when an extra rule is introduced that generates a new subset and
   * causes subsequent subset numbers to be off by one.
   *
   * <p>For example,
   *
   * <blockquote>
   * FennelAggRel.FENNEL_EXEC(child=Subset#17.FENNEL_EXEC,groupCount=1,
   * EXPR$1=COUNT())<br>
   * &nbsp;&nbsp;FennelSortRel.FENNEL_EXEC(child=Subset#2.FENNEL_EXEC,
   * key=[0], discardDuplicates=false)<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;FennelCalcRel.FENNEL_EXEC(
   * child=Subset#4.FENNEL_EXEC, expr#0..8={inputs}, expr#9=3456,
   * DEPTNO=$t7, $f0=$t9)<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MockTableImplRel.FENNEL_EXEC(
   * table=[CATALOG, SALES, EMP])</blockquote>
   *
   * <p>becomes
   *
   * <blockquote>
   * FennelAggRel.FENNEL_EXEC(child=Subset#{0}.FENNEL_EXEC, groupCount=1,
   * EXPR$1=COUNT())<br>
   * &nbsp;&nbsp;FennelSortRel.FENNEL_EXEC(child=Subset#{1}.FENNEL_EXEC,
   * key=[0], discardDuplicates=false)<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;FennelCalcRel.FENNEL_EXEC(
   * child=Subset#{2}.FENNEL_EXEC,expr#0..8={inputs},expr#9=3456,DEPTNO=$t7,
   * $f0=$t9)<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MockTableImplRel.FENNEL_EXEC(
   * table=[CATALOG, SALES, EMP])</blockquote>
   *
   * @param plan Plan
   * @return Normalized plan
   */
  public static String normalizePlan(String plan) {
    if (plan == null) {
      return null;
    }
    final Pattern poundDigits = Pattern.compile("Subset#[0-9]+\\.");
    int i = 0;
    while (true) {
      final Matcher matcher = poundDigits.matcher(plan);
      if (!matcher.find()) {
        return plan;
      }
      final String token = matcher.group(); // e.g. "Subset#23."
      plan = plan.replace(token, "Subset#{" + i++ + "}.");
    }
  }

  /**
   * Sets whether this planner is locked. A locked planner does not accept
   * new rules. {@link #addRule(org.apache.calcite.plan.RelOptRule)} will do
   * nothing and return false.
   *
   * @param locked Whether planner is locked
   */
  public void setLocked(boolean locked) {
    this.locked = locked;
  }

  public RelNode ensureRegistered(RelNode rel, RelNode equivRel,
      VolcanoRuleCall ruleCall) {
    ruleCallStack.push(ruleCall);
    RelSubset ret = ensureRegistered(rel, equivRel);
    ruleCallStack.pop();
    return ret;
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * A rule call which defers its actions. Whereas {@link RelOptRuleCall}
   * invokes the rule when it finds a match, a <code>DeferringRuleCall</code>
   * creates a {@link VolcanoRuleMatch} which can be invoked later.
   */
  private static class DeferringRuleCall extends VolcanoRuleCall {
    DeferringRuleCall(
        VolcanoPlanner planner,
        RelOptRuleOperand operand) {
      super(planner, operand);
    }

    /**
     * Rather than invoking the rule (as the base method does), creates a
     * {@link VolcanoRuleMatch} which can be invoked later.
     */
    protected void onMatch() {
      final VolcanoRuleMatch match =
          new VolcanoRuleMatch(
              volcanoPlanner,
              getOperand0(),
              rels,
              tvrs,
              tvrTraits,
              tvrProperties,
              nodeInputs);
      volcanoPlanner.ruleQueue.addMatch(match);
    }
  }

  /**
   * Where a RelNode came from.
   */
  private abstract static class Provenance {
    public static final Provenance EMPTY = new UnknownProvenance();
  }

  /**
   * We do not know where this RelNode came from. Probably created by hand,
   * or by sql-to-rel converter.
   */
  private static class UnknownProvenance extends Provenance {
  }

  /**
   * A RelNode that came directly from another RelNode via a copy.
   */
  static class DirectProvenance extends Provenance {
    final RelNode source;

    DirectProvenance(RelNode source) {
      this.source = source;
    }
  }

  /**
   * A RelNode that came via the firing of a rule.
   */
  static class RuleProvenance extends Provenance {
    final RelOptRule rule;
    final ImmutableList<RelNode> rels;
    final int callId;

    RuleProvenance(RelOptRule rule, ImmutableList<RelNode> rels, int callId) {
      this.rule = rule;
      this.rels = rels;
      this.callId = callId;
    }
  }
}

// End VolcanoPlanner.java
