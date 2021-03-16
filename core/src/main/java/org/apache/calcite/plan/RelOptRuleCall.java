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
package org.apache.calcite.plan;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrMetaSetType;
import org.apache.calcite.plan.volcano.TvrProperty;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A <code>RelOptRuleCall</code> is an invocation of a {@link RelOptRule} with a
 * set of {@link RelNode relational expression}s as arguments.
 */
public abstract class RelOptRuleCall {
  //~ Static fields/initializers ---------------------------------------------

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /**
   * Generator for {@link #id} values.
   */
  private static int nextId = 0;

  //~ Instance fields --------------------------------------------------------

  public final int id;
  protected final RelOptRuleOperand operand0;
  protected Map<RelNode, List<RelNode>> nodeInputs;
  public final RelOptRule rule;

  /**
   * Matched RelNodes. Size of rule.getMatchingRelCount(), same order as the
   * given operand roots, pre-order within each operand root.
   */
  public final RelNode[] rels;

  /**
   * Matched TvrMetaSets. Size of rule.getMatchingTvrCount(), same order as the
   * given tvr operands.
   */
  public TvrMetaSet[] tvrs;

  /**
   * Matched Tvr links between RelNode and TvrMetaSet. Size of
   * rule.getMatchingTvrEdgeCount(), same order as the given tvr operands,
   * expanded by the order in which TvrRelOptRuleOperand.addTvrConnection() is
   * called.
   */
  public TvrSemantics[] tvrTraits;

  /**
   * Matched Tvr Property links between TvrMetaSets. Size of
   * rule.getMatchingTvrPropertyEdgeCount(), same order as the given tvr
   * operands, expanded by the order in which TvrRelOptRuleOperand
   * .addTvrPropertyEdge() is called.
   */
  public TvrProperty[] tvrProperties;

  private final RelOptPlanner planner;
  private final List<RelNode> parents;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a RelOptRuleCall.
   *
   * @param planner      Planner
   * @param operand      Root operand
   * @param rels         Array of relational expressions which matched each
   *                     operand
   * @param nodeInputs   For each node which matched with
   *                     {@code matchAnyChildren}=true, a list of the node's
   *                     inputs
   * @param parents      list of parent RelNodes corresponding to the first
   *                     relational expression in the array argument, if known;
   *                     otherwise, null
   */
  protected RelOptRuleCall(
      RelOptPlanner planner,
      RelOptRuleOperand operand,
      RelNode[] rels,
      TvrMetaSet[] tvrs,
      TvrSemantics[] tvrTraits,
      TvrProperty[] tvrProperties,
      Map<RelNode, List<RelNode>> nodeInputs,
      List<RelNode> parents) {
    this.id = nextId++;
    this.planner = planner;
    this.operand0 = operand;
    this.nodeInputs = nodeInputs;
    this.rule = operand.getRule();
    this.rels = rels;
    this.tvrs = tvrs;
    this.tvrTraits = tvrTraits;
    this.tvrProperties = tvrProperties;
    this.parents = parents;
    assert rels.length + tvrs.length + tvrTraits.length + tvrProperties.length
        == rule.operands.size();
  }

  protected RelOptRuleCall(
      RelOptPlanner planner,
      RelOptRuleOperand operand,
      RelNode[] rels,
      TvrMetaSet[] tvrs,
      TvrSemantics[] tvrTraits,
      TvrProperty[] tvrProperties,
      Map<RelNode, List<RelNode>> nodeInputs) {
    this(planner, operand, rels, tvrs, tvrTraits, tvrProperties, nodeInputs,
        null);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the root operand matched by this rule.
   *
   * @return root operand
   */
  public RelOptRuleOperand getOperand0() {
    return operand0;
  }

  /**
   * Returns the invoked planner rule.
   *
   * @return planner rule
   */
  public RelOptRule getRule() {
    return rule;
  }

  /**
   * Returns a list of matched relational expressions.
   *
   * @return matched relational expressions
   * @deprecated Use {@link #getRelList()} or {@link #rel(int)}
   */
  @Deprecated // to be removed before 2.0
  public RelNode[] getRels() {
    return rels;
  }

  /**
   * Returns a list of matched relational expressions.
   *
   * @return matched relational expressions
   * @see #rel(int)
   */
  public List<RelNode> getRelList() {
    return ImmutableList.copyOf(rels);
  }

  /**
   * Retrieves the {@code ordinal}th matched relational expression. This
   * corresponds to the {@code ordinal}th operand of the rule.
   *
   * @param ordinal Ordinal
   * @param <T>     Type
   * @return Relational expression
   */
  public <T extends RelNode> T rel(int ordinal) {
    //noinspection unchecked
    return (T) rels[ordinal];
  }

  /**
   * Returns the children of a given relational expression node matched in a
   * rule.
   *
   * <p>If the policy of the operand which caused the match is not
   * {@link org.apache.calcite.plan.RelOptRuleOperandChildPolicy#UNORDERED},
   * the children will have their
   * own operands and therefore be easily available in the array returned by
   * the {@link #getRelList()} method, so this method returns null.
   *
   * <p>This method is for
   * {@link org.apache.calcite.plan.RelOptRuleOperandChildPolicy#UNORDERED},
   * which is generally used when a node can have a variable number of
   * children, and hence where the matched children are not retrievable by any
   * other means.
   *
   * <p>Warning: it produces wrong result for {@code unordered(...)} case.
   *
   * @param rel Relational expression
   * @return Children of relational expression
   */
  public List<RelNode> getChildRels(RelNode rel) {
    return nodeInputs.get(rel);
  }

  /** Assigns the input relational expressions of a given relational expression,
   * as seen by this particular call. Is only called when the operand is
   * {@link RelOptRule#any()}. */
  protected void setChildRels(RelNode rel, List<RelNode> inputs) {
    if (nodeInputs.isEmpty()) {
      nodeInputs = new HashMap<>();
    }
    nodeInputs.put(rel, inputs);
  }

  /**
   * Returns the planner.
   *
   * @return planner
   */
  public RelOptPlanner getPlanner() {
    return planner;
  }

  /**
   * Returns the current RelMetadataQuery, to be used for instance by
   * {@link RelOptRule#onMatch(RelOptRuleCall)}.
   */
  public RelMetadataQuery getMetadataQuery() {
    return rel(0).getCluster().getMetadataQuery();
  }

  /**
   * @return list of parents of the first relational expression
   */
  public List<RelNode> getParents() {
    return parents;
  }

  /**
   * transformTo with the option of adding additional tvr links.
   */
  public abstract void transformToWithOutRootEquivalence(
      Map<RelNode, Set<TvrMetaSetType>> newRels, Map<RelNode, RelNode> equiv,
      Map<RelNode, Map<TvrSemantics, List<TvrMetaSet>>> tvrUpdates,
      Map<Pair<RelNode, TvrMetaSetType>, Map<TvrProperty, List<TvrMetaSet>>> tvrPropertyInLinks);

  /**
   * Registers that a rule has produced an equivalent relational expression.
   *
   * <p>Called by the rule whenever it finds a match. The implementation of
   * this method guarantees that the original relational expression (that is,
   * <code>this.rels[0]</code>) has its traits propagated to the new
   * relational expression (<code>rel</code>) and its unregistered children.
   * Any trait not specifically set in the RelTraitSet returned by <code>
   * rel.getTraits()</code> will be copied from <code>
   * this.rels[0].getTraitSet()</code>.
   *
   * @param rel   Relational expression equivalent to the root relational
   *              expression of the rule call, {@code call.rels(0)}
   * @param equiv Map of other equivalences
   */
  public void transformTo(RelNode rel, Map<RelNode, RelNode> equiv) {
    if (equiv == null) {
      equiv = new HashMap<>();
    } else {
      equiv = new HashMap<>(equiv);
    }
    // Add the implicit root equivalence
    equiv.put(rel, rels[0]);
    transformToWithOutRootEquivalence(ImmutableMap.of(rel, ImmutableSet.of()),
        equiv, ImmutableMap.of(), ImmutableMap.of());
  }

  /**
   * Registers that a rule has produced an equivalent relational expression,
   * but no other equivalences.
   *
   * @param rel Relational expression equivalent to the root relational
   *            expression of the rule call, {@code call.rels(0)}
   */
  public final void transformTo(RelNode rel) {
    transformTo(rel, ImmutableMap.of());
  }

  /** Creates a {@link org.apache.calcite.tools.RelBuilder} to be used by
   * code within the call. The {@link RelOptRule#relBuilderFactory} argument contains policies
   * such as what implementation of {@link Filter} to create. */
  public RelBuilder builder() {
    return rule.relBuilderFactory.create(rel(0).getCluster(), null);
  }

  public TransformBuilder transformBuilder() {
    return new TransformBuilder(this);
  }

  public static class TransformBuilder {
    RelOptRuleCall call;

    // LinkedHashMap are used to preserve order
    Map<RelNode, Set<TvrMetaSetType>> newRels = new LinkedHashMap<>();
    Map<RelNode, RelNode> equiv = new HashMap<>();
    Map<RelNode, Map<TvrSemantics, List<TvrMetaSet>>> tvrUpdates =
        new LinkedHashMap<>();
    Map<Pair<RelNode, TvrMetaSetType>, Map<TvrProperty, List<TvrMetaSet>>>
        tvrPropertyInLinks = new LinkedHashMap<>();

    private TransformBuilder(RelOptRuleCall call) {
      this.call = call;
    }

    private Set<TvrMetaSetType> addRelInternal(RelNode rel) {
      return newRels.computeIfAbsent(rel, r -> new LinkedHashSet<>());
    }

    public TransformBuilder addRel(RelNode rel) {
      addRelInternal(rel);
      return this;
    }

    public TransformBuilder addTvrType(RelNode rel, TvrMetaSetType tvrType) {
      addRelInternal(rel).add(tvrType);
      return this;
    }

    public TransformBuilder addEquiv(RelNode rel, RelNode equivNode) {
      addRelInternal(rel);
      equiv.put(rel, equivNode);
      return this;
    }

    public TransformBuilder addTvrLink(RelNode rel, TvrSemantics tvrKey,
        TvrMetaSet tvr) {
      addRelInternal(rel);
      tvrUpdates.computeIfAbsent(rel, r -> new LinkedHashMap<>())
          .computeIfAbsent(tvrKey, r -> new ArrayList<>()).add(tvr);
      return this;
    }

    public TransformBuilder addPropertyLink(TvrMetaSet fromTvr,
        TvrProperty tvrProperty, RelNode rel, TvrMetaSetType tvrType) {
      addTvrType(rel, tvrType);
      tvrPropertyInLinks
          .computeIfAbsent(Pair.of(rel, tvrType), r -> new LinkedHashMap<>())
          .computeIfAbsent(tvrProperty, r -> new ArrayList<>()).add(fromTvr);
      return this;
    }

    public void transform() {
      call.transformToWithOutRootEquivalence(newRels, equiv, tvrUpdates,
          tvrPropertyInLinks);
    }
  }
}

// End RelOptRuleCall.java
