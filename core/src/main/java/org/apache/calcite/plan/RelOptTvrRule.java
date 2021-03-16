package org.apache.calcite.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrSetSnapshot;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.TvrEdgeRelOptRuleOperand;
import org.apache.calcite.plan.volcano.TvrEdgeTimeMatchInfo;
import org.apache.calcite.plan.volcano.TvrEdgeTimeMatchInfo.TvrEdgeTimeMatchPolicy;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrProperty;
import org.apache.calcite.plan.volcano.TvrPropertyEdgeRuleOperand;
import org.apache.calcite.plan.volcano.TvrRelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class RelOptTvrRule extends RelOptRule {

  public RelOptTvrRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String description) {
    super(operand, relBuilderFactory, description);
  }

  public static class RelOfTvr {
    public final RelMatch rel;
    public final TvrSemantics tvrSemantics;
    public final TvrMatch tvr;

    public RelOfTvr(RelMatch rel, TvrSemantics tvrSemantics, TvrMatch tvr) {
      this.rel = rel;
      this.tvrSemantics = tvrSemantics;
      this.tvr = tvr;
    }
  }

  public static class TvrMatch {
    int ordinal;
    int tvrOpStart;
    int edgeOpStart;
    int propertyEdgeOpStart;
    RelOptTvrRule rule;
    RelOptRuleCall call;

    public TvrMatch(RelOptTvrRule rule, RelOptRuleCall call, int ordinal) {
      this.ordinal = ordinal;
      this.rule = rule;
      this.call = call;

      tvrOpStart = rule.getTvrOpStartIndex();
      edgeOpStart = rule.getTvrEdgeOpStartIndex();
      propertyEdgeOpStart = rule.getTvrPropertyEdgeOpStartIndex();
      checkArgument(call.rule == rule);
      checkArgument(tvrOpStart >= 0);
      checkArgument(ordinal >= tvrOpStart && ordinal < edgeOpStart);
    }

    /**
     * Gets the wrapped TvrMetaSet.
     */
    public TvrMetaSet get() {
      return call.tvrs[ordinal - tvrOpStart];
    }

    /**
     * Gets the relNode corresponds to the TvrSemantics object.
     */
    public RelMatch rel(TvrSemantics tvrSemantics) {
      return Optional.ofNullable(rel((r, t) -> t.equals(tvrSemantics)))
          .map(p -> p.rel).orElse(null);
    }

    public Iterator<RelOfTvr> relsIter(
        BiPredicate<? super RelMatch, ? super TvrSemantics> p) {
      return rule.tvrOperands.get(ordinal - tvrOpStart).tvrChildren.stream()
          .map(e -> {
            int relIndex = e.getRelOp().ordinalInRule;
            RelMatch r = new RelMatch(rule, call, relIndex);
            TvrSemantics t = call.tvrTraits[e.ordinalInRule - edgeOpStart];
            return !p.test(r, t) ? null : new RelOfTvr(r, t, this);
          }).filter(Objects::nonNull).iterator();
    }

    /**
     * Gets all rel matches in this TVR that satisfies the predicate.
     */
    public List<RelOfTvr> rels(
        BiPredicate<? super RelMatch, ? super TvrSemantics> p) {
      return ImmutableList.copyOf(relsIter(p));
    }

    /**
     * Gets all rel matches in this TVR with the tvrSemantics class.
     */
    public List<RelOfTvr> rels(Class<? super TvrSemantics> tvrClass) {
      return rels((r, t) -> tvrClass.isAssignableFrom(t.getClass()));
    }

    /**
     * Gets the first rel match in this TVR that satisfies the predicate.
     */
    public RelOfTvr rel(BiPredicate<? super RelMatch, ? super TvrSemantics> p) {
      return Iterators.getNext(relsIter(p), null);
    }

    /**
     * Gets the first rel match in this TVR with the tvrSemantics class.
     */
    public RelOfTvr rel(Class<? extends TvrSemantics> tvrClass) {
      return rel((r, t) -> tvrClass.isAssignableFrom(t.getClass()));
    }

    /**
     * Gets the first match of the toTvr in the property link.
     */
    public TvrMatch propertyToTvr(Class<? extends TvrProperty> propertyClass) {
      return Iterators.getNext(
          propertyToTvrsIter(t -> propertyClass.isAssignableFrom(t.getClass())),
          null);
    }

    /**
     * Gets the first match of the toTvr in the property link.
     */
    public TvrMatch propertyToTvr() {
      return Iterators.getNext(propertyToTvrsIter(t -> true), null);
    }

    public Iterator<TvrMatch> propertyToTvrsIter(
        Predicate<? super TvrProperty> p) {
      TvrRelOptRuleOperand fromTvrOp =
          rule.tvrOperands.get(ordinal - tvrOpStart);
      return fromTvrOp.tvrPropertyEdges.stream()
          .filter(e -> e.getFromTvrOp() == fromTvrOp).map(e -> {
            TvrProperty property =
                call.tvrProperties[e.ordinalInRule - propertyEdgeOpStart];
            return p.test(property) ?
                new TvrMatch(rule, call, e.getToTvrOp().ordinalInRule) :
                null;
          }).filter(Objects::nonNull).iterator();
    }

    /**
     * Gets the first match of the tvr property in the tvr property links.
     */
    public <T extends TvrProperty> T property(
        Class<? extends TvrProperty> propertyClass) {
      return Iterators.getNext(
          propertyIter(t -> propertyClass.isAssignableFrom(t.getClass())),
          null);
    }

    /**
     * Gets the first match of the toTvr in the property link.
     */
    public <T extends TvrProperty> T property() {
      return Iterators.getNext(propertyIter(t -> true), null);
    }

    public <T extends TvrProperty> Iterator<T> propertyIter(
        Predicate<? super TvrProperty> p) {
      TvrRelOptRuleOperand fromTvrOp =
          rule.tvrOperands.get(ordinal - tvrOpStart);
      return fromTvrOp.tvrPropertyEdges.stream()
          .filter(e -> e.getFromTvrOp() == fromTvrOp).map(e -> {
            TvrProperty property =
                call.tvrProperties[e.ordinalInRule - propertyEdgeOpStart];
            return (T) (p.test(property) ? property : null);
          }).filter(Objects::nonNull).iterator();
    }
  }

  public static class RelMatch {
    int ordinal;
    RelOptTvrRule rule;
    RelOptRuleCall call;

    public RelMatch(RelOptTvrRule rule, RelOptRuleCall call, int ordinal) {
      this.ordinal = ordinal;
      this.rule = rule;
      this.call = call;

      int endOrdinal = rule.getMatchingRelCount();
      checkArgument(call.rule == rule);
      checkArgument(ordinal >= 0 && ordinal < endOrdinal);
    }

    /**
     * Gets the wrapped RelNode.
     */
    public <T extends RelNode> T get() {
      return call.rel(ordinal);
    }

    /**
     * Navigates to the parent of this matched RelNode, null if not present.
     */
    public RelMatch parent() {
      RelOptRuleOperand parent = rule.getOperands().get(ordinal).getParent();
      if (parent != null) {
        return new RelMatch(rule, call, parent.ordinalInRule);
      }
      return null;
    }

    /**
     * Gets all children of this matched RelNode.
     */
    public List<RelMatch> inputs() {
      return rule.getOperands().get(ordinal).getChildOperands().stream()
          .map(op -> new RelMatch(rule, call, op.ordinalInRule))
          .collect(Collectors.toList());
    }

    /**
     * Navigates to the i-th child of this matched RelNode, null if not
     * present.
     */
    public RelMatch input(int i) {
      List<RelOptRuleOperand> children =
          rule.getOperands().get(ordinal).getChildOperands();
      if (i < children.size()) {
        return new RelMatch(rule, call, children.get(i).ordinalInRule);
      }
      return null;
    }

    /**
     * Gets the TVR connected with this RelNode.
     */
    public TvrMatch tvr() {
      return tvrEdge().map(TvrEdgeRelOptRuleOperand::getTvrOp)
          .map(tvrOp -> new TvrMatch(rule, call, tvrOp.ordinalInRule))
          .orElse(null);
    }

    /**
     * Gets the TVR connected with this RelNode.
     */
    public List<TvrMatch> tvrs() {
      return tvrEdges().stream().map(TvrEdgeRelOptRuleOperand::getTvrOp)
          .map(tvrOp -> new TvrMatch(rule, call, tvrOp.ordinalInRule))
          .collect(Collectors.toList());
    }

    /**
     * Gets the associated TVRSemantics of this RelNode.
     */
    public TvrSemantics tvrTrait() {
      int edgeStart = rule.getTvrEdgeOpStartIndex();
      return tvrEdge()
          .map(edgeOp -> call.tvrTraits[edgeOp.ordinalInRule - edgeStart])
          .orElse(null);
    }

    private Optional<TvrEdgeRelOptRuleOperand> tvrEdge() {
      return rule.tvrEdgeOperands.stream().filter(
          edgeOp -> edgeOp.getRelOp() == rule.getOperands().get(ordinal))
          .findFirst();
    }

    private List<TvrEdgeRelOptRuleOperand> tvrEdges() {
      return rule.tvrEdgeOperands.stream().filter(
          edgeOp -> edgeOp.getRelOp() == rule.getOperands().get(ordinal))
          .collect(Collectors.toList());
    }

    /**
     * Finds the first TVR sibling of this RelNode that satisfies the
     * predicate.
     */
    public RelOfTvr tvrSibling(Predicate<? super TvrSemantics> tvrPredicate) {
      return tvr().rel(
          (r, t) -> !r.tvrTrait().equals(this.tvrTrait()) && tvrPredicate
              .test(t));
    }

    /**
     * Finds the first TVR sibling of this RelNode that is the tvrClass
     */
    public RelOfTvr tvrSibling(Class<? extends TvrSemantics> tvrClass) {
      return tvrSibling(t -> tvrClass.isAssignableFrom(t.getClass()));
    }

    /**
     * Finds the first TVR sibling of this RelNode.
     */
    public RelOfTvr tvrSibling() {
      return tvrSibling(t -> true);
    }

    /**
     * Finds all TVR siblings of this RelNode.
     */
    public List<RelOfTvr> tvrSiblings(
        Predicate<? super TvrSemantics> tvrPredicate) {
      return tvr().rels(
          (r, t) -> !r.tvrTrait().equals(this.tvrTrait()) && tvrPredicate
              .test(t));
    }

    /**
     * Finds all TVR siblings of this RelNode.
     */
    public List<RelOfTvr> tvrSiblings() {
      return tvrSiblings(t -> true);
    }

  }

  /**
   * All TvrEdgeRelOptRuleOperands sharing the same TvrEdgeTimeMatchInfo
   * instance is considered a group for the time policy.
   */
  public static final TvrEdgeTimeMatchInfo IDENTICAL_TIME =
      new TvrEdgeTimeMatchInfo(TvrEdgeTimeMatchPolicy.IDENTICAL);

  public static final TvrEdgeTimeMatchInfo IDENTICAL_TIME_2 =
      new TvrEdgeTimeMatchInfo(TvrEdgeTimeMatchPolicy.IDENTICAL);

  public RelMatch getRoot(RelOptRuleCall call) {
    return new RelMatch(this, call, 0);
  }

  public void transformToRootTvr(RelOptRuleCall call, RelNode rel,
      TvrSemantics tvrSemantics) {
    call.transformBuilder()
        .addTvrLink(rel, tvrSemantics, getRoot(call).tvr().get()).transform();
  }

  //~ TVR Rule Operands ------------------------------------------------------

  public static <R extends RelNode> RelOptRuleOperand operand(Class<R> relClass,
      TvrEdgeRelOptRuleOperand tvrEdge, RelOptRuleOperand first,
      RelOptRuleOperand... rest) {
    return operand(relClass, null, singletonList(tvrEdge), some(first, rest));
  }

  public static <R extends RelNode> RelOptRuleOperand operand(Class<R> relClass,
      TvrEdgeRelOptRuleOperand tvrEdge, RelOptRuleOperandChildren children) {
    return operand(relClass, null, singletonList(tvrEdge), children);
  }

  public static <R extends RelNode> RelOptRuleOperand operand(Class<R> relClass,
      RelOptRuleOperandChildren children,
      TvrEdgeRelOptRuleOperand... tvrEdges) {
    return operand(relClass, null, Arrays.asList(tvrEdges), children);
  }

  public static <R extends RelNode> RelOptRuleOperand operand(Class<R> relClass,
      Predicate<? super R> relPredicate, TvrEdgeRelOptRuleOperand tvrEdge,
      RelOptRuleOperand first, RelOptRuleOperand... rest) {
    return operand(relClass, relPredicate, singletonList(tvrEdge),
        some(first, rest));
  }

  public static <R extends RelNode> RelOptRuleOperand operand(Class<R> relClass,
      Predicate<? super R> relPredicate, TvrEdgeRelOptRuleOperand tvrEdge,
      RelOptRuleOperandChildren children) {
    return operand(relClass, relPredicate, singletonList(tvrEdge), children);
  }

  public static <R extends RelNode> RelOptRuleOperand operand(Class<R> relClass,
      Predicate<? super R> relPredicate,
      List<TvrEdgeRelOptRuleOperand> tvrEdges,
      RelOptRuleOperandChildren children) {
    // Hard code trait to be logical, and relPredicate to be at least
    // not converter
    Predicate<? super R> finalRelPredicate = (relPredicate == null) ?
        x -> !(x instanceof AbstractConverter) :
        x -> (relPredicate.test(x) && !(x instanceof AbstractConverter));
    RelOptRuleOperand relOp =
        new RelOptRuleOperand(relClass, Convention.NONE, finalRelPredicate,
            children.policy, children.operands);

    tvrEdges.forEach(e -> {
      assert e.getRelOp() == null && e.getTvrOp() != null;
      e.getTvrOp().addTvrConnection(e.getMatchedTvrClass(), e.getPredicate(),
          e.getTimeInfoList(), relOp, e.enforceTvrType());
    });
    return relOp;
  }

  public static TvrEdgeRelOptRuleOperand[] adjacentTimeEdges(
      TvrEdgeRelOptRuleOperand... edges) {
    TvrEdgeTimeMatchInfo adjacent =
        new TvrEdgeTimeMatchInfo(TvrEdgeTimeMatchPolicy.ADJACENT);
    for (TvrEdgeRelOptRuleOperand edge : edges) {
      edge.addTvrEdgeTimeMatchInfo(adjacent);
    }
    return edges;
  }

  public static TvrEdgeRelOptRuleOperand tvrEdgeSSMax(
      RelOptRuleOperand operand) {
    return tvrEdge(TvrSetSnapshot.class,
        t -> t.equals(TvrSemantics.SET_SNAPSHOT_MAX), operand, emptyList(),
        true);
  }

  public static TvrEdgeRelOptRuleOperand tvrEdgeSSMax(RelOptRuleOperand operand,
      TvrEdgeTimeMatchInfo timeInfo) {
    List<TvrEdgeTimeMatchInfo> infoList =
        timeInfo == null ? emptyList() : singletonList(timeInfo);
    return tvrEdge(TvrSetSnapshot.class,
        t -> t.equals(TvrSemantics.SET_SNAPSHOT_MAX), operand, infoList, true);
  }

  public static <T extends TvrSemantics> TvrEdgeRelOptRuleOperand tvrEdge(
      Class<T> tvrClass, RelOptRuleOperand operand) {
    return tvrEdge(tvrClass, null, operand, emptyList(), false);
  }

  public static <T extends TvrSemantics> TvrEdgeRelOptRuleOperand tvrEdge(
      Class<T> tvrClass, Predicate<? super T> predicate,
      RelOptRuleOperand operand) {
    return tvrEdge(tvrClass, predicate, operand, emptyList(), false);
  }

  public static <T extends TvrSemantics> TvrEdgeRelOptRuleOperand tvrEdge(
      Class<T> tvrClass, Predicate<? super T> predicate,
      TvrEdgeTimeMatchInfo timeInfo, RelOptRuleOperand operand) {
    List<TvrEdgeTimeMatchInfo> infoList =
        timeInfo == null ? emptyList() : singletonList(timeInfo);
    return tvrEdge(tvrClass, predicate, operand, infoList, false);
  }

  public static <T extends TvrSemantics> TvrEdgeRelOptRuleOperand tvrEdge(
      Class<T> tvrClass, Predicate<? super T> predicate,
      RelOptRuleOperand operand, List<TvrEdgeTimeMatchInfo> timeInfo,
      boolean enforceTvrType) {
    Predicate<? super T> pred = predicate == null ? x -> true : predicate;
    if (operand.getClass().equals(RelOptRuleOperand.class)) {
      return new TvrEdgeRelOptRuleOperand(tvrClass, pred, timeInfo, null,
          operand, enforceTvrType);
    } else if (operand.getClass().equals(TvrRelOptRuleOperand.class)) {
      return new TvrEdgeRelOptRuleOperand(tvrClass, pred, timeInfo,
          (TvrRelOptRuleOperand) operand, null, enforceTvrType);
    } else {
      throw new RuntimeException("operand must be relOperand or tvrOperand");
    }
  }

  public static <R extends TvrProperty> TvrPropertyEdgeRuleOperand tvrProperty(
      Class<R> tvrPropertyClass, TvrRelOptRuleOperand tvrOp) {
    return tvrProperty(tvrPropertyClass, p -> true, tvrOp);
  }

  public static <R extends TvrProperty> TvrPropertyEdgeRuleOperand tvrProperty(
      Class<R> tvrPropertyClass, Predicate<? super R> predicate,
      TvrRelOptRuleOperand tvrOp) {
    return new TvrPropertyEdgeRuleOperand(tvrPropertyClass, predicate, null,
        tvrOp);
  }

  public static TvrRelOptRuleOperand tvr() {
    return tvr(true);
  }

  public static TvrRelOptRuleOperand tvr(boolean sameTvrType) {
    return tvr(null, sameTvrType, emptyList(), emptyList());
  }

  public static TvrRelOptRuleOperand tvr(TvrEdgeRelOptRuleOperand tvrEdge) {
    return tvr(null, true, singletonList(tvrEdge), emptyList());
  }

  public static TvrRelOptRuleOperand tvr(TvrEdgeRelOptRuleOperand... tvrEdges) {
    return tvr(null, true, Arrays.asList(tvrEdges), emptyList());
  }

  public static TvrRelOptRuleOperand tvr(
      TvrPropertyEdgeRuleOperand tvrPropertyEdge) {
    return tvr(null, true, emptyList(), singletonList(tvrPropertyEdge));
  }

  public static TvrRelOptRuleOperand tvr(
      TvrPropertyEdgeRuleOperand... tvrPropertyEdges) {
    return tvr(null, true, emptyList(), Arrays.asList(tvrPropertyEdges));
  }

  public static TvrRelOptRuleOperand tvr(TvrEdgeRelOptRuleOperand tvrEdge,
      TvrPropertyEdgeRuleOperand... tvrProperties) {
    return tvr(null, true, singletonList(tvrEdge), Arrays.asList(tvrProperties));
  }

  public static TvrRelOptRuleOperand tvr(Predicate<TvrMetaSet> predicate,
      boolean sameTvrType, List<TvrEdgeRelOptRuleOperand> tvrEdges,
      List<TvrPropertyEdgeRuleOperand> tvrProperties) {
    TvrRelOptRuleOperand tvrOp =
        new TvrRelOptRuleOperand(predicate == null ? x -> true : predicate,
            sameTvrType);
    tvrEdges.forEach(e -> {
      assert e.getRelOp() != null;
      assert e.getTvrOp() == null;
      tvrOp.addTvrConnection(e.getMatchedTvrClass(), e.getPredicate(),
          e.getTimeInfoList(), e.getRelOp(), e.enforceTvrType());
    });
    tvrProperties.forEach(p -> {
      assert p.getFromTvrOp() == null;
      assert p.getToTvrOp() != null;
      tvrOp.addTvrPropertyEdge(p.getMatchedPropertyClass(), p.getPredicate(),
          p.getToTvrOp());
    });
    return tvrOp;
  }

  public static RelOptRuleOperandChildren some(
      List<RelOptRuleOperand> children) {
    return new RelOptRuleOperandChildren(RelOptRuleOperandChildPolicy.SOME,
        children);
  }

  public static RelOptRuleOperand logicalSubset() {
    return operand(RelSubset.class, Convention.NONE, any());
  }

}
