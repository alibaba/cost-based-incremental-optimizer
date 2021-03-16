package org.apache.calcite.plan.volcano;

/**
 * Time matching policy for a group of TvrEdgeRelOptRuleOperands. All edgeOps
 * sharing the same instance of TvrEdgeTimeMatchInfo is considered a group.
 */
public class TvrEdgeTimeMatchInfo {

  /**
   * Time matching policy for multiple TvrEdgeRelOptRuleOperands.
   */
  public enum TvrEdgeTimeMatchPolicy {

    /**
     * All tvr edges are of the exact same time interval
     */
    IDENTICAL,

    /**
     * All tvr edge time intervals are adjacent to each other, ascending order:
     * tvrEdge1.toVersion == tvrEdge2.fromVersion
     * tvrEdge2.toVersion == tvrEdge3.fromVersion
     * ...
     */
    ADJACENT,

  }

  public TvrEdgeTimeMatchInfo(TvrEdgeTimeMatchPolicy policy) {
    this.policy = policy;
  }

  public final TvrEdgeTimeMatchPolicy policy;

}