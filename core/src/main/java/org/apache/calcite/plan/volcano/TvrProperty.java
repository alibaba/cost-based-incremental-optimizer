package org.apache.calcite.plan.volcano;

/**
 * Directed links between TvrMetaSets, used by various progressive computing
 * methods to describe property components of the tvr.
 */
public abstract class TvrProperty {

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object other);

}
