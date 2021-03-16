package org.apache.calcite.plan.tvr;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.Objects;

/**
 * Time-varying relation semantics trait.
 */
public abstract class TvrSemantics {
  public static final TvrSetSnapshot SET_SNAPSHOT_MAX = new TvrSetSnapshot(TvrVersion.MAX);

  public final TvrVersion fromVersion;
  public final TvrVersion toVersion;

  protected TvrSemantics(TvrVersion fromVersion, TvrVersion toVersion) {
    this.fromVersion = fromVersion;
    this.toVersion = toVersion;
  }

  /**
   * Derive the corresponding SetSnapshot schema
   */
  public RelDataType deriveRowType(RelDataType inputRowType,
      RelDataTypeFactory typeFactory) {
    return inputRowType;
  }

  public boolean timeRangeEquals(TvrSemantics other) {
    return Objects.equals(fromVersion, other.fromVersion) && Objects
        .equals(toVersion, other.toVersion);
  }

  public abstract TvrSemantics copy(TvrVersion from, TvrVersion to);

  public boolean timeRangeOverlaps(TvrSemantics other) {
    return fromVersion.compareTo(other.toVersion) <= 0 && toVersion.compareTo(other.fromVersion) >= 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fromVersion, toVersion);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TvrSemantics)) {
      return false;
    }
    TvrSemantics other = (TvrSemantics) obj;
    return timeRangeEquals(other);
  }

  @Override
  public String toString() {
    return "TvrSemantics@(" + fromVersion + ", " + toVersion + ")";
  }
}
