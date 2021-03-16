package org.apache.calcite.util;

import org.apache.calcite.plan.tvr.TvrVersion;

import java.io.Serializable;
import java.util.Objects;

public class VersionInterval implements Serializable {
  public final TvrVersion from;
  public final TvrVersion to;

  public VersionInterval(TvrVersion from, TvrVersion to) {
    this.from = from;
    this.to = to;
  }

  public static VersionInterval of(TvrVersion from, TvrVersion to) {
    return new VersionInterval(from, to);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof VersionInterval)) {
      return false;
    }
    VersionInterval other = (VersionInterval) o;
    return Objects.equals(from, other.from) && Objects.equals(to, other.to);
  }

  @Override
  public int hashCode() {
    return Objects.hash(from, to);
  }

  @Override
  public String toString() {
    return "[" + (from.isMin() ? "" : from) + ", " + (to.isMax() ? "" : to) + "]";
  }
}
