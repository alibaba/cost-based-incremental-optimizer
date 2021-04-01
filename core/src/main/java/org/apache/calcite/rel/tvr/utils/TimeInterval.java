package org.apache.calcite.rel.tvr.utils;

import org.apache.calcite.plan.tvr.TvrVersion;

import java.io.Serializable;

public class TimeInterval implements Serializable {
  private static final long serialVersionUID = 1L;

  public final long from;
  public final long to;

  private TimeInterval(long from, long to) {
    this.from = from;
    this.to = to;
  }

  public static TimeInterval of(long from, long to) {
    return new TimeInterval(from, to);
  }

  @Override
  public int hashCode() {
    return Long.hashCode(from) * 31 + Long.hashCode(to);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o instanceof TimeInterval) {
      TimeInterval inv = (TimeInterval) o;
      return from == inv.from && to == inv.to;
    }
    return false;
  }

  private static String formatVersion(long version) {
    if (version == TvrVersion.MIN_TIME) {
      return "MIN";
    } else if (version == TvrVersion.MAX_TIME) {
      return "MAX";
    } else {
      return Long.toHexString(version);
    }
  }

  @Override
  public String toString() {
    return "[" + (from == TvrVersion.MIN_TIME ? "" : formatVersion(from)) + ", "
        + (to == TvrVersion.MAX_TIME ? "" : formatVersion(to)) + "]";
  }
}
