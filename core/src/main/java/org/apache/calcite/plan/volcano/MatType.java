package org.apache.calcite.plan.volcano;

import java.util.Collection;
import java.util.Comparator;

/**
 * Materialization type
 */
public enum MatType {
  // must be ordered in the increasing order of reuse cost
  // in memory
  MAT_MEMORY,
  // on disk
  MAT_DISK;

  static Comparator<MatType> comparator = Comparator.comparingInt(Enum::ordinal);

  public static MatType best(Collection<MatType> matTypes) {
    if (matTypes == null || matTypes.isEmpty()) {
      return null;
    }

    return matTypes.stream().sorted(comparator).findFirst().get();
  }
}
