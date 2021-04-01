package org.apache.calcite.plan.volcano;

import static org.apache.calcite.plan.volcano.MatType.MAT_DISK;
import static org.apache.calcite.plan.volcano.MatType.MAT_MEMORY;

class ReuseUtils {

  enum UseType {
    TO_MEMORY,  // require table sink
    TO_DISK,    // require table sink
    TO_OP       // require the cheapest sub-plan (with the same time)
  }

  static UseType matType2UseType(MatType matType) {
    switch (matType) {
    case MAT_DISK:
      return UseType.TO_DISK;
    case MAT_MEMORY:
      return UseType.TO_MEMORY;
    default:
      throw new IllegalArgumentException("Illegal MatType " + matType);
    }
  }

  static MatType useType2matType(UseType useType) {
    switch (useType) {
    case TO_DISK:
      return MAT_DISK;
    case TO_MEMORY:
      return MAT_MEMORY;
    default:
      throw new IllegalArgumentException("Illegal UseType " + useType);
    }
  }

}
