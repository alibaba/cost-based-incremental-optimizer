package org.apache.calcite.rel.tvr.utils;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.List;

public class TvrRowTypeUtils {
  public static boolean areRowTypesEqual(
      RelOptCluster cluster,
      RelDataType rowType1,
      RelDataType rowType2,
      boolean compareNames,
      boolean compareNullable) {
    if (rowType1 == rowType2) {
      return true;
    }

    boolean isStruct = rowType1.isStruct();
    if (isStruct != rowType2.isStruct()) {
      return false;
    }

    if (isStruct) {
      if (compareNames) {
        // if types are not identity-equal, then either the names or
        // the types must be different
        return false;
      }
      if (rowType2.getFieldCount() != rowType1.getFieldCount()) {
        return false;
      }
      final List<RelDataTypeField> f1 = rowType1.getFieldList();
      final List<RelDataTypeField> f2 = rowType2.getFieldList();
      for (Pair<RelDataTypeField, RelDataTypeField> pair : Pair.zip(f1, f2)) {
        final RelDataType type1 = pair.left.getType();
        final RelDataType type2 = pair.right.getType();
        if (!areRowTypesEqual(cluster, type1, type2, compareNullable)) {
          return false;
        }
      }
      return true;
    } else {
      return areRowTypesEqual(cluster, rowType1, rowType2, compareNullable);
    }
  }

  private static boolean areRowTypesEqual(RelOptCluster cluster, RelDataType type1, RelDataType type2,
                                          boolean compareNullable) {
    if (type1.isStruct() || type2.isStruct()) {
      throw new IllegalArgumentException("This method is just used to compare unstructured RelDataType");
    }

    // If one of the types is ANY comparison should succeed
    if (type1.getSqlTypeName() == SqlTypeName.ANY
        || type2.getSqlTypeName() == SqlTypeName.ANY) {
      return true;
    }

    if (compareNullable) {
      return type1.equals(type2);
    } else {
      final RelDataType type2WithSameNullable =
              cluster.getTypeFactory()
              .createTypeWithNullability(type2, type1.isNullable());
      return type1.equals(type2WithSameNullable);
    }
  }
}
