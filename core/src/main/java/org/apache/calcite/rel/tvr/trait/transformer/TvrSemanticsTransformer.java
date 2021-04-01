package org.apache.calcite.rel.tvr.trait.transformer;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.trait.transformer.predicate.TransformerPredicate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.ImmutableIntList;

import java.util.*;
import java.util.function.Predicate;

/**
 * Transforms a (component of) TvrSemantics to another one.
 */
public abstract class TvrSemanticsTransformer {

  protected Multimap<ImmutableIntList, Integer> forwardMapping =
      ArrayListMultimap.create();

  protected Map<Integer, ImmutableIntList> backwardMapping = new HashMap<>();

  /**
   * Gets a mapping that maps some columns from the original schema to
   * some other columns in the transformed schema.
   *
   * @return the forward mapping.
   */
  public Multimap<ImmutableIntList, Integer> forwardMapping() {
    return forwardMapping;
  }

  /**
   * Gets a mapping that maps some columns from the transformed schema back to
   * some other columns in the original schema.
   *
   * @return the backward mapping.
   */
  public Map<Integer, ImmutableIntList> backwardMapping() {
    return backwardMapping;
  }

  /**
   * Creates a new transformer based on the existing transformer.
   *
   * @param mapping the mapping between columns of the current operator
   *                to the one that will be created.
   *                If the key is -1, it means that there is a new column added to the schema
   * @return the new transformer.
   */
  public abstract TvrSemanticsTransformer transform(
      Multimap<Integer, Integer> mapping);

  /**
   * Get the necessary columns that the transformer needs to work properly
   * @return a set of the column refs
   */
  public abstract Set<Integer> getRequiredColumns();

  /**
   * Apply this transformer on the given input nodes.
   */
  public abstract List<RelNode> apply(List<RelNode> inputs);

  public abstract List<RelNode> consolidate(List<RelNode> inputs);

  /**
   * Return the row type after applying this transformer
   */
  public abstract RelDataType deriveRowType(RelDataType inputRowType,
      RelDataTypeFactory factory);

  public abstract TvrSemanticsTransformer addNewTransformer(
      TvrSemanticsTransformer newTransformer);

  /**
   * Check whether could apply certain rule with the given transformer
   */
  public abstract boolean isCompatible(TransformerPredicate predicate, ImmutableIntList indices);

  public boolean referToPartialResult(int index) {
    return false;
  }

  public boolean anyMatch(Predicate<TvrSemanticsTransformer> predicate) {
    return predicate.test(this);
  }

  public boolean allMatch(Predicate<TvrSemanticsTransformer> predicate) {
    return predicate.test(this);
  }

  /**
   * Get one-to-one mapping result (the result should contain only one element)
   * from the multimap by the given key.
   * e.g., multimap : [[0] -> [0], [1] -> [1], [1] -> [2]]
   *        key: [0]   return 0
   *        key: [1]   return 1
   */
  protected Integer getFirstMappingResult(
      Multimap<Integer, Integer> multimap, Integer key) {
    Collection<Integer> collection = multimap.get(key);
    if (collection.isEmpty()) {
      return null;
    }
    return collection.iterator().next();
  }

}
