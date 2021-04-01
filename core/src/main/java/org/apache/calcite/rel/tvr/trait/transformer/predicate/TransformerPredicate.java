package org.apache.calcite.rel.tvr.trait.transformer.predicate;

import org.apache.calcite.rel.tvr.trait.transformer.TvrAggregateTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrProjectTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrSortTransformer;
import org.apache.calcite.util.ImmutableIntList;

/**
 * Transformer compatible predicate
 */
public interface TransformerPredicate {

  boolean test(TvrAggregateTransformer transformer, ImmutableIntList indices);

  boolean test(TvrProjectTransformer transformer, ImmutableIntList indices);

  boolean test(TvrSortTransformer transformer, ImmutableIntList indices);

}
