package org.apache.calcite.rel.tvr.trait.transformer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.gson.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.tvr.rels.LogicalTvrSortDeduper;
import org.apache.calcite.rel.tvr.rels.LogicalTvrWindowDeduper;
import org.apache.calcite.rel.tvr.trait.transformer.predicate.TransformerPredicate;
import org.apache.calcite.rel.tvr.utils.TvrJsonUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Sample implementation for sort transformer (for value semantics).
 */
public class TvrSortTransformer extends TvrSemanticsTransformer {
  public ImmutableBitSet partitionKeys;

  private final RelCollation collation;
  private final RelDistribution distribution;

  private final BigDecimal limit;
  private final BigDecimal offset;

  // For window, this field indicates the index of the row number column.
  // If it is null, it means that this column has been removed by a project.
  // Otherwise, the value of this column needs to be recalculated when all the deltas have been merged.
  private Integer rowNumberColIndex;

  public TvrSortTransformer(ImmutableBitSet partitionKeys,
      RelCollation collation, BigDecimal limit, RelDistribution distribution,
      Integer rowNumberColIndex, int fieldCount) {
    this(partitionKeys, collation, limit, null, distribution, rowNumberColIndex,
        fieldCount);
  }

  public TvrSortTransformer(ImmutableBitSet partitionKeys,
      RelCollation collation, BigDecimal limit, BigDecimal offset,
      RelDistribution distribution, Integer rowNumberColIndex, int fieldCount) {
    this.partitionKeys = partitionKeys;
    this.rowNumberColIndex = rowNumberColIndex;
    if (collation != null && !(collation instanceof RelCollationImpl)) {
      collation = new RelCollationImpl(
          ImmutableList.copyOf(collation.getFieldCollations().stream().map(
              relFieldCollation -> new RelFieldCollation(
                  relFieldCollation.getFieldIndex(),
                  relFieldCollation.direction, relFieldCollation.nullDirection))
              .collect(Collectors.toList())));
    }
    this.collation = collation;
    this.distribution = distribution;
    this.limit = limit;
    this.offset = offset;
    IntStream.range(0, fieldCount).forEach(i -> {
      ImmutableIntList index = ImmutableIntList.of(i);
      forwardMapping.put(index, i);
      backwardMapping.put(i, index);
    });
  }

  @Override
  public TvrSemanticsTransformer transform(
      Multimap<Integer, Integer> colsMapping) {
    ImmutableBitSet newPartitionKeys = null;
    if (null != partitionKeys) {
      newPartitionKeys = ImmutableBitSet.of(partitionKeys.toList().stream()
          .map(i -> getFirstMappingResult(colsMapping, i))
          .collect(Collectors.toList()));
    }

    RelCollationImpl newCollation = null;
    if (null != collation) {
      List<RelFieldCollation> newFieldCollations =
          collation.getFieldCollations().stream().map(relFieldCollation -> {
            int index = relFieldCollation.getFieldIndex();
            Integer newIndex = Objects
                .requireNonNull(getFirstMappingResult(colsMapping, index));
            return new RelFieldCollation(newIndex,
                relFieldCollation.direction, relFieldCollation.nullDirection);
          }).collect(Collectors.toList());

      // calcite doesn't have the concept of getEquivalents()

//      List<ImmutableBitSet> newEquivalents = new ArrayList<>();
//      if (collation instanceof RelCollationImpl) {
//        ((RelCollationImpl) collation).getEquivalents().forEach(bitSet -> {
//          List<Integer> newIndexList = bitSet.toList().stream()
//              .map(i -> getFirstMappingResult(colsMapping, i))
//              .filter(Objects::nonNull)
//                  .collect(Collectors.toList());
//          if (newIndexList.size() > 1) {
//            newEquivalents.add(ImmutableBitSet.of(newIndexList));
//          }
//        });
//      }

      newCollation =
          new RelCollationImpl(ImmutableList.copyOf(newFieldCollations));
    }

    Set<Integer> colKeySet = colsMapping.keySet();
    int sourceCount =
        colKeySet.stream().max(Integer::compareTo).get() + 1 + (int) colKeySet
            .stream().filter(i -> i < 0).count();
    int targetCount = (int) colsMapping.values().stream().distinct().count();
    Mappings.TargetMapping mapping =
        Mappings.create(MappingType.FUNCTION, sourceCount, targetCount);
    colsMapping.entries().stream().filter(e -> e.getKey() >= 0)
        .forEach(e -> mapping.set(e.getKey(), e.getValue()));
    RelDistribution newDistribution = distribution.apply(mapping);

    Integer newRowNumberColIndex = null;
    if (rowNumberColIndex != null) {
      newRowNumberColIndex = getFirstMappingResult(colsMapping, rowNumberColIndex);
    }

    // init forward mapping and backward mapping, all of them are identity maps
    return new TvrSortTransformer(newPartitionKeys, newCollation, limit, offset,
        newDistribution, newRowNumberColIndex, targetCount);
  }

  @Override
  public Set<Integer> getRequiredColumns() {
    Set<Integer> keySet = new HashSet<>();
    if (partitionKeys != null) {
      keySet.addAll(partitionKeys.asSet());
    }

    if (collation != null) {
      for (RelFieldCollation field : collation.getFieldCollations()) {
        keySet.add(field.getFieldIndex());
      }
    }

    return keySet;
  }

  /**
   *  value delta
   *    -> window:  1. re-order the input tuples by the partition keys
   *                2. add a new column "__tvr__window" to save the result of
   *                   window function (it must be 'ROW_NUMBER' here)
   *
   *    -> filter:  select the specified number of tuples (i.e., only one)
   *
   *    -> project: 1. delete the window column that is temporarily created to store the results
   *                   if this column is just used to select the specified number of tuples
   *                   (that is, the window column has been removed in the original query by a project).
   *                2. replace the obsolete window column with the new window column
   *                   whose value has been recomputed by the new window operator
   *                   (that is, the window column is still kept in the schema).
   *
   *    -> set snapshot (or a pure value delta)
   * -----------------------------------------------------------------------------
   *  value delta
   *    -> sort:    1. re-order the input tuples by the partition keys
   *                2. select the specified number of tuples
   *
   *    -> set snapshot (or a pure value delta)
   */
  @Override
  public List<RelNode> apply(List<RelNode> inputs) {
    // NOTE: 'consolidate' reuses this method,
    // when modifying this function, please strictly confirm the correctness of the modification.
    assert !inputs.isEmpty();
    RelOptCluster cluster = inputs.get(0).getCluster();

    if (isWindow()) {
      RelDataTypeFactory typeFactory = cluster.getTypeFactory();
      return inputs.stream().map(input -> {
        int columnNum = input.getRowType().getFieldCount();

        // create window, add a new window column.
        // e.g., col_1, col_2, ..., col_i, new Window Column (compute ROW_NUMBER)
        columnNum += 1;

        // create filter, columns will not be modified.

        // create project, remove the new window column or
        // replace the original row number column with it.
        RelDataType newWindowType;
        List<Integer> projectRefs = new ArrayList<>();
        if (rowNumberColIndex != null) {
          // if the row number column is still in the schema,
          // it is necessary to recompute the value of this column.
          // replace the original row number column with the newly created window column here.
          for (int i = 0; i < columnNum - 1; i++) {
            if (i != rowNumberColIndex) {
              projectRefs.add(i);
            } else {
              projectRefs.add(columnNum - 1);
            }
          }
          // keep the row type of the new window column be the same with the original one
          newWindowType = input.getRowType().getFieldList().get(rowNumberColIndex).getType();
        } else {
          // remove the temporary window column (which is the last column)
          IntStream.range(0, columnNum - 1).forEach(projectRefs::add);
          // set the default row type, because this column will be deleted finally.
          newWindowType = typeFactory.createSqlType(SqlTypeName.BIGINT);
          newWindowType = typeFactory.createTypeWithNullability(newWindowType, true);
        }

        return LogicalTvrWindowDeduper
            .create(input, partitionKeys, newWindowType, collation, limit,
                ImmutableIntList.copyOf(projectRefs));
      }).collect(Collectors.toList());
    } else {
      return inputs.stream().map(input -> LogicalTvrSortDeduper
          .create(input, collation, distribution, limit, offset))
          .collect(Collectors.toList());
    }
  }

  @Override
  public List<RelNode> consolidate(List<RelNode> inputs) {
    assert !inputs.isEmpty();
    // sort transformer can reuse the 'apply' method to do the consolidation
    return apply(inputs);
  }

  public boolean isWindow() {
    return partitionKeys != null && !partitionKeys.isEmpty();
  }

  @Override
  public RelDataType deriveRowType(RelDataType inputRowType,
      RelDataTypeFactory factory) {
    // sort transformer does not change the columns of input.
    return inputRowType;
  }

  public Integer getRowNumberColIndex() {
    return rowNumberColIndex;
  }

  @Override
  public TvrSemanticsTransformer addNewTransformer(
      TvrSemanticsTransformer newTransformer) {
    if (newTransformer instanceof TvrCompositeTransformer) {
      List<TvrSemanticsTransformer> transformers = Lists.newArrayList(this);
      transformers.addAll(
          ((TvrCompositeTransformer) newTransformer).getInnerTransformers());
      return new TvrCompositeTransformer(transformers);
    }
    return new TvrCompositeTransformer(
        Lists.newArrayList(this, newTransformer));
  }

  @Override
  public boolean isCompatible(TransformerPredicate predicate, ImmutableIntList indices) {
    return predicate.test(this, indices);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TvrSortTransformer)) {
      return false;
    }
    TvrSortTransformer that = (TvrSortTransformer) o;
    return Objects.equals(collation, that.collation) && Objects
        .equals(limit, that.limit) && Objects
        .equals(partitionKeys, that.partitionKeys);
  }

  @Override
  public int hashCode() {
    return Objects.hash(collation, limit, partitionKeys);
  }

  public RelCollation getCollation() {
    return collation;
  }

  public BigDecimal getLimit() {
    return limit;
  }

  public ImmutableBitSet getPartitionKeys() {
    return partitionKeys;
  }

  public static class TvrSortTransformerSerde
      implements JsonDeserializer<TvrSortTransformer>,
      JsonSerializer<TvrSortTransformer> {

    public static TvrSortTransformerSerde INSTANCE = new TvrSortTransformerSerde();

    @Override
    public TvrSortTransformer deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      BigDecimal limit = null;
      BigDecimal offset = null;
      if (jsonObject.get("limit") != null) {
        limit = new BigDecimal(jsonObject.get("limit").getAsString());
      }
      if (jsonObject.get("offset") != null) {
        offset = new BigDecimal(jsonObject.get("offset").getAsString());
      }
      RelCollation collation = null;
      if (jsonObject.getAsJsonObject("collation") != null) {
        collation = context.deserialize(jsonObject.getAsJsonObject("collation"),
            RelCollationImpl.class);
      }

      ImmutableBitSet partitionKeys = null;
      if (jsonObject.get("partitionKeys") != null) {
        partitionKeys = ImmutableBitSet.of(Arrays.asList(context
            .deserialize(jsonObject.get("partitionKeys"), Integer[].class)));
      }

      RelDistribution distribution;
      if (jsonObject.get("relDistributionImpl") != null) {
        distribution = TvrJsonUtils.deserializeRelDistributionImpl(
            jsonObject.get("relDistributionImpl").getAsString());
      } else {
        distribution = context
            .deserialize(jsonObject.getAsJsonObject("distribution"),
                    RelDistribution.class);
      }

      Integer rowNumberColIndex = null;
      if (jsonObject.get("rowNumberColIndex") != null) {
        rowNumberColIndex = jsonObject.get("rowNumberColIndex").getAsInt();
      }

      int fieldCount = jsonObject.get("fieldCount").getAsInt();
      return new TvrSortTransformer(partitionKeys, collation, limit, offset,
          distribution, rowNumberColIndex, fieldCount);
    }

    @Override
    public JsonElement serialize(TvrSortTransformer src, Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject jsonObject = new JsonObject();
      if (src.limit != null) {
        jsonObject.add("limit", context.serialize(src.limit.toString()));
      }
      if (src.offset != null) {
        jsonObject.add("offset", context.serialize(src.offset.toString()));
      }
      if (src.collation != null) {
        jsonObject.add("collation", context.serialize(src.collation));
      }
      if (src.partitionKeys != null) {
        jsonObject.add("partitionKeys",
            context.serialize(src.partitionKeys.asList().toArray()));
      }

      if (src.distribution instanceof RelDistribution) {
        jsonObject.add("distribution",
            context.serialize(src.distribution, RelDistribution.class));
      } else {
        jsonObject.add("relDistributionImpl", context.serialize(
            TvrJsonUtils.serializeRelDistributionImpl(src.distribution)));
      }

      if (src.rowNumberColIndex != null) {
        jsonObject.add("rowNumberColIndex",
            context.serialize(String.valueOf(src.rowNumberColIndex)));
      }

      int forwardMappingSize = (int) src.forwardMapping.keySet().stream()
          .flatMap(ImmutableIntList::stream).distinct().count();
      jsonObject
          .add("fieldCount", context.serialize(forwardMappingSize));
      return jsonObject;
    }
  }
}
