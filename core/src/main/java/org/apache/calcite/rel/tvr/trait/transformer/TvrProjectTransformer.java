package org.apache.calcite.rel.tvr.trait.transformer;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.gson.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.trait.transformer.predicate.TransformerPredicate;
import org.apache.calcite.rel.tvr.utils.ProjectBuilder;
import org.apache.calcite.rel.tvr.utils.TvrJsonUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableIntList;

import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Sample implementation for project transformer (for value semantics).
 */
public class TvrProjectTransformer extends TvrSemanticsTransformer {

  private final ImmutableIntList projectRefs;

  public TvrProjectTransformer(ImmutableIntList projectRefs) {
    this.projectRefs = projectRefs;

    IntStream.range(0, projectRefs.size()).forEach(i -> {
      ImmutableIntList ref = ImmutableIntList.of(projectRefs.get(i));
      forwardMapping.put(ref, i);
      backwardMapping.put(i, ref);
    });
  }

  @Override
  public RelDataType deriveRowType(RelDataType inputRowType,
      RelDataTypeFactory factory) {
    RelDataTypeFactory.Builder builder = factory.builder();
    List<RelDataTypeField> fields = inputRowType.getFieldList();

    IntStream.range(0, projectRefs.size()).forEach(i -> {
      RelDataTypeField field = fields.get(projectRefs.get(i));
      builder.add(field.getName(), field.getType());
    });
    return builder.build();
  }

  @Override
  public TvrSemanticsTransformer addNewTransformer(
      TvrSemanticsTransformer newTransformer) {
    if (newTransformer instanceof TvrCompositeTransformer) {
      List<TvrSemanticsTransformer> transformers = Lists.newArrayList();
      List<TvrSemanticsTransformer> innerTrans =
          ((TvrCompositeTransformer) newTransformer).getInnerTransformers();
      TvrSemanticsTransformer firstTrans = innerTrans.get(0);
      if (firstTrans instanceof TvrProjectTransformer) {
        transformers.add(mergeProjectTransformers(firstTrans));
      } else {
        transformers.add(this);
        transformers.add(firstTrans);
      }
      transformers.addAll(innerTrans.subList(1, innerTrans.size()));
      return new TvrCompositeTransformer(transformers);
    } else if (newTransformer instanceof TvrProjectTransformer) {
      return mergeProjectTransformers(newTransformer);
    }

    return new TvrCompositeTransformer(
        Lists.newArrayList(this, newTransformer));
  }

  @Override
  public boolean isCompatible(TransformerPredicate predicate, ImmutableIntList indices) {
    return predicate.test(this, indices);
  }

  private TvrSemanticsTransformer mergeProjectTransformers(
      TvrSemanticsTransformer newTrans) {
    assert newTrans instanceof TvrProjectTransformer;
    Multimap<ImmutableIntList, Integer> oldForwardMapping = forwardMapping;
    Multimap<ImmutableIntList, Integer> newForwardMapping =
        newTrans.forwardMapping;

    Map<Integer, Integer> mergedForwardMapping = new HashMap<>();
    oldForwardMapping.entries().forEach(e -> {
      Collection<Integer> newRefs =
          newForwardMapping.get(ImmutableIntList.of(e.getValue()));
      newRefs.forEach(i -> mergedForwardMapping.put(i, e.getKey().get(0)));
    });

    List<Integer> newProjectRefs = new ArrayList<>();
    IntStream.range(0, mergedForwardMapping.size())
        .forEach(i -> newProjectRefs.add(mergedForwardMapping.get(i)));

    return new TvrProjectTransformer(ImmutableIntList.copyOf(newProjectRefs));
  }

  @Override
  public TvrSemanticsTransformer transform(Multimap<Integer, Integer> mapping) {
    List<Integer> newProjectRefs = projectRefs.stream().map(i ->
        // negative index means this column has been pruned by input node
        getFirstMappingResult(mapping, i)).filter(Objects::nonNull)
        .collect(Collectors.toList());

    // if there are some new columns added by the front transformers,
    // this project transformer should still add them to the row type.
    // if the key in the mapping is negative, it means this column is newly
    // added by other transformers
    mapping.entries().stream().filter(e -> e.getKey() < 0)
        .map(Map.Entry::getValue).forEach(newProjectRefs::add);

    return new TvrProjectTransformer(ImmutableIntList.copyOf(newProjectRefs));
  }

  @Override
  public Set<Integer> getRequiredColumns() {
    return Collections.emptySet();
  }

  @Override
  public List<RelNode> apply(List<RelNode> inputs) {
    assert !inputs.isEmpty();

    return inputs.stream().map(input -> {
      ProjectBuilder builder = ProjectBuilder.anchor(input);
      builder.addAll(projectRefs);
      return builder.build();
    }).collect(Collectors.toList());
  }

  @Override
  public List<RelNode> consolidate(List<RelNode> inputs) {
    return inputs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TvrProjectTransformer)) {
      return false;
    }
    TvrProjectTransformer that = (TvrProjectTransformer) o;
    return Objects.equals(projectRefs, that.projectRefs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(projectRefs);
  }

  public static class TvrProjectTransformerSerde
      implements JsonDeserializer<TvrProjectTransformer>,
      JsonSerializer<TvrProjectTransformer>, TvrJsonUtils.ColumnOrderAgnostic {

    protected RelDataType rowType;
    protected RelDataTypeFactory typeFactory;

    public TvrProjectTransformerSerde(RelDataType rowType,
        RelDataTypeFactory typeFactory) {
      this.rowType = rowType;
      this.typeFactory = typeFactory;
    }

    @Override
    public TvrProjectTransformer deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      ImmutableIntList projectRefs = ImmutableIntList.copyOf(c2i(context
              .deserialize(jsonObject.get("projectRefs"), String[].class),
          rowType));

      return new TvrProjectTransformer(projectRefs);
    }

    @Override
    public JsonElement serialize(TvrProjectTransformer src, Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject jsonObject = new JsonObject();
      jsonObject
          .add("projectRefs", context.serialize(i2c(src.projectRefs, rowType)));
      return jsonObject;
    }
  }
}
