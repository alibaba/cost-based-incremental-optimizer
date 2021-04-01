package org.apache.calcite.rel.tvr.trait;

import com.google.gson.*;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.trait.transformer.TvrSemanticsTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrSortTransformer;
import org.apache.calcite.rel.tvr.utils.TvrJsonUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;

public class TvrValueDelta extends TvrValueSemantics {
  // whether this delta can cause deletion of tuples from the corresponding snapshot
  private final boolean isPositiveOnly;

  public TvrValueDelta(
      TvrVersion fromVersion,
      TvrVersion toVersion,
      TvrSemanticsTransformer transformer,
      boolean isPositiveOnlyValueDelta) {
    super(fromVersion, toVersion, transformer);
    this.isPositiveOnly = isPositiveOnlyValueDelta;
  }

  @Override
  public String toString() {
    return "ValueDelta" + (isPositiveOnly ? "+" : "") + "@(" + fromVersion
        + ", " + toVersion + ")";
  }

  public boolean isPositiveOnly() {
    return isPositiveOnly;
  }

  public boolean hasWindow() {
    return transformer.anyMatch(
        t -> t instanceof TvrSortTransformer && ((TvrSortTransformer) t)
            .isWindow());
  }

  public List<RelNode> consolidate(List<RelNode> inputs) {
    return transformer.consolidate(inputs);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    return o instanceof TvrValueDelta
        && isPositiveOnly == ((TvrValueDelta) o).isPositiveOnly && super
        .equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), isPositiveOnly);
  }

  @Override
  public TvrValueDelta copy(TvrVersion from, TvrVersion to) {
    return new TvrValueDelta(from, to, transformer, isPositiveOnly);
  }

  public static class TvrValueDeltaSerde
      implements JsonDeserializer<TvrValueDelta>, JsonSerializer<TvrValueDelta>,
      TvrJsonUtils.ColumnOrderAgnostic {

    protected RelDataType rowType;
    protected RelDataTypeFactory typeFactory;

    public TvrValueDeltaSerde(RelDataType rowType,
        RelDataTypeFactory typeFactory) {
      this.rowType = rowType;
      this.typeFactory = typeFactory;
    }

    @Override
    public TvrValueDelta deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      TvrVersion fromVersion = context.deserialize(jsonObject.get("fromVersion"), TvrVersion.class);
      TvrVersion toVersion = context.deserialize(jsonObject.get("toVersion"), TvrVersion.class);
      boolean isPositive = jsonObject.get("isPositiveOnly").getAsBoolean();
      TvrSemanticsTransformer transformer = context
          .deserialize(jsonObject.get("transformer"),
              TvrSemanticsTransformer.class);
      return new TvrValueDelta(fromVersion, toVersion, transformer, isPositive);
    }

    @Override
    public JsonElement serialize(TvrValueDelta src, Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject jsonObject = new JsonObject();

      jsonObject.add("fromVersion", context.serialize(src.fromVersion, TvrVersion.class));
      jsonObject.add("toVersion", context.serialize(src.toVersion, TvrVersion.class));
      jsonObject.addProperty("isPositiveOnly", src.isPositiveOnly());
      jsonObject.add("transformer",
          context.serialize(src.transformer, TvrSemanticsTransformer.class));
      return jsonObject;
    }
  }
}
