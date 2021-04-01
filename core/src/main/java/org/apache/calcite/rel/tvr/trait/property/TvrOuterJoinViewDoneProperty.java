package org.apache.calcite.rel.tvr.trait.property;

import com.google.gson.*;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.TvrProperty;
import org.apache.calcite.util.VersionInterval;

import java.lang.reflect.Type;
import java.util.Objects;

/**
 * A done message, meaning TvrOuterJoinViewRule has already fired on the RelNode
 * in this RelSet.
 */
public class TvrOuterJoinViewDoneProperty extends TvrProperty {

  private VersionInterval delta;

  public TvrOuterJoinViewDoneProperty(VersionInterval delta) {
    this.delta = delta;
  }

  @Override
  public String toString() {
    return "OJVDone:" + delta;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(delta);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TvrOuterJoinViewDoneProperty)) {
      return false;
    }
    TvrOuterJoinViewDoneProperty o = (TvrOuterJoinViewDoneProperty) other;
    return Objects.equals(delta, o.delta);
  }

  public static class TvrOuterJoinViewDonePropertySerde
      implements JsonDeserializer<TvrOuterJoinViewDoneProperty>,
      JsonSerializer<TvrOuterJoinViewDoneProperty> {

    public static TvrOuterJoinViewDonePropertySerde INSTANCE =
        new TvrOuterJoinViewDonePropertySerde();

    @Override
    public TvrOuterJoinViewDoneProperty deserialize(JsonElement json,
        Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      TvrVersion fromVersion = context.deserialize(jsonObject.get("fromVersion"), TvrVersion.class);
      TvrVersion toVersion = context.deserialize(jsonObject.get("toVersion"), TvrVersion.class);
      return new TvrOuterJoinViewDoneProperty(VersionInterval.of(fromVersion, toVersion));
    }

    @Override
    public JsonElement serialize(TvrOuterJoinViewDoneProperty src,
        Type typeOfSrc, JsonSerializationContext context) {
      JsonObject jsonObject = new JsonObject();
      jsonObject.add("fromVersion", context.serialize(src.delta.from, TvrVersion.class));
      jsonObject.add("toVersion", context.serialize(src.delta.to, TvrVersion.class));
      return jsonObject;
    }
  }

}
