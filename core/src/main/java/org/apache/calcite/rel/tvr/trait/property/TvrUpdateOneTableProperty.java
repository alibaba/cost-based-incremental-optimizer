package org.apache.calcite.rel.tvr.trait.property;

import com.google.gson.*;
import org.apache.calcite.plan.volcano.TvrProperty;

import java.lang.reflect.Type;
import java.util.Objects;

public class TvrUpdateOneTableProperty extends TvrProperty {

  public enum PropertyType {
    DB_TOASTER, OJV,
  }

  private int changingTable;

  // Which algorithm this property is used for
  private PropertyType type;

  public TvrUpdateOneTableProperty(int changingTable, PropertyType type) {
    this.changingTable = changingTable;
    this.type = type;
  }

  public int getChangingTable() {
    return changingTable;
  }

  public PropertyType getType() {
    return type;
  }

  @Override
  public String toString() {
    return changingTable + ":" + type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(changingTable, type);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TvrUpdateOneTableProperty)) {
      return false;
    }
    TvrUpdateOneTableProperty o = (TvrUpdateOneTableProperty) other;
    return Objects.equals(changingTable, o.changingTable) && Objects
        .equals(type, o.type);
  }

  public static class TvrUpdateOneTablePropertySerde
      implements JsonDeserializer<TvrUpdateOneTableProperty>,
      JsonSerializer<TvrUpdateOneTableProperty> {

    public static TvrUpdateOneTablePropertySerde INSTANCE =
        new TvrUpdateOneTablePropertySerde();

    @Override
    public TvrUpdateOneTableProperty deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      int changingTable = jsonObject.get("changingTable").getAsInt();
      String propertyType = jsonObject.get("propertyType").getAsString();
      return new TvrUpdateOneTableProperty(changingTable,
          PropertyType.valueOf(propertyType));
    }

    @Override
    public JsonElement serialize(TvrUpdateOneTableProperty src, Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty("changingTable", src.changingTable);
      jsonObject.addProperty("propertyType", src.type.name());
      return jsonObject;
    }
  }

}
