package org.apache.calcite.rel.tvr.trait;

import com.google.gson.*;
import org.apache.calcite.plan.tvr.TvrSetSnapshot;
import org.apache.calcite.plan.tvr.TvrVersion;

import java.lang.reflect.Type;

public class TvrSetSnapshotSerde implements JsonDeserializer<TvrSetSnapshot>,
    JsonSerializer<TvrSetSnapshot> {

  public static TvrSetSnapshotSerde INSTANCE = new TvrSetSnapshotSerde();

  @Override
  public TvrSetSnapshot deserialize(JsonElement json, Type typeOfT,
      JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObject = json.getAsJsonObject();
    TvrVersion toVersion = context.deserialize(jsonObject.get("toVersion"),
        TvrVersion.class);
    return new TvrSetSnapshot(toVersion);
  }

  @Override
  public JsonElement serialize(TvrSetSnapshot src, Type typeOfSrc,
      JsonSerializationContext context) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.add("toVersion", context.serialize(src.toVersion, TvrVersion.class));
    return jsonObject;
  }
}
