package org.apache.calcite.rel.tvr.trait.property;

import com.google.gson.*;
import org.apache.calcite.plan.volcano.TvrProperty;

import java.lang.reflect.Type;

public class TvrStreamingPropertyQN extends TvrProperty {

  public static TvrStreamingPropertyQN INSTANCE = new TvrStreamingPropertyQN();

  private TvrStreamingPropertyQN() {
  }

  @Override
  public String toString() {
    return "QN";
  }

  @Override
  public int hashCode() {
    return "TvrStreamingPropertyQN".hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof TvrStreamingPropertyQN;
  }

  public static class TvrStreamingPropertyQNSerde
      implements JsonDeserializer<TvrStreamingPropertyQN>,
      JsonSerializer<TvrStreamingPropertyQN> {

    public static TvrStreamingPropertyQNSerde INSTANCE =
        new TvrStreamingPropertyQNSerde();

    @Override
    public TvrStreamingPropertyQN deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      return TvrStreamingPropertyQN.INSTANCE;
    }

    @Override
    public JsonElement serialize(TvrStreamingPropertyQN src, Type typeOfSrc,
        JsonSerializationContext context) {
      return new JsonObject();
    }
  }
}
