package org.apache.calcite.rel.tvr.trait.property;

import com.google.gson.*;
import org.apache.calcite.plan.volcano.TvrProperty;

import java.lang.reflect.Type;

public class TvrStreamingPropertyQP extends TvrProperty {

  public static TvrStreamingPropertyQP INSTANCE = new TvrStreamingPropertyQP();

  private TvrStreamingPropertyQP() {
  }

  @Override
  public String toString() {
    return "QP";
  }

  @Override
  public int hashCode() {
    return "TvrStreamingPropertyQP".hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof TvrStreamingPropertyQP;
  }

  public static class TvrStreamingPropertyQPSerde
      implements JsonDeserializer<TvrStreamingPropertyQP>,
      JsonSerializer<TvrStreamingPropertyQP> {

    public static TvrStreamingPropertyQPSerde INSTANCE =
        new TvrStreamingPropertyQPSerde();

    @Override
    public TvrStreamingPropertyQP deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      return TvrStreamingPropertyQP.INSTANCE;
    }

    @Override
    public JsonElement serialize(TvrStreamingPropertyQP src, Type typeOfSrc,
        JsonSerializationContext context) {
      return new JsonObject();
    }
  }

}
