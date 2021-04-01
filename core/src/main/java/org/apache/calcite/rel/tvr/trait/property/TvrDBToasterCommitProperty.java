package org.apache.calcite.rel.tvr.trait.property;

import com.google.gson.*;
import org.apache.calcite.plan.volcano.TvrProperty;

import java.lang.reflect.Type;

public class TvrDBToasterCommitProperty extends TvrProperty {

  public static TvrDBToasterCommitProperty INSTANCE =
      new TvrDBToasterCommitProperty();

  private TvrDBToasterCommitProperty() {
  }

  public static TvrDBToasterCommitProperty get() {
    return INSTANCE;
  }

  @Override
  public String toString() {
    return "DBToasterCommit";
  }

  @Override
  public int hashCode() {
    return 97;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof TvrDBToasterCommitProperty;
  }

  public static class TvrDBToasterCommitPropertySerde
      implements JsonDeserializer<TvrDBToasterCommitProperty>,
      JsonSerializer<TvrDBToasterCommitProperty> {

    public static TvrDBToasterCommitPropertySerde INSTANCE =
        new TvrDBToasterCommitPropertySerde();

    @Override
    public TvrDBToasterCommitProperty deserialize(JsonElement json,
        Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      return TvrDBToasterCommitProperty.INSTANCE;
    }

    @Override
    public JsonElement serialize(TvrDBToasterCommitProperty src, Type typeOfSrc,
        JsonSerializationContext context) {
      return new JsonObject();
    }
  }

}
