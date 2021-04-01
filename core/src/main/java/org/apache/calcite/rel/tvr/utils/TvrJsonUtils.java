package org.apache.calcite.rel.tvr.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrSetSnapshot;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.TvrProperty;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.trait.TvrSetSnapshotSerde;
import org.apache.calcite.rel.tvr.trait.TvrValueDelta;
import org.apache.calcite.rel.tvr.trait.property.*;
import org.apache.calcite.rel.tvr.trait.transformer.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.RuntimeTypeAdapterFactory;

import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;

public class TvrJsonUtils {
  public static Gson createTvrGson(RelDataType rowType,
      RelDataTypeFactory typeFactory) {
    RuntimeTypeAdapterFactory tvrSemanticsFactory =
        RuntimeTypeAdapterFactory.of(TvrSemantics.class, "className")
            .registerSubtype(TvrSetSnapshot.class)
            .registerSubtype(TvrSetDelta.class)
            .registerSubtype(TvrValueDelta.class);
    RuntimeTypeAdapterFactory tvrPropertiesFactory;
    tvrPropertiesFactory = RuntimeTypeAdapterFactory.of(TvrProperty.class, "className")
        .registerSubtype(TvrStreamingPropertyQP.class)
        .registerSubtype(TvrStreamingPropertyQN.class)
        .registerSubtype(TvrDBToasterCommitProperty.class)
        .registerSubtype(TvrUpdateOneTableProperty.class)
        .registerSubtype(TvrOuterJoinViewProperty.class)
        .registerSubtype(TvrOuterJoinViewDoneProperty.class);
//    RuntimeTypeAdapterFactory distributionCollationFactory =
//        RuntimeTypeAdapterFactory.of(RelDistributionBase.class, "className")
//            .registerSubtype(RelDistributionBase.class)
//            .registerSubtype(RelHashDistribution.class)
//            .registerSubtype(RelRangeDistribution.class);
    RuntimeTypeAdapterFactory transformerFactory = RuntimeTypeAdapterFactory
        .of(TvrSemanticsTransformer.class, "className")
        .registerSubtype(TvrProjectTransformer.class)
        .registerSubtype(TvrAggregateTransformer.class)
        .registerSubtype(TvrSortTransformer.class)
        .registerSubtype(TvrCompositeTransformer.class);
    return new GsonBuilder().registerTypeAdapter(TvrSetSnapshot.class,
        TvrSetSnapshotSerde.INSTANCE)
        .registerTypeAdapter(TvrSetDelta.class,
            TvrSetDelta.TvrSetDeltaSerde.INSTANCE)
//        .registerTypeHierarchyAdapter(RelDataType.class,
//            new RelDataTypeSerde(typeFactory))
        .registerTypeAdapter(RexLiteral.class,
            RexLiteralSerde.INSTANCE)
//        .registerTypeAdapter(AggregateCall.class,
//            new OdpsAggregateCallSerde(rowType))
        .registerTypeAdapter(TvrValueDelta.class,
            new TvrValueDelta.TvrValueDeltaSerde(rowType, typeFactory))
        .registerTypeAdapter(TvrStreamingPropertyQP.class,
            TvrStreamingPropertyQP.TvrStreamingPropertyQPSerde.INSTANCE)
        .registerTypeAdapter(TvrStreamingPropertyQN.class,
            TvrStreamingPropertyQN.TvrStreamingPropertyQNSerde.INSTANCE)
        .registerTypeAdapter(TvrDBToasterCommitProperty.class,
            TvrDBToasterCommitProperty.TvrDBToasterCommitPropertySerde.INSTANCE)
        .registerTypeAdapter(TvrUpdateOneTableProperty.class,
            TvrUpdateOneTableProperty.TvrUpdateOneTablePropertySerde.INSTANCE)
        .registerTypeAdapter(TvrOuterJoinViewProperty.class,
            TvrOuterJoinViewProperty.TvrOuterJoinViewPropertySerde.INSTANCE)
        .registerTypeAdapter(TvrOuterJoinViewDoneProperty.class,
            TvrOuterJoinViewDoneProperty.TvrOuterJoinViewDonePropertySerde.INSTANCE)
//        .registerTypeAdapter(RelDistributionBase.class,
//            RelDistributionBaseSerde.INSTANCE)
//        .registerTypeAdapter(RelHashDistribution.class,
//            new RelHashDistributionSerde(rowType))
//        .registerTypeAdapter(RelRangeDistribution.class,
//            RelRangeDistributionSerde.INSTANCE)
//        .registerTypeAdapter(OdpsRelFieldCollation.class,
//            new OdpsRelFieldCollationSerde(rowType))
//        .registerTypeAdapter(OdpsRelCollationImpl.class,
//            OdpsRelCollationImplSerde.INSTANCE)
        .registerTypeAdapter(TvrAggregateTransformer.class,
            new TvrAggregateTransformer.TvrAggregateTransformerSerde(rowType,
                typeFactory))
        .registerTypeAdapter(TvrCompositeTransformer.class,
            new TvrCompositeTransformer.TvrCompositeTransformerSerde(rowType,
                typeFactory))
        .registerTypeAdapter(TvrProjectTransformer.class,
            new TvrProjectTransformer.TvrProjectTransformerSerde(rowType,
                typeFactory))
        .registerTypeAdapter(TvrSortTransformer.class,
            TvrSortTransformer.TvrSortTransformerSerde.INSTANCE)
        .registerTypeAdapter(TvrVersion.class, TvrVersionSerde.INSTANCE)
        .registerTypeAdapterFactory(tvrSemanticsFactory)
        .registerTypeAdapterFactory(tvrPropertiesFactory)
//        .registerTypeAdapterFactory(distributionCollationFactory)
        .registerTypeAdapterFactory(transformerFactory)
        .create();
  }

  public interface ColumnOrderAgnostic {

    default List<Integer> c2i(String[] cols, RelDataType rowType) {
      List<String> fieldNames = rowType.getFieldNames();
      return Arrays.stream(cols).map(fieldNames::indexOf)
          .collect(Collectors.toList());
    }

    default String[] i2c(List<Integer> indices, RelDataType rowType) {
      return indices.stream().map(i -> rowType.getFieldList().get(i).getName())
          .toArray(String[]::new);
    }

  }

//  public static class RelDistributionBaseSerde
//      implements JsonSerializer<RelDistributionBase>,
//      JsonDeserializer<RelDistributionBase>, ColumnOrderAgnostic {
//
//    public static RelDistributionBaseSerde INSTANCE =
//        new RelDistributionBaseSerde();
//
//    @Override
//    public RelDistributionBase deserialize(JsonElement json,
//        Type type, JsonDeserializationContext context)
//        throws JsonParseException {
//      JsonObject jsonObject = json.getAsJsonObject();
//
//      RelDistribution.Type distributionType = RelDistribution.Type
//          .valueOf(context.deserialize(jsonObject.get("type"), String.class));
//      switch (distributionType) {
//      case SINGLETON:
//        return (RelDistributionBase) OdpsRelDistributions.SINGLETON;
//      case BROADCAST_DISTRIBUTED:
//        return (RelDistributionBase) OdpsRelDistributions.BROADCAST_DISTRIBUTED;
//      default:
//        throw new UnsupportedOperationException();
//      }
//    }
//
//    @Override
//    public JsonElement serialize(RelDistributionBase relDistributionBase,
//        Type type, JsonSerializationContext context) {
//      JsonObject jsonObject = new JsonObject();
//
//      jsonObject.add("type", context.serialize(relDistributionBase.getType()));
//      return jsonObject;
//    }
//  }
//
//  public static class RelHashDistributionSerde
//      implements JsonSerializer<RelHashDistribution>,
//      JsonDeserializer<RelHashDistribution>, ColumnOrderAgnostic {
//
//    private RelDataType rowType;
//
//    public RelHashDistributionSerde(RelDataType rowType) {
//      this.rowType = rowType;
//    }
//
//    @Override
//    public RelHashDistribution deserialize(JsonElement json,
//        Type type, JsonDeserializationContext context)
//        throws JsonParseException {
//      JsonObject jsonObject = json.getAsJsonObject();
//
//      String hasher =
//          context.deserialize(jsonObject.get("hasher"), String.class);
//      int bucketNum =
//          context.deserialize(jsonObject.get("bucketNum"), Integer.class);
//      List<Integer> keys =
//          c2i(context.deserialize(jsonObject.get("keys"), String[].class),
//              rowType);
//      Boolean isJoinHasher =
//          context.deserialize(jsonObject.get("isJoinHasher"), Boolean.class);
//
//      List<ImmutableBitSet> equivalentBitSets = new ArrayList<>();
//      JsonArray jsonArray =
//          jsonObject.get("equivalentBitSets").getAsJsonArray();
//      jsonArray.forEach(j -> equivalentBitSets
//          .add(context.deserialize(j, ImmutableBitSet.class)));
//
//      return new RelHashDistribution(ImmutableIntList.copyOf(keys), bucketNum,
//          isJoinHasher, ImmutableList.copyOf(equivalentBitSets), hasher);
//    }
//
//    @Override
//    public JsonElement serialize(RelHashDistribution relHashDistribution,
//        Type type, JsonSerializationContext context) {
//      JsonObject jsonObject = new JsonObject();
//
//      jsonObject
//          .add("hasher", context.serialize(relHashDistribution.getHasher()));
//      jsonObject.add("bucketNum",
//          context.serialize(relHashDistribution.getBucketNum()));
//      jsonObject.add("keys",
//          context.serialize(i2c(relHashDistribution.getKeys(), rowType)));
//      jsonObject.add("isJoinHasher",
//          context.serialize(relHashDistribution.isJoinHasher()));
//      jsonObject.add("equivalentBitSets",
//          context.serialize(relHashDistribution.getEquivalents()));
//
//      return jsonObject;
//    }
//  }
//
//  public static class RelRangeDistributionSerde
//      implements JsonSerializer<RelRangeDistribution>,
//      JsonDeserializer<RelRangeDistribution>, ColumnOrderAgnostic {
//
//    public static RelRangeDistributionSerde INSTANCE =
//        new RelRangeDistributionSerde();
//
//    @Override
//    public RelRangeDistribution deserialize(JsonElement json,
//        Type type, JsonDeserializationContext context)
//        throws JsonParseException {
//      JsonObject jsonObject = json.getAsJsonObject();
//
//      List<ClusteredByDesc.KeyRange> boundaryList = context
//          .deserialize(jsonObject.get("boundaryList"),
//              new TypeToken<List<ClusteredByDesc.KeyRange>>() {
//              }.getType());
//      Integer bucketNum =
//          context.deserialize(jsonObject.get("bucketNum"), Integer.class);
//      Boolean isRequired =
//          context.deserialize(jsonObject.get("isRequired"), Boolean.class);
//
//      List<List<Integer>> equivalentBitSets = context
//          .deserialize(jsonObject.get("equivalentBitSets"),
//              new TypeToken<List<List<Integer>>>() {
//              }.getType());
//      ImmutableList.Builder<ImmutableBitSet> builder = ImmutableList.builder();
//      equivalentBitSets.forEach(l -> builder.add(ImmutableBitSet.of(l)));
//
//      RelCollation collation = context
//          .deserialize(jsonObject.get("collation"), OdpsRelCollationImpl.class);
//
//      return new RelRangeDistribution(collation,
//          ImmutableList.copyOf(boundaryList), builder.build(), bucketNum,
//          isRequired);
//    }
//
//    @Override
//    public JsonElement serialize(RelRangeDistribution relRangeDistribution,
//        Type type, JsonSerializationContext context) {
//      JsonObject jsonObject = new JsonObject();
//
//      jsonObject.add("boundaryList",
//          context.serialize(relRangeDistribution.getBoundaryList()));
//      jsonObject.add("bucketNum",
//          context.serialize(relRangeDistribution.getBucketNum()));
//      jsonObject.add("isRequired",
//          context.serialize(relRangeDistribution.isRequired()));
//      jsonObject.add("collation",
//          context.serialize(relRangeDistribution.getCollation()));
//      List<List<Integer>> equivalentBitLists =
//          relRangeDistribution.getEquivalents().stream()
//              .map(ImmutableBitSet::toList).collect(Collectors.toList());
//      jsonObject
//          .add("equivalentBitSets", context.serialize(equivalentBitLists));
//
//      return jsonObject;
//    }
//  }
//
//  public static class OdpsRelFieldCollationSerde
//      implements JsonSerializer<OdpsRelFieldCollation>,
//      JsonDeserializer<OdpsRelFieldCollation>, ColumnOrderAgnostic {
//
//    private RelDataType rowType;
//
//    public OdpsRelFieldCollationSerde(RelDataType rowType) {
//      this.rowType = rowType;
//    }
//
//    @Override
//    public OdpsRelFieldCollation deserialize(JsonElement json, Type type,
//        JsonDeserializationContext context) throws JsonParseException {
//      JsonObject jsonObject = json.getAsJsonObject();
//      List<Integer> fieldIndex =
//          c2i(context.deserialize(jsonObject.get("fieldIndex"), String[].class),
//              rowType);
//      RelFieldCollation.Direction direction = context
//          .deserialize(jsonObject.get("direction"),
//              RelFieldCollation.Direction.class);
//      RelFieldCollation.NullDirection nullDirection = context
//          .deserialize(jsonObject.get("nullDirection"),
//              RelFieldCollation.NullDirection.class);
//
//      return new OdpsRelFieldCollation(fieldIndex.get(0), direction,
//          nullDirection);
//    }
//
//    @Override
//    public JsonElement serialize(OdpsRelFieldCollation odpsRelFieldCollation,
//        Type type, JsonSerializationContext context) {
//      JsonObject jsonObject = new JsonObject();
//      jsonObject.add("fieldIndex", context.serialize(
//          i2c(ImmutableIntList.of(odpsRelFieldCollation.getFieldIndex()),
//              rowType)));
//      jsonObject.add("direction",
//          context.serialize(odpsRelFieldCollation.getDirection()));
//      jsonObject.add("nullDirection",
//          context.serialize(odpsRelFieldCollation.nullDirection));
//
//      return jsonObject;
//    }
//  }
//
  public static class RelDataTypeSerde
      implements JsonDeserializer<RelDataType>, JsonSerializer<RelDataType> {

    private RelDataTypeFactory typeFactory;

    public RelDataTypeSerde(RelDataTypeFactory typeFactory) {
      this.typeFactory = typeFactory;
    }

    @Override
    public RelDataType deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      String byteString = context.deserialize(json, String.class);
//        TypeInfoProtos.TypeInfo type =
//            TypeInfoProtos.TypeInfo.parseFrom(Base64.getDecoder().decode(byteString));
      return convertType( byteString, typeFactory);
    }

    @Override
    public JsonElement serialize(RelDataType src, Type typeOfSrc,
        JsonSerializationContext context) {
      String type = convertType2(src);
      return context
          .serialize(Base64.getEncoder().encodeToString(type.getBytes()));
    }
  }


  public static String convertType2(RelDataType type) {
    throw new UnsupportedOperationException("TvrJsonUtils convertType2 not supported");
  }

  public static RelDataType convertType(
          String type2,
          RelDataTypeFactory factory) {
    throw new UnsupportedOperationException("TvrJsonUtils convertType not supported");
  }


  public static class RexLiteralSerde
      implements JsonDeserializer<RexLiteral>, JsonSerializer<RexLiteral> {

    public static RexLiteralSerde INSTANCE = new RexLiteralSerde();

    private RexLiteralSerde() {
    }

    @Override
    public RexLiteral deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      Comparable value =
          context.deserialize(jsonObject.get("value"), Comparable.class);
      RelDataType type =
          context.deserialize(jsonObject.get("type"), RelDataType.class);
      SqlTypeName sqlTypeName = SqlTypeName.get(
          context.deserialize(jsonObject.get("sqlTypeName"), String.class));
      return new RexLiteral(value, type, sqlTypeName);
    }

    @Override
    public JsonElement serialize(RexLiteral src, Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject json = new JsonObject();
      json.add("value", context.serialize(src.getValue()));
      json.add("type", context.serialize(src.getType()));
      json.add("sqlTypeName", context.serialize(src.getTypeName().getName()));
      return json;
    }
  }

//  public static class OdpsAggregateCallSerde
//      implements JsonDeserializer<OdpsAggregateCall>,
//      JsonSerializer<OdpsAggregateCall>, ColumnOrderAgnostic {
//
//    private static Type constantsMapType =
//        new TypeToken<Map<Integer, RexLiteral>>() {
//        }.getType();
//    private static Type argTypesType = new TypeToken<List<RelDataType>>() {
//    }.getType();
//
//    private RelDataType rowType;
//
//    public OdpsAggregateCallSerde(RelDataType rowType) {
//      this.rowType = rowType;
//    }
//
//    @Override
//    public OdpsAggregateCall deserialize(JsonElement json, Type typeOfT,
//        JsonDeserializationContext context) throws JsonParseException {
//      JsonObject jsonObject = json.getAsJsonObject();
//
//      OdpsSqlAggFunction aggFunction = (OdpsSqlAggFunction) funcRegistry
//          .get(jsonObject.get("aggFunction").getAsString());
//      boolean distinct = jsonObject.get("distinct").getAsBoolean();
//      List<Integer> argList =
//          c2i(context.deserialize(jsonObject.get("argList"), String[].class),
//              rowType);
//      RelDataType type =
//          context.deserialize(jsonObject.get("type"), RelDataType.class);
//      RelDataType partialType =
//          context.deserialize(jsonObject.get("partialType"), RelDataType.class);
//      String name = jsonObject.get("name").getAsString();
//      OdpsAggregateCall.Stage
//          stage = OdpsAggregateCall.Stage.valueOf(jsonObject.get("stage").getAsString());
//      Map<Integer, RexLiteral> constantsMap =
//          context.deserialize(jsonObject.get("constantsMap"), constantsMapType);
//      List<RelDataType> argTypes =
//          context.deserialize(jsonObject.get("argTypes"), argTypesType);
//
//      return OdpsAggregateCall
//          .create(aggFunction, distinct, argList, type, partialType, name,
//              stage, constantsMap, argTypes);
//    }
//
//    @Override
//    public JsonElement serialize(OdpsAggregateCall aggCall, Type typeOfSrc,
//        JsonSerializationContext context) {
//      JsonObject json = new JsonObject();
//      String aggFunction = aggCall.getAggregation().getName();
//      json.addProperty("aggFunction", aggFunction);
//      json.addProperty("distinct", aggCall.isDistinct());
//      json.add("argList",
//          context.serialize(i2c(aggCall.getArgList(), rowType)));
//      json.add("type", context.serialize(aggCall.getType()));
//      json.add("partialType", context.serialize(aggCall.getPartialType()));
//      json.addProperty("name", aggCall.getName());
//      json.addProperty("stage", aggCall.getStage().toString());
//      json.add("constantsMap", context.serialize(aggCall.getConstantsMap()));
//      json.add("argTypes", context.serialize(aggCall.getArgTypes()));
//
//      return json;
//    }
//
//    private static Map<String, SqlAggFunction> funcRegistry =
//        new ImmutableMap.Builder<String, SqlAggFunction>().put("SUM", SUM)
//            .put("COUNT", COUNT).put("MIN", MIN).put("MAX", MAX)
//            .put("MEDIAN", MEDIAN).put("AVG", AVG).put("STDDEV", STDDEV)
//            .put("STD", STD).put("STDDEV_SAMP", STDDEV_SAMP)
//            .put("STDDEV_POP", STDDEV_POP).put("VARIANCE", VARIANCE)
//            .put("VAR_SAMP", VAR_SAMP).put("PERCENTILE", PERCENTILE)
//            .put("PERCENTILE_APPROX", PERCENTILE_APPROX).put("CORR", CORR)
//            .put("COVARIANCE", COVARIANCE).put("CORVAR_SAMP", CORVAR_SAMP)
//            .put("COLLECT_SET", COLLECT_SET).put("COLLECT_LIST", COLLECT_LIST)
//            .put("WM_CONCAT", WM_CONCAT).put("COMPUTE_STATS", COMPUTE_STATS)
//            .put("WF_AVG", WF_AVG).put("WF_COUNT", WF_COUNT)
//            .put("WF_SUM", WF_SUM).put("WF_MAX", WF_MAX).put("WF_MIN", WF_MIN)
//            .put("WF_MEDIAN", WF_MEDIAN).put("WF_STDDEV", WF_STDDEV)
//            .put("WF_STDDEV_POP", WF_STDDEV_POP)
//            .put("WF_STDDEV_SAMP", WF_STDDEV_SAMP).put("WF_CORR", WF_CORR)
//            .put("WF_COVARIANCE", WF_COVARIANCE)
//            .put("WF_COVAR_SAMP", WF_COVAR_SAMP).put("WF_VARIANCE", WF_VARIANCE)
//            .put("WF_VAR_SAMP", WF_VAR_SAMP).put("WF_PERCENTILE", WF_PERCENTILE)
//            .put("WF_PERCENTILE_APPROX", WF_PERCENTILE_APPROX).put("LAG", LAG)
//            .put("LEAD", LEAD).put("CLUSTER_SAMPLE", CLUSTER_SAMPLE)
//            .put("CUME_DIST", CUME_DIST).put("PERCENT_RANK", PERCENT_RANK)
//            .put("NTILE", NTILE).put("FIRST_VALUE", FIRST_VALUE)
//            .put("LAST_VALUE", LAST_VALUE).put("NTH_VALUE", NTH_VALUE).build();
//  }

  public static RelDataType deserializeRowType(RelDataTypeFactory typeFactory,
      String rowTypeStr) {
    Gson gson = new GsonBuilder().registerTypeAdapter(RelDataType.class,
        new RelDataTypeSerde(typeFactory)).create();
    return gson.fromJson(rowTypeStr, RelDataType.class);
  }

  public static String serializeRowType(RelDataTypeFactory typeFactory,
      RelDataType rowType) {
    Gson gson = new GsonBuilder().registerTypeAdapter(RelDataType.class,
        new RelDataTypeSerde(typeFactory)).create();
    return gson.toJson(rowType, RelDataType.class);
  }

  public static String serializeRelDistributionImpl(
      RelDistribution distribution) {
    return distribution.getType().shortName;
  }

  public static RelDistribution deserializeRelDistributionImpl(
      String distributionStr) {
    switch (distributionStr) {
    case "any":
      return RelDistributions.ANY;
    case "single":
      return RelDistributions.SINGLETON;
    case "broadcast":
      return RelDistributions.BROADCAST_DISTRIBUTED;
    case "rr":
      return RelDistributions.ROUND_ROBIN_DISTRIBUTED;
    case "random":
      return RelDistributions.RANDOM_DISTRIBUTED;
    default:
      throw new UnsupportedOperationException();
    }
  }

}

//class OdpsRelCollationImplSerde implements JsonSerializer<OdpsRelCollationImpl>,
//    JsonDeserializer<OdpsRelCollationImpl> {
//
//  public static OdpsRelCollationImplSerde INSTANCE = new OdpsRelCollationImplSerde();
//
//  @Override
//  public OdpsRelCollationImpl deserialize(JsonElement json, Type typeOfT,
//      JsonDeserializationContext context) throws JsonParseException {
//    JsonObject jsonObject = json.getAsJsonObject();
//
//    List<OdpsRelFieldCollation> collations = new ArrayList<>();
//    jsonObject.get("collations").getAsJsonArray().forEach(
//        collation -> collations.add(
//            context.deserialize(collation, OdpsRelFieldCollation.class)));
//
//    List<ImmutableBitSet> equivalentBitSets = new ArrayList<>();
//    JsonArray jsonArray =
//        jsonObject.get("equivalentBitSets").getAsJsonArray();
//    jsonArray.forEach(j -> equivalentBitSets
//        .add(context.deserialize(j, ImmutableBitSet.class)));
//    return new OdpsRelCollationImpl(collations, equivalentBitSets);
//  }
//
//  @Override
//  public JsonElement serialize(OdpsRelCollationImpl collation, Type typeOfSrc,
//      JsonSerializationContext context) {
//    JsonObject jsonObject = new JsonObject();
//
//    jsonObject
//        .add("collations", context.serialize(collation.getFieldCollations()));
//    jsonObject.add("equivalentBitSets",
//        context.serialize(collation.getEquivalents()));
//
//    return jsonObject;
//  }
//}

class TvrVersionSerde implements JsonDeserializer<TvrVersion>, JsonSerializer<TvrVersion> {

  public static final TvrVersionSerde INSTANCE = new TvrVersionSerde();

  @Override
  public TvrVersion deserialize(JsonElement jsonElement, Type type,
      JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObject = jsonElement.getAsJsonObject();
    long[] versions =
        context.deserialize(jsonObject.get("version"), long[].class);
    return TvrVersion.of(versions);
  }

  @Override
  public JsonElement serialize(TvrVersion tvrVersion, Type type,
      JsonSerializationContext context) {
    long[] versions = tvrVersion.getVersions();
    JsonObject jsonObject = new JsonObject();
    jsonObject.add("version", context.serialize(versions));
    return jsonObject;
  }
}
