//package org.apache.calcite.rel.tvr.utils;
//
//import com.aliyun.odps.lot.cbo.OdpsRelContext;
//import com.google.common.collect.ArrayListMultimap;
//import com.google.common.collect.Multimap;
//import com.google.gson.*;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//
//import java.security.MessageDigest;
//import java.security.NoSuchAlgorithmException;
//import java.util.Collection;
//import java.util.List;
//import java.util.Map;
//
///**
// * Each query has a unique file in Pangu File System to save the mapping between
// * state tables and their digests.
// * 1. Firstly, Each query will be used to calculate a digest to determine the location of the storage file.
// *    (the partitions of its input tables and output tables are not used to compute this digest).
// *    To solve the encoding conflict that different queries may get the same digest,
// *    there will be a folder (named by this digest) in the Pangu File System,
// *    and all the mappings for these conflicting queries are saved in different files in this folder.
// * 2. Each file consists of two parts, one is the digest of a certain query (used to double check
// *    whether the file belongs to the current query), and another is the mapping between
// *    state tables and their digests (The digest is generated from the plan that creates the table,
// *    so that it can be used to verify whether the table is still suitable for the current query).
// * 3. For each table, a digest of the plan generating the table will also be recorded in its property,
// *    and the final verification will be made when a certain plan is replaced with this table.
// *
// *    File System
// *        |
// *      Folder [named by md5(query digest)]
// *      /  |               \
// *  file1 file2          file3
// *              /                    \
// *      query digest (plaintext)  multi-mapping {md5(plan digest) -> table name}
// */
//public class TvrPanguDigestManager {
//  private static final Log LOG = LogFactory.getLog(TvrPanguDigestManager.class);
//
//  // Indicate the active pangu cluster for this optimization.
//  public static final String ODPS_PROGRESSIVE_ACTIVE_PANGU_CLUSTER =
//      "odps.optimizer.progressive.active.pangu.cluster";
//
//  // It is used to indicate the Pangu cluster location,
//  // which is a folder that saves all the state files.
//  public static final String RANGE_QUERY_PANGU_LOCATION =
//      "odps.progressive.range.query.state.location";
//
//  public static final String RANGE_QUERY_TEST_PANGU_LOCATION =
//      "progressive/test/range-query/";
//
//  // The state version indicates whether this table is still valid.
//  // All the state tables share the same state version, if it is not equal
//  // to the required one, all the state tables will be disabled and all the state files
//  // in Pangu will be updated with the newly created tables.
//  public static final String RANGE_QUERY_STATE_VERSION =
//      "odps.progressive.range.query.state.version";
//
//  private static final ThreadLocal<TvrPanguDigestManager> INSTANCE = new ThreadLocal<>();
//
//  // For local test, panguUtil is null.
//  private PanguUtil panguUtil;
//  private String queryDigest;
//  private TableDigestManager tableDigestManager;
//  private String filePath;
//  private String stateVersion;
//  private String dumpStates;
//
//  private TvrPanguDigestManager(PanguUtil panguUtil, String queryDigest,
//      TableDigestManager tableDigestManager, String filePath, String stateVersion) {
//    this.panguUtil = panguUtil;
//    this.queryDigest = queryDigest;
//    this.tableDigestManager = tableDigestManager;
//    this.filePath = filePath;
//    this.stateVersion = stateVersion;
//  }
//
//  public static void set(TvrPanguDigestManager tvrPanguDigestManager) {
//    INSTANCE.set(tvrPanguDigestManager);
//  }
//
//  public static void remove() {
//    INSTANCE.remove();
//  }
//
//  public static void initPanguFileSystem(OdpsRelContext ctx, String queryDigest) {
//    if (!com.aliyun.odps.lot.cbo.utils.TvrUtils.progressiveRangeQueryOptEnabled(ctx)) {
//      throw new IllegalArgumentException("TvrPanguUtils just supports range query optimization now.");
//    }
//
//    String stateVersion = getRangeQueryStateVersion(ctx);
//    if (stateVersion == null) {
//      throw new IllegalArgumentException(
//          "Please set a valid state version (set odps.progressive.range.query.state.version=xx).");
//    }
//
//    String stateLocation = getRangeQueryStateLocation(ctx);
//    if (stateLocation == null) {
//      throw new IllegalArgumentException(
//          "Please set a valid Pangu cluster location (set odps.progressive.range.query.state.location=xx).");
//    }
//
//    String compressedQueryDigest = compressDigest(queryDigest);
//    String folderName = concatePanguPath(getActivePanguCluster(ctx),
//        stateLocation + compressedQueryDigest);
//    LOG.info("Init PanguFileSystem with query digest " + compressedQueryDigest);
//
//    PanguUtil panguUtil = null;
//    String fileName = null;
//    TableDigestManager tableDigestManager = null;
//
//    if (com.aliyun.odps.lot.cbo.utils.TvrUtils.progressiveMetaAvailable(ctx)) {
//      LOG.info("Try to find the expected pangu folder that keeps the names of state tables: " + folderName);
//      panguUtil = PanguUtil.getInstance();
//      if (!panguUtil.fileExists(folderName)) {
//        LOG.info("Path " + folderName + " is not existed, try to create it...");
//        panguUtil.mkdir(folderName);
//      }
//
//      List<String> fileList = panguUtil.listNormalFile(folderName);
//      for (String file : fileList) {
//        tableDigestManager = TableDigestManager
//            .deserialize(panguUtil.readText(file), queryDigest, stateVersion);
//        if (tableDigestManager != null) {
//          LOG.info("Successfully find the state table file:" + file);
//          fileName = file;
//          break;
//        }
//      }
//      if (fileName == null) {
//        fileName = folderName + "/" + fileList.size() + ".json";
//        LOG.info(
//            "There is no available state table for this query, "
//                + "create a new file to save state tables. (Path is " + fileName + ").");
//      }
//    }
//
//    INSTANCE.set(new TvrPanguDigestManager(panguUtil, queryDigest, tableDigestManager,
//        fileName, stateVersion));
//  }
//
//  public static TvrPanguDigestManager getInstance() {
//    TvrPanguDigestManager manager = INSTANCE.get();
//    if (manager == null) {
//      throw new RuntimeException("TvrPanguHelper is not initialized.");
//    }
//    return manager;
//  }
//
//  private static String compressDigest(String queryDigest) {
//    String[] hexArray = {"0","1","2","3","4","5","6","7","8","9","A","B","C","D","E","F"};
//    try {
//      MessageDigest md = MessageDigest.getInstance("MD5");
//      md.update(queryDigest.getBytes());
//      byte[] rawBit = md.digest();
//      StringBuilder outputMD5 = new StringBuilder();
//      for (int i = 0; i < 16; i++) {
//        outputMD5.append(hexArray[rawBit[i] >>> 4 & 0x0f]);
//        outputMD5.append(hexArray[rawBit[i] & 0x0f]);
//      }
//      return outputMD5.toString();
//    } catch (NoSuchAlgorithmException e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  public Collection<String> getStateTableName(String planDigest) {
//    if (tableDigestManager == null) {
//      return null;
//    }
//
//    String compressedPlanDigest = compressDigest(planDigest);
//    LOG.info("Try to find the state table with digest: " + compressedPlanDigest);
//    return tableDigestManager.getTable(compressedPlanDigest);
//  }
//
//  private static String concatePanguPath(String panguUri, String folderName) {
//    return panguUri+ folderName;
//  }
//
//  public void readyToDumpTableDigests(Multimap<String, String> states) {
//    if (panguUtil == null) {
//      // for local test, panguUtil is null.
//      return;
//    }
//
//    Multimap<String, String> compressedQueryDigests = ArrayListMultimap.create();
//    states.asMap().forEach((digest, tableNames) -> {
//      String compressedDigest = compressDigest(digest);
//      tableNames.forEach(
//          tableName -> compressedQueryDigests.put(compressedDigest, tableName));
//    });
//
//    dumpStates = TableDigestManager
//        .serialize(new TableDigestManager(queryDigest, compressedQueryDigests, stateVersion));
//  }
//
//  /**
//   * When all the plans have been executed successfully,
//   * driver will call this method to write the new digests.
//   */
//  public void dumpTableDigests() {
//    if (dumpStates == null || "".equals(dumpStates)) {
//      LOG.info("No digest of state table will be written to Pangu file.");
//      return;
//    }
//
//    if (!panguUtil.fileExists(filePath)) {
//      panguUtil.createFile(filePath);
//    }
//    panguUtil.writeText(filePath, dumpStates);
//  }
//
//  private static class TableDigestManager {
//    String queryDigest;
//    // <md5(digest), list of table names>
//    // different tables may have the same digest
//    Multimap<String, String> tableDigests;
//    String stateVersion;
//
//    TableDigestManager(String queryDigest,
//        Multimap<String, String> tableDigests, String stateVersion) {
//      this.queryDigest = queryDigest;
//      this.tableDigests = tableDigests;
//      this.stateVersion = stateVersion;
//    }
//
//    public Collection<String> getTable(String digest) {
//      return tableDigests.get(digest);
//    }
//
//    public static String serialize(TableDigestManager tableDigestManager) {
//      JsonObject jsonObject = new JsonObject();
//      jsonObject.addProperty("queryDigest", tableDigestManager.queryDigest);
//
//      Map<String, Collection<String>> digestMultiMap = tableDigestManager.tableDigests.asMap();
//      JsonObject tableDigestJson = new JsonObject();
//      for (Map.Entry<String, Collection<String>> e: digestMultiMap.entrySet()) {
//        JsonArray tableArr = new JsonArray();
//        e.getValue().forEach(tableArr::add);
//        tableDigestJson.add(e.getKey(), tableArr);
//      }
//      jsonObject.add("tableDigests", tableDigestJson);
//      jsonObject.addProperty("stateVersion", tableDigestManager.stateVersion);
//
//      return jsonObject.toString();
//    }
//
//    public static TableDigestManager deserialize(String jsonStr,
//        String requiredQueryDigest, String requiredStateVersion) {
//      JsonObject jsonObject;
//      try {
//        JsonElement jsonEle = new JsonParser().parse(jsonStr);
//        jsonObject = jsonEle.getAsJsonObject();
//      } catch (JsonSyntaxException e) {
//        LOG.info("The json string is invalid: " + jsonStr);
//        return null;
//      }
//
//      JsonElement queryDigestEle = jsonObject.get("queryDigest");
//      if (queryDigestEle == null) {
//        return null;
//      }
//
//      String queryDigest = queryDigestEle.getAsString();
//      if (!queryDigest.equals(requiredQueryDigest)) {
//        return null;
//      }
//
//      Multimap<String, String> tableDigests = ArrayListMultimap.create();
//      JsonElement stateVersionEle = jsonObject.get("stateVersion");
//      if (stateVersionEle == null) {
//        return null;
//      }
//      String stateVersion = stateVersionEle.getAsString();
//      if (requiredStateVersion.equals(stateVersion)) {
//        LOG.info("Successfully find the state tables with the state version:" + stateVersion);
//        JsonElement tableDigestEle = jsonObject.get("tableDigests");
//        if (tableDigestEle != null) {
//          JsonObject tableDigestJson = tableDigestEle.getAsJsonObject();
//          tableDigestJson.entrySet().forEach(e -> {
//            String tableDigest = e.getKey();
//            e.getValue().getAsJsonArray()
//                .forEach(t -> tableDigests.put(tableDigest, t.getAsString()));
//          });
//        }
//      } else {
//        LOG.info(
//            "The state tables for this query are invalid, because the required state version "
//                + requiredStateVersion
//                + " is not equal to the recorded state version " + stateVersion + ".");
//      }
//
//      return new TableDigestManager(queryDigest, tableDigests, stateVersion);
//    }
//  }
//
//  private static String getRangeQueryStateVersion(OdpsRelContext ctx) {
//    return ctx.getConfig().get(RANGE_QUERY_STATE_VERSION);
//  }
//
//  private static String getRangeQueryStateLocation(OdpsRelContext ctx) {
//    return ctx.getConfig().get(RANGE_QUERY_PANGU_LOCATION);
//  }
//
//  private static String getActivePanguCluster(OdpsRelContext ctx) {
//    return ctx.getConfig().get(
//        ODPS_PROGRESSIVE_ACTIVE_PANGU_CLUSTER, "pangu://localcluster/");
//  }
//}
