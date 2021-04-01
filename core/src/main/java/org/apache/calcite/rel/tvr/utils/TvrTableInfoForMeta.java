package org.apache.calcite.rel.tvr.utils;

import org.apache.calcite.plan.volcano.TvrNode;
import org.apache.calcite.rel.core.TableSink;
import org.apache.calcite.rel.logical.LogicalTableScan;

import java.io.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class TvrTableInfoForMeta implements Serializable {

  private static final long serialVersionUID = 1L;

  // The tvr relation map that records all related tvrs and tables saved for
  // downstream queries
  private TvrNode tvrNodeGraph;

  // Table lineage. The list of source/base tables used by upstream queries
  // and this query to compute the output table of this query
  private List<String> updateTables;

  public TvrTableInfoForMeta(TableSink originTableSink,
                             TvrNode tvrNodeGraph) {
    this.tvrNodeGraph = tvrNodeGraph;
    this.updateTables =
        new ArrayList<>(TvrUtils.collectUpdateTableNames(originTableSink));
  }

  public TvrNode getTvrNodeGraph() {
    return tvrNodeGraph;
  }

  public List<String> getUpdateTables() {
    return updateTables;
  }

  public static String serialize(TvrTableInfoForMeta obj) {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream os = new ObjectOutputStream(bos);
      os.writeObject(obj);
      os.close();
      return Base64.getEncoder().encodeToString(bos.toByteArray());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static TvrTableInfoForMeta deserialize(String s) {
    try {
      // NOTE: check whether derived table is enabled
      //       i.e., TvrUtils.progressiveDerivedTableEnabled(ctx)
      ByteArrayInputStream bis =
          new ByteArrayInputStream(Base64.getDecoder().decode(s));
      ObjectInputStream oi = new ObjectInputStream(bis);
      TvrTableInfoForMeta ret = (TvrTableInfoForMeta) oi.readObject();
      oi.close();
      return ret;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static TvrTableInfoForMeta deserialize(LogicalTableScan tableScan) {
    TvrContext ctx = TvrContext.getInstance(tableScan.getCluster());
    if (!TvrUtils.progressiveDerivedTableEnabled(ctx)) {
      return null;
    }

    String tvrTableMetaStr = TvrTableUtils.findTvrRelatedTableMapStr(tableScan);
    if (tvrTableMetaStr == null) {
      return null;
    }
    return deserialize(tvrTableMetaStr);
  }
}
