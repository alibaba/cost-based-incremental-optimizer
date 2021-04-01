package org.apache.calcite.rel.tvr.trait;

import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.TvrMetaSetType;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.VersionInterval;

import java.util.HashMap;
import java.util.Map;

/**
 * Produce a tvrType for only updating one table out of all tables.
 * The series with three tables and three versions for updating table ord 1:
 * 112 \
 * 122 /
 * 223 \
 * 233 /
 */
public class TvrUpdateOneMetaSetType extends TvrMetaSetType {

  // maxVersion, numTables -> tableOrdinal -> UpdateOneTableTvrType
  private final static Map<Pair<Long, Integer>, Map<Integer, TvrUpdateOneMetaSetType>>
      UPDATE_ONE_TABLE_TVR_TYPES = new HashMap<>();

  private final TvrDefaultMetaSetType baseType;
  private int tableOrd;

  private TvrUpdateOneMetaSetType(TvrDefaultMetaSetType baseType, int tableOrd) {
    super(createVersions(baseType, tableOrd));
    this.baseType = baseType;
    this.tableOrd = tableOrd;
  }

  public static TvrUpdateOneMetaSetType create(TvrDefaultMetaSetType type, int tableOrd) {
    long[] versions = type.getTvrVersions();
    int versionDim = type.getVersionDim();

    assert versionDim > 0;
    long maxVersion = versions[versions.length - 1];
    Pair<Long, Integer> key = new Pair<>(maxVersion, versionDim);
    Map<Integer, TvrUpdateOneMetaSetType> tvrTypeMap =
        UPDATE_ONE_TABLE_TVR_TYPES.computeIfAbsent(key, r -> new HashMap<>());
    if (tvrTypeMap.containsKey(tableOrd)) {
      return tvrTypeMap.get(tableOrd);
    }

    TvrUpdateOneMetaSetType ret = new TvrUpdateOneMetaSetType(type, tableOrd);
    ret.updateCache();

    System.out.println(
        "UpdateOneTableTvrType table ord " + tableOrd + " with " + maxVersion
            + " versions " + versionDim + " tables: ");
    System.out.println(ret.toString());

    return ret;
  }

  private static Pair<TvrVersion[], VersionInterval[]> createVersions(
      TvrDefaultMetaSetType type, int tableOrd) {
    long[] versions = type.getTvrVersions();
    int versionDim = type.getVersionDim();
    int versionNum = versions.length;
    assert versionDim > 0;

    int index = 0;
    TvrVersion[] snapshots = new TvrVersion[2 * versionNum - 2];
    for (int i = 0; i < versionNum - 1; i++) {
      // from snapshot version
      long[] snapshot = new long[versionDim];
      long instant = versions[i + 1];
      for (int j = 0; j < tableOrd; j++) {
        snapshot[j] = instant;
      }
      instant = versions[i];
      for (int j = tableOrd; j < versionDim; j++) {
        snapshot[j] = instant;
      }
      snapshots[index++] = TvrVersion.of(snapshot);

      // to snapshot version
      snapshot = new long[versionDim];
      instant = versions[i + 1];
      for (int j = 0; j <= tableOrd; j++) {
        snapshot[j] = instant;
      }
      instant = versions[i];
      for (int j = tableOrd + 1; j < versionDim; j++) {
        snapshot[j] = instant;
      }
      snapshots[index++] = TvrVersion.of(snapshot);
    }

    VersionInterval[] deltas = new VersionInterval[versionNum - 1];
    for (int i = 0; i < deltas.length; i++) {
      deltas[i] = VersionInterval.of(snapshots[2 * i], snapshots[2 * i + 1]);
    }

    return Pair.of(snapshots, deltas);
  }

  @Override
  public long[] getTvrVersions() {
    return baseType.getTvrVersions();
  }

  @Override
  public TvrUpdateOneMetaSetType copy() {
    return new TvrUpdateOneMetaSetType(baseType, tableOrd);
  }

  @Override
  public boolean addNewDimensions(int newVersionDim,
      Map<Integer, Integer> ordinalMapping) {
    if (newVersionDim == baseType.getVersionDim()
        && ordinalMapping.get(tableOrd) == tableOrd) {
      return false;
    }
    this.baseType.addNewDimensions(newVersionDim, ordinalMapping);
    this.tableOrd = ordinalMapping.get(tableOrd);
    this.updateTvrVersions(createVersions(baseType, tableOrd));
    this.updateCache();
    return true;
  }

  @Override
  public boolean addNewVersion(long newVersion) {
    boolean changed = this.baseType.addNewVersion(newVersion);
    if (! changed) {
      return false;
    }
    this.updateTvrVersions(createVersions(baseType, tableOrd));
    this.updateCache();
    return true;
  }

  private void updateCache() {
    long maxVersion = baseType.getTvrVersions()[baseType.getTvrVersions().length - 1];
    int versionDim = baseType.getVersionDim();
    Map<Integer, TvrUpdateOneMetaSetType> map = UPDATE_ONE_TABLE_TVR_TYPES
        .computeIfAbsent(Pair.of(maxVersion, versionDim), k -> new HashMap<>());
    map.put(tableOrd, this);
  }


  public TvrDefaultMetaSetType getBaseType() {
    return baseType;
  }

  public int getTableOrd() {
    return tableOrd;
  }
}
