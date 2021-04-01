package org.apache.calcite.rel.tvr.trait;

import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.TvrMetaSetType;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.VersionInterval;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Produce a tvrType for OJV algorithm, which update one table at a time.
 * The snapshot series with three tables and three versions is:
 * 111
 * 112
 * 122
 * 222
 * 223
 * 233
 * 333
 */
public class TvrFullMetaSetType extends TvrMetaSetType {

  // maxVersion, numTables -> UpdateOneTableFullTvrType
  private final static Map<Pair<Long, Integer>, TvrFullMetaSetType>
      UPDATE_ONE_TABLE_FULL_TVR_TYPES = new HashMap<>();

  private final TvrDefaultMetaSetType baseType;

  private TvrFullMetaSetType(TvrDefaultMetaSetType baseType) {
    super(createVersions(baseType));
    this.baseType = baseType.copy();
  }

  public static TvrFullMetaSetType create(TvrDefaultMetaSetType type) {
    long[] versions = type.getTvrVersions();
    long maxVersion = versions[versions.length - 1];
    int versionDim = type.getVersionDim();
    assert versionDim > 0;

    Pair<Long, Integer> key = new Pair<>(maxVersion, versionDim);
    TvrFullMetaSetType ret = UPDATE_ONE_TABLE_FULL_TVR_TYPES.get(key);
    if (ret != null) {
      return ret;
    }

    ret = new TvrFullMetaSetType(type);
    ret.updateCache();

    System.out.println(
        "UpdateOneTableFullTvrType with " + maxVersion + " versions "
            + versionDim + " tables: ");
    System.out.println(ret.toString());

    return ret;
  }

  private static Pair<TvrVersion[], VersionInterval[]> createVersions(
      TvrDefaultMetaSetType type) {
    long[] versions = type.getTvrVersions();
    int versionDim = type.getVersionDim();
    assert versionDim > 0;

    TvrVersion[] snapshots = new TvrVersion[(versions.length - 1) * versionDim + 1];
    int[] versionIndex = new int[versionDim];

    Arrays.fill(versionIndex, 0); // all tables start with first version
    int updateIndex = 0;
    for (int i = 0; i < snapshots.length; i++) {
      // Encode the snapshot version
      long[] snapshot = new long[versionDim];
      for (int j = 0; j < versionIndex.length; j++) {
        snapshot[j] = versions[versionIndex[j]];
      }
      snapshots[i] = TvrVersion.of(snapshot);

      // Bump up the version index of the next table
      versionIndex[updateIndex]++;
      updateIndex = (updateIndex + 1) % versionDim;
    }

    VersionInterval[] deltas = new VersionInterval[snapshots.length - 1];
    for (int i = 0; i < deltas.length; i++) {
      deltas[i] = VersionInterval.of(snapshots[i], snapshots[i + 1]);
    }

    return Pair.of(snapshots, deltas);
  }

  @Override
  public long[] getTvrVersions() {
    return baseType.getTvrVersions();
  }

  @Override
  public boolean addNewDimensions(int newVersionDim,
      Map<Integer, Integer> ordinalMapping) {
    boolean changed =
        this.baseType.addNewDimensions(newVersionDim, ordinalMapping);
    if (! changed) {
      return false;
    }

    this.updateTvrVersions(createVersions(baseType));
    this.updateCache();
    return true;
  }

  @Override
  public boolean addNewVersion(long newVersion) {
    boolean changed = this.baseType.addNewVersion(newVersion);
    if (! changed) {
      return false;
    }

    this.updateTvrVersions(createVersions(baseType));
    this.updateCache();
    return true;
  }

  @Override
  public TvrFullMetaSetType copy() {
    return new TvrFullMetaSetType(baseType);
  }

  private void updateCache() {
    long maxVersion = baseType.getTvrVersions()[baseType.getTvrVersions().length - 1];
    int versionDim = baseType.getVersionDim();
    UPDATE_ONE_TABLE_FULL_TVR_TYPES.put(Pair.of(maxVersion, versionDim), this);
  }

}

