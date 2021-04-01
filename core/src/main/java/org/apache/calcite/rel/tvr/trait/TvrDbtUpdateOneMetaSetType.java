package org.apache.calcite.rel.tvr.trait;

import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.TvrMetaSetType;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.VersionInterval;

import java.util.*;
import java.util.stream.Stream;

public class TvrDbtUpdateOneMetaSetType extends TvrMetaSetType {

  // maxVersion, numTables -> tableOrdinal -> UpdateOneTableTvrType
  private final static Map<Pair<Long, Integer>, Map<Integer, TvrDbtUpdateOneMetaSetType>>
      DBT_UPDATE_ONE_TABLE_TVR_TYPES = new HashMap<>();

  private final TvrDefaultMetaSetType baseType;
  private int tableOrd;

  private TvrDbtUpdateOneMetaSetType(TvrDefaultMetaSetType baseType, int tableOrd) {
    super(createVersions(baseType, tableOrd));
    this.baseType = baseType.copy();
    this.tableOrd = tableOrd;
  }

  public static TvrDbtUpdateOneMetaSetType create(TvrDefaultMetaSetType tvrType, int tableOrd) {
    long maxVersion = tvrType.getTvrVersions()[tvrType.getTvrVersions().length - 1];
    Pair<Long, Integer> key = Pair.of(maxVersion, tvrType.getVersionDim());
    Map<Integer, TvrDbtUpdateOneMetaSetType> cache =
        DBT_UPDATE_ONE_TABLE_TVR_TYPES.computeIfAbsent(key, r -> new HashMap<>());

    // look up in cache
    if (cache.containsKey(tableOrd)) {
      return cache.get(tableOrd);
    }

    TvrDbtUpdateOneMetaSetType type =
        new TvrDbtUpdateOneMetaSetType(tvrType, tableOrd);
    type.updateCache();

    return type;
  }

  private static Pair<TvrVersion[], VersionInterval[]> createVersions(
      TvrDefaultMetaSetType tvrType, int tableOrd) {
    TvrFullMetaSetType fullType = TvrFullMetaSetType.create(tvrType);
    TvrUpdateOneMetaSetType updateOneTable =
        TvrUpdateOneMetaSetType.create(tvrType, tableOrd);

    List<TvrVersion> fullSnapshots = Arrays.asList(fullType.getSnapshots());
    List<VersionInterval> newDeltas = new ArrayList<>();
    for (VersionInterval delta : tvrType.getDeltas()) {
      int fromIndex = fullSnapshots.indexOf(delta.from);
      int toIndex = fullSnapshots.indexOf(delta.to);
      for (VersionInterval updateOneDelta : updateOneTable.getDeltas()) {
        int updateFromIndex = fullSnapshots.indexOf(updateOneDelta.from);
        int updateToIndex = fullSnapshots.indexOf(updateOneDelta.to);
        if (updateFromIndex >= fromIndex && updateToIndex <= toIndex) {
          newDeltas
              .add(VersionInterval.of(updateOneDelta.from, updateOneDelta.to));
        }
      }
    }

    TvrVersion[] newSnapshots =
        newDeltas.stream().flatMap(inv -> Stream.of(inv.from, inv.to))
            .sorted(Comparator.comparing(fullSnapshots::indexOf))
            .toArray(TvrVersion[]::new);
    VersionInterval[] newDeltasArray =
        newDeltas.toArray(new VersionInterval[0]);

    return Pair.of(newSnapshots, newDeltasArray);
  }

  @Override
  public long[] getTvrVersions() {
    return baseType.getTvrVersions();
  }

  @Override
  public TvrDbtUpdateOneMetaSetType copy() {
    return new TvrDbtUpdateOneMetaSetType(baseType, tableOrd);
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
    long maxVersion =
        baseType.getTvrVersions()[baseType.getTvrVersions().length - 1];
    int versionDim = baseType.getVersionDim();
    Map<Integer, TvrDbtUpdateOneMetaSetType> map =
        DBT_UPDATE_ONE_TABLE_TVR_TYPES
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
