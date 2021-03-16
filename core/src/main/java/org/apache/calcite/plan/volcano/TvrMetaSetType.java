package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.VersionInterval;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class TvrMetaSetType implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final TvrMetaSetType DEFAULT =
      new TvrMetaSetType(new TvrVersion[] { TvrVersion.MAX },
          new VersionInterval[0]) {
        private final long[] versions = new long[] { TvrVersion.MAX_TIME };

        @Override
        public boolean addNewVersion(long newVersion) {
          return false;
        }

        @Override
        public boolean addNewDimensions(int newVersionDim,
            Map<Integer, Integer> ordinalMapping) {
          return false;
        }

        @Override
        public long[] getTvrVersions() {
          return versions;
        }

        @Override
        public TvrMetaSetType copy() {
          return this;
        }
      };

  protected TvrVersion[] snapshots;
  protected VersionInterval[] deltas;

  public TvrMetaSetType(TvrVersion[] snapshots, VersionInterval[] deltas) {
    this.snapshots = snapshots;
    this.deltas = deltas;
  }

  public TvrMetaSetType(Pair<TvrVersion[], VersionInterval[]> versions) {
    this.snapshots = versions.left;
    this.deltas = versions.right;
  }

  public TvrVersion[] getSnapshots() {
    return snapshots;
  }

  public VersionInterval[] getDeltas() {
    return deltas;
  }

  public boolean contains(TvrVersion snapshot) {
    return Arrays.asList(snapshots).contains(snapshot);
  }

  public boolean contains(VersionInterval delta) {
    if (deltas == null) {
      return false;
    }
    for (VersionInterval d : deltas) {
      if (d.equals(delta)) {
        return true;
      }
    }
    return false;
  }

  protected void updateTvrVersions(Pair<TvrVersion[], VersionInterval[]> tvrVersions) {
    this.snapshots = tvrVersions.left;
    this.deltas = tvrVersions.right;
  }

  public abstract long[] getTvrVersions();

  public abstract TvrMetaSetType copy();

  public abstract boolean addNewDimensions(int newVersionDim,
      Map<Integer, Integer> ordinalMapping);

  public abstract boolean addNewVersion(long newVersion);

  @Override
  public String toString() {
    return "{" + Arrays.stream(snapshots).map(Object::toString)
        .collect(Collectors.joining(";")) + Arrays.stream(deltas)
        .map(VersionInterval::toString).collect(Collectors.joining(";")) + "}";
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof TvrMetaSetType)) {
      return false;
    }
    TvrMetaSetType o = (TvrMetaSetType) other;
    return Arrays.equals(snapshots, o.snapshots) && Arrays
        .equals(deltas, o.deltas);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(snapshots) * 31 + Arrays.hashCode(deltas);
  }

  public VersionInterval findFirstDelta(int ord, long fromVersion, long toVersion) {
    for (VersionInterval d : deltas) {
      if (fromVersion == d.from.get(ord) && toVersion == d.to.get(ord)) {
        return d;
      }
    }
    return null;
  }

}
