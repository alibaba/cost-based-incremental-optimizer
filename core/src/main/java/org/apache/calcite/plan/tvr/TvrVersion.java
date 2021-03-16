package org.apache.calcite.plan.tvr;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringJoiner;

public class TvrVersion implements Comparable<TvrVersion>, Serializable {
  private static final long serialVersionUID = 1L;

  private final static ThreadLocal<ListMultimap<Long, TvrVersion>> cache =
      ThreadLocal.withInitial(() -> Multimaps.newListMultimap(new HashMap<>(), ArrayList::new));

  public static final long MIN_TIME = Long.MIN_VALUE;
  public static final long MAX_TIME = Long.MAX_VALUE;

  public static final TvrVersion MIN = new TvrVersion(new long[] { MIN_TIME }) {
    @Override
    public boolean isMax() {
      return false;
    }

    @Override
    public boolean isMin() {
      return true;
    }

    @Override
    public long get(int tableOrd) {
      return MIN_TIME;
    }
  };

  public static final TvrVersion MAX = new TvrVersion(new long[] { MAX_TIME }) {
    @Override
    public boolean isMax() {
      return true;
    }

    @Override
    public boolean isMin() {
      return false;
    }

    @Override
    public long get(int tableOrd) {
      return MAX_TIME;
    }
  };

  private final long[] versions;

  protected TvrVersion(long[] versions) {
    this.versions = versions;
  }

  public static TvrVersion of(long[] versions) {
    assert versions.length > 0;

    // [MAX, MAX, ...] -> MAX
    // [MIN, MIN, ...] -> MIN
    if (Arrays.stream(versions).allMatch(x -> x == MIN_TIME)) {
      return MIN;
    } else if (Arrays.stream(versions).allMatch(x -> x == MAX_TIME)) {
      return MAX;
    }

    long hash = Arrays.hashCode(versions);
    ListMultimap<Long, TvrVersion> cacheMap = cache.get();
    for (TvrVersion v : cacheMap.get(hash)) {
      if (Arrays.equals(v.versions, versions)) {
        return v;
      }
    }

    TvrVersion v = new TvrVersion(versions);
    cacheMap.put(hash, v);
    return v;
  }

  public boolean isMax() {
    return this.equals(MAX);
  }

  public boolean isMin() {
    return this.equals(MIN);
  }

  public long get(int tableOrd) {
    assert versions.length > tableOrd;
    return versions[tableOrd];
  }

  public long[] getVersions() {
    return versions;
  }

  public long maxVersion() {
    return Arrays.stream(versions).max().getAsLong();
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(",", "[", "]");
    for (Long v : versions) {
      if (v == MIN_TIME) {
        joiner.add("MIN");
      } else if (v == MAX_TIME) {
        joiner.add("MAX");
      } else {
        joiner.add(v.toString());
      }
    }
    return joiner.toString();
  }

  @Override
  public int compareTo(TvrVersion o) {
    if (this == MAX && o != MAX) {
      return 1;
    } else if (this == MIN && o != MIN) {
      return -1;
    } else if (this != MAX && o == MAX) {
      return -1;
    } else if (this != MIN && o == MIN) {
      return 1;
    }

    // Compare these two TvrVersion element by element.
    // For example:
    //      [2,2,2] is greater than [1,1,1]
    //      [2,2,1] is greater than [2,1,1]
    //      [2,1,1] can not compare with [1,1,2]
    long[] versions = getVersions();
    long[] otherVersions = o.getVersions();
    if (versions.length != otherVersions.length) {
      throw new RuntimeException(
          "These two tvr versions are not comparable "
              + "because they do not have the same number of elements. \n"
              + "One is " + Arrays.toString(versions) + ", "
              + "another is " + Arrays.toString(otherVersions));
    }

    int result = 0;
    int i = 0;

    // Find the non-zero one.
    while (i < versions.length && result == 0) {
      result = Long.compare(versions[i], otherVersions[i]);
      i++;
    }

    // Check whether these two tvr versions are comparable.
    while (i < versions.length) {
      int compareI = Long.compare(versions[i], otherVersions[i]);
      if (compareI != 0 && result != compareI) {
        throw new RuntimeException("These two tvr versions are not comparable, "
            + "because not all elements of one array are greater than or equal to "
            + "the corresponding elements of the other array. "
            + "For example, \"[2,1,1] can not compare with [1,1,2]\". \n"
            + "One is " + Arrays.toString(versions) + ", "
            + "another is " + Arrays.toString(otherVersions));
      }
      i++;
    }

    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TvrVersion)) {
      return false;
    }
    TvrVersion that = (TvrVersion) o;
    return Arrays.equals(versions, that.versions);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(versions);
  }

}
