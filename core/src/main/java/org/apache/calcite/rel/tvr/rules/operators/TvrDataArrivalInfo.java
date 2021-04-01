package org.apache.calcite.rel.tvr.rules.operators;

import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TvrDataArrivalInfo {

  public static class TvrDataArrivalPattern {
    public String insert;
    public String delete;

    public TvrDataArrivalPattern() { }

    public TvrDataArrivalPattern(String insert, String delete) {
      this.insert = insert;
      this.delete = delete;
    }

    public List<Integer> insertPartitions() {
      if (!insert.contains("-")) {
        return Collections.singletonList(Integer.parseInt(insert.trim()));
      }

      int from = Integer.parseInt(insert.substring(0, insert.indexOf("-")));
      int to = Integer.parseInt(insert.substring(insert.indexOf("-") + 1));
      return IntStream.rangeClosed(from, to).boxed().collect(Collectors.toList());
    }

    public List<Integer> deletePartitions() {
      if (delete == null) {
        return Collections.emptyList();
      }
      if (!delete.contains("-")) {
        return Collections.singletonList(Integer.parseInt(delete.trim()));
      }
      int from = Integer.parseInt(delete.substring(0, delete.indexOf("-")));
      int to = Integer.parseInt(delete.substring(delete.indexOf("-") + 1));
      return IntStream.range(from, to).boxed().collect(Collectors.toList());
    }

  }

  public String partition;
  public Map<String, TvrDataArrivalPattern> pattern;

  public TvrDataArrivalInfo() { }

  public TvrDataArrivalInfo(String partition,
      Map<String, TvrDataArrivalPattern> pattern) {
    this.partition = partition;
    this.pattern = pattern;
  }

  public boolean isPositiveOnly(long from, long to) {
    if (from == to) {
      return true;
    }
    if (from == TvrVersion.MIN_TIME) {
      return true;
    }
//    Pair<List<PartitionSpec>, List<PartitionSpec>> partitions =
//        this.partitions(from, to);
//    return partitions.right.isEmpty();
    return true;
  }


  private static long from(String invStr) {
    return timeStrToLong(invStr.substring(0, invStr.indexOf("-")).trim());
  }

  private static long to(String invStr) {
    return timeStrToLong(invStr.substring(invStr.indexOf("-") + 1).trim());
  }

  private static long timeStrToLong(String timeStr) {
    if (timeStr.equalsIgnoreCase("MIN")) {
      return TvrVersion.MIN_TIME;
    } else if (timeStr.equalsIgnoreCase("MAX")) {
      return TvrVersion.MAX_TIME;
    } else {
      return Long.parseLong(timeStr);
    }
  }

//  private PartitionSpec toPartitionSpec(int partitionValue) {
//    return new PartitionSpec(partition + "=" + partitionValue);
//  }
//
//  /**
//   * Returns a Pair of InsertPartitions, DeletePartitions
//   */
//  public Pair<List<PartitionSpec>, List<PartitionSpec>> partitions(long from,
//      long to) {
//    List<Integer> insParts = new ArrayList<>();
//    List<Integer> delParts = new ArrayList<>();
//
//    for (Map.Entry<String, TvrDataArrivalPattern> entry : this.pattern
//        .entrySet()) {
//      String invStr = entry.getKey();
//      TvrDataArrivalPattern parts = entry.getValue();
//      long invFrom = from(invStr);
//      long invTo = to(invStr);
//      assert invFrom < invTo;
//      if (invFrom >= from && invTo <= to) {
//        insParts.addAll(parts.insertPartitions());
//        delParts.addAll(parts.deletePartitions());
//      }
//    }
//
//    if (from == TvrVersion.MIN_TIME) {
//      assert insParts.containsAll(delParts);
//      insParts.removeAll(delParts);
//      delParts = Collections.emptyList();
//    }
//
//    List<PartitionSpec> insPartSpecs =
//        insParts.stream().map(this::toPartitionSpec)
//            .collect(Collectors.toList());
//    List<PartitionSpec> delPartSpecs =
//        delParts.stream().map(this::toPartitionSpec)
//            .collect(Collectors.toList());
//
//    return Pair.of(insPartSpecs, delPartSpecs);
//  }

}
