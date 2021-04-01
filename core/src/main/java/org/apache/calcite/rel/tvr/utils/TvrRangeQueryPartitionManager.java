package org.apache.calcite.rel.tvr.utils;

import org.apache.calcite.plan.RelOptTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Manage the mapping between table partition and tvr version.
 */
public class TvrRangeQueryPartitionManager {

  // <Table name, <Tvr version, Qualified partitions>>
  private Map<String, Map<Long, Set<String>>> partitionVersionMapping = new HashMap<>();

  private static final Log LOG = LogFactory.getLog(TvrRangeQueryPartitionManager.class);

  /**
   *  convert the partitions in the input tables to the tvr versions.
   */
  public void recordTablePartitions(Set<RelOptTable> tables) {
    throw new UnsupportedOperationException("recordTablePartitions not supported");
//    for (RelOptTable t : tables) {
//      OdpsTable odpsTable = t.getTable();
//      if (!odpsTable.isPartitioned()) {
//        continue;
//      }
//
//      List<RelOptPartition> qualifiedPartitions = t.getQualifiedPartitions();
//      List<Partition> partitions;
//      if (qualifiedPartitions == null) {
//        // read all the partitions
//        partitions = odpsTable.getPartitions();
//      } else if (qualifiedPartitions.isEmpty()) {
//        // read nothing
//        partitions = Collections.emptyList();
//      } else {
//        partitions = qualifiedPartitions.stream()
//            .map(p -> p.getPartition().getInnerPartition())
//            .collect(Collectors.toList());
//      }
//
//      List<String> partitionCols = odpsTable.getPartitionColumnNames();
//      assert !partitionCols.isEmpty();
//      TablePartitionToTvrVersionHelper helper =
//          new TablePartitionToTvrVersionHelper(partitionCols, partitions);
//
//      // All partitions are grouped by the first-level time related partition.
//      // For example, "ds=20200618, hh=00", "ds=20200618, hh=08" and "ds=20200618, hh=16"
//      //  all belong to the same tvr version "1592409600000" (which is the timestamp of "20200618").
//      String tableName = odpsTable.getNormalizedName();
//      Map<Long, Set<String>> partitionMapping =
//          partitionVersionMapping.computeIfAbsent(tableName, k -> new HashMap<>());
//      helper.getPartitionMapping().forEach(
//          (timestamp, partitionStrs) -> partitionMapping
//              .computeIfAbsent(timestamp, k -> new HashSet<>())
//              .addAll(partitionStrs));
//    }
  }

//  private class TablePartitionToTvrVersionHelper {
//    // all the possible patterns for matching partitions,
//    // and patterns are sorted according to the time precision.
//    private final String[] possiblePatterns = new String[]{"yyyyMMdd", "yyyyMM"};
//    // If all patterns are inconsistent with the given partition, try fuzzy matching.
//    // And the date needs to be rounded.
//    private final String[] fuzzyMatchingPatterns = new String[]{"yyyyMMddHHmmss", "yyyyMMddHHmm", "yyyyMMddHH"};
//    // each fuzzy matching pattern has its rounding pattern (i.e., day-level)
//    // NOTE: roundingPatterns.size == fuzzyMatchingPatterns.size
//    private final int[] roundingPatterns = new int[]{ Calendar.DAY_OF_MONTH, Calendar.DAY_OF_MONTH, Calendar.DAY_OF_MONTH};
//    // the specific rounding pattern
//    private Integer roundingPattern = null;
//
//    // the matched pattern for these input partitions
//    private String pattern = null;
//    // the name of the first-level time related partition column
//    private String firstTimePartitionCol = null;
//    // partitions waiting to be parsed to get the tvr versions
//    private List<Partition> qualifiedPartitions;
//
//    TablePartitionToTvrVersionHelper(List<String> partitionCols,
//        List<Partition> qualifiedPartitions) {
//      assert qualifiedPartitions != null;
//      this.qualifiedPartitions = qualifiedPartitions;
//
//      if (!qualifiedPartitions.isEmpty()) {
//        // randomly select an input partition to infer the time column and time format pattern
//        PartitionSpec sample = qualifiedPartitions.get(0).getPartitionSpec();
//        inferTimeColAndFormatPattern(partitionCols, sample, false);
//        if (pattern == null) {
//          // if all patterns are inconsistent with the given partition, try fuzzy matching.
//          // e.g., although the pattern of "2020091201" is "yyyyMMddhh",
//          //       it can be matched with "yyyyMMdd".
//          inferTimeColAndFormatPattern(partitionCols, sample, true);
//        }
//      }
//    }
//
//    private void inferTimeColAndFormatPattern(List<String> partitionCols,
//        PartitionSpec sample, boolean fuzzyMatching) {
//      String[] patterns = fuzzyMatching ? fuzzyMatchingPatterns : possiblePatterns;
//      for (int i = 0; i < partitionCols.size() && firstTimePartitionCol == null; i++) {
//        String partitionCol = partitionCols.get(i);
//        for (int j = 0; j < patterns.length; j++) {
//          String pattern = patterns[j];
//          try {
//            // find the first matched time related partition for the given time format patterns.
//            // e.g., "apptype='tb', ds='20200811', hh='08', mm='30'"
//            //       => firstTimePartitionCol = "ds", pattern = "yyyyMMdd"
//            DateUtils.parseDateStrictly(sample.get(partitionCol), pattern);
//            this.firstTimePartitionCol = partitionCol;
//            this.pattern = pattern;
//            if (fuzzyMatching) {
//              this.roundingPattern = roundingPatterns[j];
//            }
//            break;
//          } catch (ParseException ignored) {
//          }
//        }
//      }
//
//      if (LOG.isDebugEnabled() && (firstTimePartitionCol == null || pattern == null)) {
//        LOG.debug("Unknown partition pattern : " + sample
//            + ", range query optimization only supports these types of input partitioned tables, "
//            + "whose first-level partition is named in the following formats: "
//            + Stream
//            .of(possiblePatterns).collect(Collectors.joining(", ", "{", "}")));
//      }
//    }
//
//    Map<Long, Set<String>> getPartitionMapping() {
//      if (firstTimePartitionCol == null || pattern == null) {
//        // the given partitions can not be convert to the tvr version
//        return ImmutableMap.of(Long.MAX_VALUE,
//            qualifiedPartitions.stream().map(Partition::getPartSpecStr)
//                .collect(Collectors.toSet()));
//      }
//
//      Map<Long, Set<String>> m = new HashMap<>();
//      for (Partition p : qualifiedPartitions) {
//        PartitionSpec spec = p.getPartitionSpec();
//        try {
//          String timeStr = spec.get(firstTimePartitionCol);
//          Date date = DateUtils.parseDateStrictly(timeStr, pattern);
//          if (roundingPattern != null) {
//            date = DateUtils.round(date, roundingPattern);
//          }
//          long timestamp = new Timestamp(date.getTime()).getTime();
//          Set<String> partitions = m.computeIfAbsent(timestamp, k -> new HashSet<>());
//          partitions.add(p.getPartSpecStr());
//        } catch (ParseException e) {
//          // if any partition fails to get timestamp, the other results will not be retained
//          return Collections.emptyMap();
//        }
//      }
//
//      return m;
//    }
//  }
//
//  public Map<String, Map<Long, Set<String>>> getPartitionVersionMapping() {
//    return partitionVersionMapping;
//  }
//
//  /**
//   * Apply the time interval of each partition table to get the real qualified partitions.
//   *
//   * For example,
//   *    Table: qualified partitions = [20200901, 20200902, 20200903]
//   *           time interval = [MIN, 1598976000000 (which is the timestamp of "20200902")]
//   *    Real qualified partitions = [20200901, 20200902]
//   */
//  public List<RelOptPartition> getQualifiedPartitions(OdpsRelOptTable odpsRelOptTable) {
//    OdpsTable odpsTable = odpsRelOptTable.getTable();
//    if (!odpsTable.isPartitioned()) {
//      return null;
//    }
//
//    TimeInterval timeInterval = odpsRelOptTable.getInterval();
//    List<RelOptPartition> qualifiedPartitions = odpsRelOptTable.getQualifiedPartitions();
//    if (qualifiedPartitions == null) {
//      qualifiedPartitions = OdpsRelOptUtils
//          .convertRelOptPartitions(odpsRelOptTable, odpsTable.getPartitions());
//    }
//
//    Map<Long, Set<String>> partitionVersionMapping = this.partitionVersionMapping
//        .get(odpsTable.getNormalizedName());
//    Set<String> selectedPartitions = partitionVersionMapping.entrySet().stream()
//        .filter(e -> e.getKey() <= timeInterval.to && e.getKey() > timeInterval.from)
//        .flatMap(e -> e.getValue().stream())
//        .collect(Collectors.toSet());
//
//    return qualifiedPartitions.stream().filter(p -> selectedPartitions
//        .contains(p.getPartition().getInnerPartition().getPartSpecStr()))
//        .collect(Collectors.toList());
//  }
}
