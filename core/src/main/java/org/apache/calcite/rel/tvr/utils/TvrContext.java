package org.apache.calcite.rel.tvr.utils;

import com.google.common.collect.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.PlannerMetricsListener;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.TvrMetaSetType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.tvr.rules.operators.TvrDataArrivalInfo;
import org.apache.calcite.rel.tvr.trait.TvrDefaultMetaSetType;
import org.apache.calcite.schema.SchemaPlus;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import static org.apache.calcite.rel.tvr.utils.TvrUtils.PROGRESSIVE_EXP_DATA_ARRIVAL_INFO;
import static org.apache.calcite.rel.tvr.utils.TvrUtils.PROGRESSIVE_INSTANTS;

public class TvrContext {

    private Config config;
    private ProgressiveMetrics progressiveMetrics;
    private PlannerMetricsListener listener;
    private Map<String, Integer> nonDimTableOrdinals;
    private List<String> allNonDimTablesSorted;   // sorted by nonDimTableOrdinals
    private Set<String> dimensionTables;    // tables that do not change over time
    private TvrMetaSetType defaultTvrType;

    // mapping from an actual table to the original tables the update is from
    // e.g.: upstream is A JOIN B -> C, downstream is D JOIN C
    // mapping is C:[A,B], D:[D]
    private Multimap<String, String> updateTableMapping;
    // mapping from upstream table ordinal to current table ordinal
    // e.g.: upstream is A JOIN B -> C, downstream is D JOIN C
    // mapping is: C: <1:2, 2:3>, D:<1:1>
    private Map<String, Map<Integer, Integer>> updateTableOrdinalMapping;

    // Key set is all the original table scans, values are their latest
    // upstream info string from meta. Value is null if it is a base table
    // without upstream.
    private Map<LogicalTableScan, TvrTableInfoForMeta> origTableScan2MetaStr;

    private Map<String, TvrDataArrivalInfo> tvrDataArrivalInfos;
    public Set<Integer> joinHugeCost;

    public Map<RelNode, RelSubset> mqcBestPlanMapping;
    public Map<RelSubset, String> mqcMemoCostString;

    // for range query, save the mapping between table partition and tvr version.
    private TvrRangeQueryPartitionManager tvrRangeQueryPartitionManager;

    private final AtomicInteger NEXT_DF_ID = new AtomicInteger(0);
    private final Map<Long, Integer> dfHintId2DfId = Maps.newLinkedHashMap();

    // it is the index of the real time in the instants array
    private int currentOptimizationTimeIndex = 0;


    public TvrContext() {
        this.config = new Config();
    }

    public static TvrContext getInstance(RelOptCluster cluster) {
        return cluster.getPlanner().getContext().unwrap(TvrContext.class);
    }

    public Config getConfig() {
        return this.config;
    }



    private boolean isDownstream = false;

    public boolean isDownstream() {
        return isDownstream;
    }

    public int getCurrentOptimizationRound() {
        return currentOptimizationTimeIndex;
    }

    public long getCurrentOptimizationTime() {
        if (!TvrUtils.progressiveEnabled(this) || !TvrUtils
                .progressiveReoptimizeEnabled(this)) {
            return 0;
        }
        return getTvrVersions()[getCurrentOptimizationRound()];
    }

    public void registerTables(RelNode root) {
        assert nonDimTableOrdinals == null;
        nonDimTableOrdinals = new HashMap<>();
        dimensionTables = new HashSet<>();
        updateTableMapping = HashMultimap.create();
        updateTableOrdinalMapping = new HashMap<>();
        origTableScan2MetaStr = new HashMap<>();
        TvrContext ctx = this;
        ImmutableList.Builder<String> sortedTablesBuilder = ImmutableList.builder();
        Map<String, List<String>> updateTablesForInputTables = new HashMap<>();
        Set<RelOptTable> inputTables = new HashSet<>();

        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                super.visit(node, ordinal, parent);
                assert !(node instanceof RelSubset);
                if (node instanceof LogicalTableScan) {
                    LogicalTableScan ts = (LogicalTableScan) node;
                    TvrTableInfoForMeta tableInfo = TvrTableInfoForMeta.deserialize(ts);
                    // tableInfo is null here for base tables
                    origTableScan2MetaStr.put(ts, tableInfo);
                    String table = TvrTableUtils.getNormalizedTableName(node);

                    List<String> updateTables;
                    if (tableInfo == null) {
                        updateTables = Collections.singletonList(table);
                    } else {
                        updateTables = tableInfo.getUpdateTables();
                        updateTablesForInputTables.put(table, updateTables);
                    }

                    updateTables.forEach(updateTable -> {
                        if (TvrUtils.isDimTable(ctx, updateTable)) {
                            dimensionTables.add(updateTable);
                        } else if (nonDimTableOrdinals
                                .putIfAbsent(updateTable, nonDimTableOrdinals.size()) == null) {
                            sortedTablesBuilder.add(updateTable);
                            updateTableMapping.put(table, updateTable);
                        }
                    });

                    inputTables.add(ts.getTable());
                }
            }
        }.go(root);
        allNonDimTablesSorted = sortedTablesBuilder.build();
        assert allNonDimTablesSorted.size() == nonDimTableOrdinals.size();

        if (TvrUtils.progressiveRangeQueryOptEnabled(this)) {
            // infer tvr versions
            tvrRangeQueryPartitionManager = new TvrRangeQueryPartitionManager();
            tvrRangeQueryPartitionManager.recordTablePartitions(inputTables);
        }

        int numNonDimTables = nonDimTableOrdinals.size();
        defaultTvrType = numNonDimTables == 0 ?
                TvrMetaSetType.DEFAULT :
                TvrDefaultMetaSetType.create(getTvrVersions(), numNonDimTables, this);

        updateTablesForInputTables.forEach((inputTable, updateTables) -> {
            Map<Integer, Integer> ordinalMapping = new HashMap<>();
            for (int i = 0; i < updateTables.size(); i++) {
                ordinalMapping.put(i, nonDimTableOrdinals.get(updateTables.get(i)));
            }
            updateTableOrdinalMapping.put(inputTable, ordinalMapping);
        });

        updateTvrVersionBasedOnUpstream();
    }

//    public TableVersionManager getTableVersionManager() {
//        if (tableVersionManager == null) {
//            tableVersionManager = new TableVersionManager();
//        }
//        return tableVersionManager;
//    }

    public void reloadUpstreamTablePropertyFromMeta() {
        throw new UnsupportedOperationException("reloadUpstreamTablePropertyFromMeta not supported");
//        // reload the table property from meta (TvrTableRelation.TVR_RELATED_TABLE_MAP)
//        if (!TvrUtils.progressiveMetaAvailable(this)) {
//            return;
//        }
//
//        // Load into origTableScan2MetaStr
//        for (OdpsLogicalTableScan ts : origTableScan2MetaStr.keySet()) {
//            TvrTableInfoForMeta tableInfo = TvrTableInfoForMeta.deserialize(ts);
//            TvrTableInfoForMeta existing = origTableScan2MetaStr.put(ts, tableInfo);
//            SchemaPlus schemaPlus =
//                    ((OdpsCatalogReader) getOdpsRelBuilder().getRelOptSchema()).getSchema();
//            if (tableInfo != null) {
//                tableInfo.getTvrNodeGraph().getTables().values()
//                        .forEach(tvrRelations -> tvrRelations.forEach(tvrRelation -> {
//                            // get the derived tables and set the row count estimated by upstream query
//                            OdpsTable t = (OdpsTable) schemaPlus.getTable(
//                                    tvrRelation.projectName + "." + tvrRelation.tableName);
//                            if (t != null) {
//                                t.getProperties().put(TvrTableUtils.TVR_ESTIMATED_ROW_COUNT,
//                                        tvrRelation.estimatedRowCount.toString());
//                            }
//                        }));
//            }
//
//            // Should be both null or both non null
//            assert (tableInfo == null) == (existing == null) :
//                    "problematic scheduling setup: is " + TvrTableUtils
//                            .getNormalizedTableName(ts) + " a base table or not? old " + (
//                            existing == null) + " now " + (tableInfo == null);
//        }
    }

    public boolean updateTvrVersionBasedOnUpstream() {
        boolean changed = false;
        for (TvrTableInfoForMeta tableInfo : origTableScan2MetaStr.values()) {
            if (tableInfo == null) {
                continue;
            }

            isDownstream = true;

            // This is the default tvrType of the upstream query
            TvrMetaSetType tvrType = tableInfo.getTvrNodeGraph().getTvrType();
            for (long version : tvrType.getTvrVersions()) {
                boolean versionChanged = this.addNewVersion(version);
                changed = changed || versionChanged;
            }
        }
        return changed;
    }

    public ProgressiveMetrics getProgressiveMetrics(){
        if(progressiveMetrics == null){
            progressiveMetrics = new ProgressiveMetrics();
        }
        return progressiveMetrics;
    }

    public TvrTableInfoForMeta getTvrTableInfoForMeta(LogicalTableScan ts) {
        return origTableScan2MetaStr.get(ts);
    }

    public Integer tableOrdinal(LogicalTableScan ts) {
        String tableName = TvrTableUtils.getNormalizedTableName(ts);
        return tableOrdinal(tableName);
    }

    public Integer tableOrdinal(String normalizedTableName) {
        return nonDimTableOrdinals.get(normalizedTableName);
    }

    public List<String> allNonDimTablesSorted() {
        return allNonDimTablesSorted;
    }

    public Set<String> getDimensionTables() {
        return dimensionTables;
    }

    public Multimap<String, String> getUpdateTableMapping() {
        return updateTableMapping;
    }

    public Map<String, Map<Integer, Integer>> getUpdateTableOrdinalMapping() {
        return updateTableOrdinalMapping;
    }

    public void loadJoinHugeCost() {
        this.joinHugeCost = TvrUtils.progressiveJoinHugeCost(this);
        System.out.println("adjust join huge cost loaded, rels: " + joinHugeCost);
    }

    public Set<Integer> getJoinHugeCost() {
        if (joinHugeCost == null) {
            return ImmutableSet.of();
        }
        return joinHugeCost;
    }

    public TvrMetaSetType getDefaultTvrType() {
        return defaultTvrType;
    }

    public int getVersionDim() {
        return nonDimTableOrdinals.size();
    }

    public Map<String, TvrDataArrivalInfo> getTvrDataArrivalInfos() {
        if (tvrDataArrivalInfos == null) {
            String arrivalInfoJson =
                    this.getConfig().get(PROGRESSIVE_EXP_DATA_ARRIVAL_INFO);
            if (arrivalInfoJson == null) {
                this.tvrDataArrivalInfos = Collections.emptyMap();
            } else {
                Gson gson = new GsonBuilder().setLenient().create();
                this.tvrDataArrivalInfos = gson.fromJson(arrivalInfoJson,
                        new TypeToken<Map<String, TvrDataArrivalInfo>>() {}.getType());
            }
        }
        return tvrDataArrivalInfos;
    }

    public TvrRangeQueryPartitionManager getTvrRangeQueryPartitionManager() {
        return tvrRangeQueryPartitionManager;
    }

    // Progressive Instants
    private long[] tvrVersions;

    public long[] getTvrVersions() {
        if (tvrVersions == null) {
//            if (TvrUtils.progressiveRangeQueryOptEnabled(this)) {
//                // for range query optimization
//                tvrVersions = parseTvrVersionsByTablePartitions();
//            } else
            if (TvrUtils.progressiveReoptimizeEnabled(this)) {
                // for re-optimization
                tvrVersions = new long[2];
                tvrVersions[0] = System.currentTimeMillis();
                tvrVersions[1] = TvrVersion.MAX_TIME;
            } else {
                tvrVersions = parseTvrVersions();
            }
        }
        return tvrVersions;
    }

    private long[] parseTvrVersions() {
        String invSetting = getConfig().get(PROGRESSIVE_INSTANTS);
        if (invSetting == null) {
            return null;
        }
        return LongStream.concat(
                Arrays.stream(invSetting.split(",")).map(String::trim)
                        .filter(s -> !s.isEmpty()).mapToLong(Long::parseLong),
                LongStream.of(TvrVersion.MAX_TIME)).sorted().toArray();
    }

//    private long[] parseTvrVersionsByTablePartitions() {
//        if (tvrRangeQueryPartitionManager == null) {
//            throw new IllegalArgumentException("TvrRangeQueryPartitionManager does not initialize");
//        }
//
//        Map<String, Map<Long, Set<String>>> partitionVersionMapping =
//                tvrRangeQueryPartitionManager.getPartitionVersionMapping();
//        if (partitionVersionMapping.isEmpty()) {
//            return new long[] { TvrVersion.MAX_TIME };
//        }
//
//        long[] versions = LongStream.concat(partitionVersionMapping.values().stream()
//                        .flatMapToLong(m -> m.keySet().stream().mapToLong(Long::longValue)),
//                LongStream.of(TvrVersion.MAX_TIME)).distinct().sorted().toArray();
//
//        // the lifecycle of each derived table is the maximum time range in all the input tables
//        // when range optimization is enabled.
//        if (versions.length > 2) {
//            // versions.length - 2: skip TvrVersion.MAX_TIME
//            double timeRange = versions[versions.length - 2] - versions[0];
//            TvrTableUtils.LIFECYCLE.set((int) Math.round(timeRange / 1000 / 60 / 60 / 24) + 1);
//        }
//
//        return versions;
//    }

    public void updateCurrentOptimizationTimeIndex(long newOptimizationTime) {
        currentOptimizationTimeIndex = Arrays.binarySearch(getTvrVersions(), newOptimizationTime);
        assert currentOptimizationTimeIndex >= 0;
    }

    public boolean addNewVersion(long newVersion) {
        assert tvrVersions != null;

        int insertIndex = Arrays.binarySearch(getTvrVersions(), newVersion);
        if (insertIndex >= 0 || getVersionDim() == 0) {
            return false;
        }

        insertIndex = -insertIndex - 1;
        tvrVersions = TvrUtils.addVersion(newVersion, tvrVersions, insertIndex);
        // update default tvr meta set type
        defaultTvrType.addNewVersion(newVersion);
        return true;
    }

    public PlannerMetricsListener getListener() {
        if (listener == null) {
            listener = new PlannerMetricsListener(this);
        }
        return listener;
    }
}
