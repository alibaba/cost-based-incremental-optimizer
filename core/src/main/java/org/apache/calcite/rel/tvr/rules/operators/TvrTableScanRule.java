package org.apache.calcite.rel.tvr.rules.operators;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.tvr.TvrSetSnapshot;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.RelSet;
import org.apache.calcite.plan.volcano.TvrMetaSetType;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.TvrLogicalTableScan;
import org.apache.calcite.rel.tvr.TvrVolcanoPlanner;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.utils.TimeInterval;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrTableUtils;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.*;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.VersionInterval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * This rule is part of the Progressive Computing Prototype.
 * This rule converts a LogicalTableScan that outputs SNAPSHOT to DELTA.
 * The conversion is driven by AbstractConverter
 */
public class TvrTableScanRule extends RelOptTvrRule {
  public static TvrTableScanRule INSTANCE = new TvrTableScanRule(false, false);
  public static TvrTableScanRule SAMPLE_INSTANCE = new TvrTableScanRule(true, false);
  public static TvrTableScanRule COPY_INSTANCE = new TvrTableScanRule(false, true);

  private final boolean sample;
  private final boolean copy;

  private TvrTableScanRule(boolean sample, boolean copy) {
    super(operand(LogicalTableScan.class, s -> !TvrTableUtils.isDerivedTable(s),
        tvrEdgeSSMax(tvr()), none()),
        "TvrTableScanRule-" + (sample ? "sample" : "full") + (copy ? "-copy" : ""));
    this.sample = sample;
    this.copy = copy;
  }

  /**
   * <p>
   * Matches:
   *   LogicalTableScan  --Any--  Tvr
   * <p>
   * Converts to:
   *                                   (Set Delta, +/- or +Only)
   *   LogicalTableScan  --Any--  Tvr  ----------------------  LogicalTableScan
   * <p>
   * The result LogicalTableScan could be either Positive/Negative or Positive-Only,
   * depending on the table scanned.
   * If the table is an append-only table, then it produces Positive-Only set delta.
   * <p>
   */
  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalTableScan tableScan = getRoot(call).get();
    TvrContext ctx = TvrContext.getInstance(tableScan.getCluster());

    transformBaseRelation(call, tableScan, ctx);

  }

  private void transformBaseRelation(RelOptRuleCall call,
                                     LogicalTableScan ts, TvrContext ctx) {
    TvrMetaSetType tvrType = getRoot(call).tvr().get().getTvrType();
    RelOptCluster cluster = ts.getCluster();
    RelBuilder builder = call.builder();
    final RelOptTable relOptTable = ts.getTable();

    VersionInterval[] deltas = tvrType.getDeltas();
    TvrVersion[] snapshots = tvrType.getSnapshots();

    if (sample) {
      deltas =  deltas.length == 0 ? deltas : Arrays.copyOfRange(deltas, 0, 1);
      snapshots = Arrays.copyOfRange(snapshots, 0, 1);
    }

    VersionInterval deltaSeed = null;
    TvrVersion snapshotSeed = null;
    TvrVolcanoPlanner planner = null;
    if (copy) {
      deltaSeed = deltas.length == 0 ? null : deltas[0];
      snapshotSeed = snapshots[0];
      planner = (TvrVolcanoPlanner) call.getPlanner();
    }

    boolean appendOnly = TvrUtils.isBaseTableAppendOnly(ts.getTable());

    if (TvrUtils.isDimTable(ctx, ts)) {
      // snapshot is always the same
      for (TvrVersion s : snapshots) {
        transformToRootTvr(call, ts, new TvrSetSnapshot(s));
        if (copy && s.equals(snapshotSeed)) {
          RelSet seed = planner.getSet(planner.ensureRegistered(ts, null));
          planner.copyHelper.seed(seed, tvrType);
        }
      }
      // delta is always empty
      RelNode empty = builder.values(ts.getRowType()).build();
      for (VersionInterval d : deltas) {
        transformToRootTvr(call, empty, new TvrSetDelta(d.from, d.to, true));
        if (copy && d.equals(deltaSeed)) {
          RelSet seed = planner.getSet(planner.ensureRegistered(empty, null));
          planner.copyHelper.seed(seed, tvrType);
        }
      }
    } else {
      int ord = ctx.tableOrdinal(ts);
      for (TvrVersion s : snapshots) {
        long scanTo = s.get(ord);
        TimeInterval inv = TimeInterval.of(TvrVersion.MIN_TIME, scanTo);
        RelNode tableScan = createTableScan(relOptTable, inv, builder, cluster);
        transformToRootTvr(call, tableScan, new TvrSetSnapshot(s));
        if (copy && s.equals(snapshotSeed)) {
          RelSet seed = planner.getSet(planner.ensureRegistered(tableScan, null));
          planner.copyHelper.seed(seed, tvrType);
        }
      }
      for (VersionInterval d : deltas) {
        long scanFrom = d.from.get(ord);
        long scanTo = d.to.get(ord);
        TimeInterval inv = TimeInterval.of(scanFrom, scanTo);
        RelNode tableScan = createTableScan(relOptTable, inv, builder, cluster);
        transformToRootTvr(call, tableScan, new TvrSetDelta(d.from, d.to, appendOnly));
        if (copy && d.equals(deltaSeed)) {
          RelSet seed = planner.getSet(planner.ensureRegistered(tableScan, null));
          planner.copyHelper.seed(seed, tvrType);
        }
      }
    }
  }

  public static RelNode createTableScan(RelOptTable table, TimeInterval inv, RelBuilder relBuilder, RelOptCluster cluster) {
    if (table instanceof RelOptTableImpl) {
      RelOptTableImpl relOptTable = (RelOptTableImpl) table;
      RelDataType rowType = relOptTable.getRowType();
      if (relOptTable.getTable() instanceof ReflectiveSchema.FieldTable) {
        ReflectiveSchema.FieldTable<?> fieldTable = (ReflectiveSchema.FieldTable<?>) ((RelOptTableImpl) table).getTable();
        Enumerable enumerable = fieldTable.getEnumerable();
        Pair<Integer, Integer> subListIndex = getSubListIndex(enumerable.count(), inv, cluster);
        Enumerable subEnumerable = Linq4j.asEnumerable(enumerable.toList().subList(subListIndex.left, subListIndex.right));
//        Enumerable subEnumerable = enumerable.skip(subListIndex.left).take(subListIndex.right - subListIndex.left);

        ReflectiveSchema.FieldTable<?> newFieldTable = new ReflectiveSchema.FieldTable<>(
                fieldTable.getField(), fieldTable.getElementType(), subEnumerable, fieldTable.getStatistic());

        CalciteCatalogReader rootSchema = TvrUtils.getRootRelOptSchema(cluster);
        String tableName = "_tvr_" + relOptTable.getQualifiedName().get(1) + "_" + inv.toString();
        SchemaPlus schemaPlus = rootSchema.getRootSchema().plus();
        schemaPlus.add(tableName, newFieldTable);

        Double newRowCount = relOptTable.getRowCount() * ((subListIndex.right - subListIndex.left) / (double) enumerable.count());
        relOptTable = relOptTable.copy(newFieldTable).copy(inv).copy(newRowCount);
        relOptTable = RelOptTableImpl.create(rootSchema, rowType, newFieldTable,
                ImmutableList.of(relOptTable.getQualifiedName().get(0), tableName),
                clazz -> Schemas.tableExpression(schemaPlus, newFieldTable.getElementType(), tableName, clazz),
                newRowCount, inv);

        return inv.from == inv.to ?
                relBuilder.values(table.getRowType()).build() :
                TvrLogicalTableScan.create(cluster, relOptTable, inv);
      }
      throw new UnsupportedOperationException("internal table of type " + relOptTable.getTable().getClass() + " is not supported yet");
    }
    throw new UnsupportedOperationException("table of type " + table.getClass() + " is not supported yet");
  }

  public static Pair<Integer, Integer> getSubListIndex(int size, TimeInterval inv, RelOptCluster cluster) {
    TvrContext ctx = TvrContext.getInstance(cluster);
    long[] tvrVersions = ctx.getDefaultTvrType().getTvrVersions();
    int versionFrom = inv.from == TvrVersion.MIN_TIME ? 0 : Arrays.binarySearch(tvrVersions, inv.from) + 1;
    int versionTo = inv.to == TvrVersion.MIN_TIME ? 0 : Arrays.binarySearch(tvrVersions, inv.to) + 1;

    int startIndex = versionFrom * (size / tvrVersions.length);
    int toIndex = versionTo == tvrVersions.length ? size : versionTo * (size / tvrVersions.length);

    return Pair.of(startIndex, toIndex);
  }

  public static boolean satisfyVersion(Map<Integer, Integer> dimMapping,
      long[] oldTvrVersions, TvrVersion oldTvrVersion,
      TvrVersion newTvrVersion) {
    if (oldTvrVersion.equals(TvrVersion.MAX) || newTvrVersion.equals(TvrVersion.MAX)) {
      return oldTvrVersion.equals(TvrVersion.MAX) && newTvrVersion.equals(TvrVersion.MAX);
    }
    if (oldTvrVersion.equals(TvrVersion.MIN) || newTvrVersion.equals(TvrVersion.MIN)) {
      return oldTvrVersion.equals(TvrVersion.MIN) && newTvrVersion.equals(TvrVersion.MIN);
    }

    for (int i = 0; i < oldTvrVersion.getVersions().length; i++) {
      long oldVersion = oldTvrVersion.getVersions()[i];
      int oldVersionIndex = Arrays.binarySearch(oldTvrVersions, oldVersion);
      long oldVersionNext = oldTvrVersions[oldVersionIndex + 1];

      long newVersion = newTvrVersion.getVersions()[dimMapping.get(i)];
      if (! (newVersion >= oldVersion && newVersion < oldVersionNext)) {
        return false;
      }
    }
    return true;
  }

  /**
   * A dummy relNode without physical implementation, and thus has a cost of
   * infinity.
   */
  public static class DummyNode extends AbstractRelNode {
    private static int nextId = 0;

    // Used in digest so that calcite will not merge dummy nodes
    private final int id;

    public DummyNode(RelOptCluster cluster, RelDataType rowType) {
      this(cluster, rowType, cluster.traitSet(), nextId++);
    }

    public DummyNode(RelOptCluster cluster, RelDataType rowType,
        RelTraitSet traitSet, int id) {
      super(cluster, traitSet);
      this.id = id;
      this.rowType = rowType;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
      return super.explainTerms(pw).item("id", id);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      if (traitSet == getTraitSet()) {
        return this;
      }
      return new DummyNode(getCluster(), getRowType(), traitSet, id);
    }
  }

}
