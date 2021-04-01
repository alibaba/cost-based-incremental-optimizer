package org.apache.calcite.rel.temp;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableSink;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * temporary class to be compatible with code that uses the table sink operator
 * In Calcite, there is no corresponding table sink operator to represent a INSERT INTO TABLE operation
 */
public class PhysicalTableSink extends TableSink implements EnumerableRel {

    // adopted from ListTransientTable in Calcite master branch
    public static class TvrListTable extends AbstractQueryableTable implements ModifiableTable, ScannableTable {
        public static final Type TYPE = Object[].class;
        public final List rows = new ArrayList();
        public final String name;
        public final RelDataType protoRowType;

        public TvrListTable(String name, RelDataType rowType) {
            super(TYPE);
            this.name = name;
            this.protoRowType = rowType;
        }

        public List getRows() {
            return this.rows;
        }

        @Override public TableModify toModificationRel(
                RelOptCluster cluster,
                RelOptTable table,
                Prepare.CatalogReader catalogReader,
                RelNode child,
                TableModify.Operation operation,
                List<String> updateColumnList,
                List<RexNode> sourceExpressionList,
                boolean flattened) {
            return LogicalTableModify.create(table, catalogReader, child, operation,
                    updateColumnList, sourceExpressionList, flattened);
        }

        @Override public Collection getModifiableCollection() {
            return rows;
        }

        @Override public Enumerable<Object[]> scan(DataContext root) {
            // add the table into the schema, so that it is accessible by any potential operator
            if (root.getRootSchema() != null) {
                root.getRootSchema().add(name, this);
            }

            final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

            return new AbstractEnumerable<Object[]>() {
                @Override public Enumerator<Object[]> enumerator() {
                    return new Enumerator<Object[]>() {
                        //                        private final List list = new ArrayList(rows);
                        private int i = -1;

                        // TODO cleaner way to handle non-array objects?
                        @Override public Object[] current() {
                            Object current = rows.get(i);
                            return current != null && current.getClass().isArray()
                                    ? (Object[]) current
                                    : new Object[]{current};
                        }

                        @Override public boolean moveNext() {
                            if (cancelFlag != null && cancelFlag.get()) {
                                return false;
                            }

                            return ++i < rows.size();
                        }

                        @Override public void reset() {
                            i = -1;
                        }

                        @Override public void close() {
                        }
                    };
                }
            };
        }

        @Override public Expression getExpression(SchemaPlus schema, String tableName,
                                                  Class clazz) {
            return Schemas.tableExpression(schema, elementType, tableName, clazz);
        }

        @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                                      SchemaPlus schema, String tableName) {
            return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
                @Override public Enumerator<T> enumerator() {
                    //noinspection unchecked
                    return (Enumerator<T>) Linq4j.enumerator(rows);
                }
            };
        }

        @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.copyType(protoRowType);
        }

        @Override public Type getElementType() {
            return TYPE;
        }
    };

    public String projName;
    public String tableName;
    public TvrListTable tvrListTable;
    public List rows;
    public RelOptTable relOptTable;

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param traits
     * @param input   Input relational expression
     */
    public PhysicalTableSink(RelTraitSet traits, RelNode input, String projName, String tableName) {
        super(input.getCluster(), traits, input);
        this.tvrListTable = new TvrListTable(tableName, input.getRowType());
        this.rows = tvrListTable.rows;
        this.projName = projName;
        this.tableName = tableName;

        CalciteCatalogReader schema = TvrUtils.getRootRelOptSchema(getCluster());
        SchemaPlus schemaPlus = schema.getRootSchema().plus();
        schemaPlus.add(tableName, this.tvrListTable);


        this.relOptTable =  RelOptTableImpl.create(
                schema, this.input.getRowType(), this.tvrListTable, ImmutableList.of(schemaPlus.getName(), tableName),
                clazz -> Schemas.tableExpression(schemaPlus, Object[].class, tableName, clazz));
    }

    public RelOptTable getTable() {
        return relOptTable;
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        BlockBuilder builder = new BlockBuilder();

        RelNode input = getInput();
        Result inputResult = implementor.visitChild(this, 0, (EnumerableRel) input, pref);

        Expression tableExp = Expressions.convert_(
                Expressions.call(
                        Expressions.call(
                                implementor.getRootExpression(),
                                BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method),
                        BuiltInMethod.SCHEMA_GET_TABLE.method,
                        Expressions.constant(tableName, String.class)),
                ModifiableTable.class);
        Expression collectionExp = Expressions.call(
                tableExp,
                BuiltInMethod.MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION.method);

        Expression inputExp = builder.append("input", inputResult.block);

        Expression spoolExp = Expressions.call(
                BuiltInMethod.TVR_PHYSICAL_TABLE_SINK.method,
                collectionExp,
                inputExp);
        builder.add(spoolExp);

        PhysType physType = PhysTypeImpl.of(
                implementor.getTypeFactory(),
                getRowType(),
                pref.prefer(inputResult.format));
        return implementor.result(physType, builder.toBlock());
    }
}
