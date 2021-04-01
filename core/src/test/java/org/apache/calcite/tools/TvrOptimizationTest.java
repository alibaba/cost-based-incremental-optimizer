package org.apache.calcite.tools;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.tvr.rules.TvrRuleCollection;
import org.apache.calcite.rel.tvr.utils.DotPrinter;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/**
 * Demonstration program to run the Tempura optimizer.
 * This program produces a progressive physical plan by the Tempura optimizer that runs across several time points.
 * The physical plan is printed out to the console in DOT format, which can be viewed using an online graphviz tool.
 */
public class TvrOptimizationTest {

    // the early progressive run time points besides the original final run
    // for example, if the original query runs at time 24:00
    // 28800000 represents a time point at 8:00, 57600000 represents a time point at 16:00
    // there will be 2 early runs that produce partial results, and 1 final run that produces the final result
    //
    public static String instants = "28800000, 57600000";

    @Test
    public void testSumAgg() throws Exception {
        String query = "select deptno, sum(salary) from \"hr\".\"emps\" group by deptno\n";
        testQuery(query);
    }

    @Test
    public void testSumCountAgg() throws Exception {
        String query = "select deptno, sum(salary), count(salary) from \"hr\".\"emps\" group by deptno\n";
        testQuery(query);
    }

    @Test
    public void testAvgAgg() throws Exception {
        String query = "select deptno, avg(salary) from \"hr\".\"emps\" group by deptno\n";
        testQuery(query);
    }

    @Test
    public void testJoin() throws Exception {
        String query = "select *\n"
                + "from hr.emps as e\n"
                + "join hr.depts as d\n"
                + "  on e.deptno = d.deptno\n";
        testQuery(query);

    }

    public void testQuery(String query) throws Exception {

        // must set ENABLE_ENUMERABLE in CalcitePrepareImpl to false
        // to prevent directly generating EnumerableTableScan instead of LogicalTableScan
        System.getProperties().setProperty("calcite.enable.enumerable", "false");

        TvrContext tvrContext = new TvrContext();
        tvrContext.getConfig().set(TvrUtils.PROGRESSIVE_ENABLE, "true");
        tvrContext.getConfig().set(TvrUtils.ENABLE_VOLCANO_VISUALIZER, "false");
        tvrContext.getConfig().set(TvrUtils.PROGRESSIVE_INSTANTS, instants);

        tvrContext.getConfig().set(TvrUtils.PROGRESSIVE_META_AVAILABLE, "false");
        tvrContext.getConfig().set(TvrUtils.PROGRESSIVE_TUPLE_NO_DUPLICATE, "true");
        tvrContext.getConfig().set(TvrUtils.PROGRESSIVE_VIRTUAL_TABLESINK, "false");

        tvrContext.getConfig().set(TvrUtils.PROGRESSIVE_REQUIRE_OUTPUT_VIEW, "true");

        Program hepProgram = Programs.hep(
                ImmutableList.of(
                        JoinProjectTransposeRule.RIGHT_PROJECT,
                        JoinProjectTransposeRule.LEFT_PROJECT,
                        FilterJoinRule.FilterIntoJoinRule.FILTER_ON_JOIN,
                        ProjectRemoveRule.INSTANCE,
                        ProjectMergeRule.INSTANCE,
                        JoinToMultiJoinRule.INSTANCE),
                false,
                DefaultRelMetadataProvider.INSTANCE);
        Program tvrProgram = new Programs.TvrRuleSetProgram(RuleSets.ofList(TvrRuleCollection.tvrStandardRuleSet()));

        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(
                        CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.HR))
                .traitDefs(
                        ConventionTraitDef.INSTANCE
//                        ,RelCollationTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE
                )
//                .programs(Programs.standard())
                .programs(hepProgram, tvrProgram)
                .context(Contexts.of(tvrContext))
                .build();
        Planner planner = Frameworks.getPlanner(config);

        SqlNode parse = planner.parse(query);
        SqlNode validate = planner.validate(parse);
        RelNode rel = planner.rel(validate).project();
        RelNode hepOutput = planner.transform(0, rel.getCluster().traitSet(), rel);
        RelNode volcanoOutput = planner.transform(1, rel.getCluster().traitSet().replace(EnumerableConvention.INSTANCE), hepOutput);


        System.out.println(DotPrinter.getDot(volcanoOutput));

    }




}
