
# Tempura: A General Cost-Based Optimizer Framework for Incremental Data Processing

Tempura is built on top of [Apache Calcite](https://github.com/apache/calcite). For details see our research [paper](http://www.vldb.org/pvldb/vol14/p14-wang.pdf) published in VLDB'20.

To build the project, run
```
mvn clean install -DskipTests -Dcheckstyle.skip=true -Dforbiddenapis.skip=true
```

To demonstrate how Tempura works, we have added the following example programs that can be run directly:

 - `TvrOptimizationTest.java` runs the Tempura optimizer.
    This program produces a progressive physical plan by the Tempura optimizer that runs across several time points.
    The physical plan is printed out to the console in DOT format, which can be viewed using an online graphviz tool.
 - `TvrExecutionTest.java` uses the Tempura optimizer in an end-to-end query.
     This program generates a progressive physical plan and then uses Calcite's built-in executor to run the plan.
     The output at each time point is printed to the console.
