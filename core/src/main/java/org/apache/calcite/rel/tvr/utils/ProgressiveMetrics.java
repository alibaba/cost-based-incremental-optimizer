package org.apache.calcite.rel.tvr.utils;

import org.apache.calcite.plan.volcano.PlannerMetricsListener;
import org.apache.calcite.plan.volcano.VolcanoPlannerPhase;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;


public class ProgressiveMetrics {

  static Logger LOG = CalciteTrace.getPlannerTracer();

  private long startFindBestExp;
  private long findBestExpMillis;

  private long startMultiQueryOpt;
  private long multiQueryOptMillis;

  private long startInferMissingTvr;
  private long inferMissingTvrMillis;

  private long startAnalyzePatterns;
  private long analyzePatternsMillis;

  private long startCopy;
  private long copyMillis;

  private long startPropagateCost;
  private long propagateCostMillis;

  private Map<VolcanoPlannerPhase, Long> phaseStartTime = new LinkedHashMap<>();
  private Map<VolcanoPlannerPhase, Long> phaseTime = new LinkedHashMap<>();

  private int rulesFired;

  private int relSetCountTotal;
  private int relNodeCountTotal;
  private int reuseCandidateNum;
  private int matNum;

  public TreeMap<Class<? extends RelNode>, Integer> relNodeCount =
      new TreeMap<>(Comparator.comparing(Class::getSimpleName));

  public ProgressiveMetrics() {
  }

  public void startFindBestExp() {
    startFindBestExp = System.currentTimeMillis();
  }

  public void endFindBestExp() {
    findBestExpMillis = System.currentTimeMillis() - startFindBestExp;
  }

  public long getFindBestExpMillis() {
    return findBestExpMillis;
  }

  public void startMultiQueryOpt() {
    startMultiQueryOpt = System.currentTimeMillis();
  }

  public void endMultiqueryOpt() {
    multiQueryOptMillis = System.currentTimeMillis() - startMultiQueryOpt;
  }

  public void startInferMissingTvr() {
    startInferMissingTvr = System.currentTimeMillis();
  }

  public void endInferMissingTvr() {
    inferMissingTvrMillis = System.currentTimeMillis() - startInferMissingTvr;
  }

  public void startAnalyzePatterns() {
    startAnalyzePatterns = System.currentTimeMillis();
  }

  public void endAnalyzePatterns() {
    analyzePatternsMillis = System.currentTimeMillis() - startAnalyzePatterns;
  }

  public void startCopy() {
    startCopy = System.currentTimeMillis();
  }

  public void endCopy() {
    copyMillis = System.currentTimeMillis() - startCopy;
  }

  public void startPropagateCost() {
    startPropagateCost = System.currentTimeMillis();
  }

  public void endPropagateCost() {
    propagateCostMillis = System.currentTimeMillis() - startPropagateCost;
  }

  public void startPhase(VolcanoPlannerPhase phase) {
    phaseStartTime.put(phase, System.currentTimeMillis());
  }

  public void endPhase(VolcanoPlannerPhase phase) {
    phaseTime.put(phase, System.currentTimeMillis() - phaseStartTime.get(phase));
  }

  public void setReuseCandidateNum(int reuseCandidateNum) {
    this.reuseCandidateNum = reuseCandidateNum;
  }

  public void setMatNum(int matNum) {
    this.matNum = matNum;
  }

  public long getMultiQueryOptMillis() {
    return multiQueryOptMillis;
  }

  public void fetchRuleMatchData(PlannerMetricsListener metricsListener) {
    rulesFired = metricsListener.numAttempts();
  }

  public int getRulesFired() {
    return rulesFired;
  }

  public void setRelSetCountTotal(int relSetCountTotal) {
    this.relSetCountTotal = relSetCountTotal;
  }

  public void setRelNodeCountTotal(int relNodeCountTotal) {
    this.relNodeCountTotal = relNodeCountTotal;
  }

  public String summarize(Config optimizerConfig) {
    String queryName = optimizerConfig
        .get(TvrUtils.PROGRESSIVE_METRICS_QUERY_NAME, "query");

    /*
    int logicalJoinCount = relNodeCount.entrySet().stream().filter(
        entry -> Join.class.isAssignableFrom(entry.getKey())
            || LogicalUserDefinedMultiJoin.class
            .isAssignableFrom(entry.getKey()))
        .mapToInt(entry -> entry.getValue()).sum();
    int physicalJoinCount = relNodeCount.entrySet().stream().filter(
        entry -> OdpsMultiJoin.class.isAssignableFrom(entry.getKey())
            || UserDefinedMultiJoin.class.isAssignableFrom(entry.getKey()))
        .mapToInt(entry -> entry.getValue()).sum();
    */

    int joinCount = relNodeCount.entrySet().stream()
        .filter(entry -> entry.getKey().getSimpleName().endsWith("Join"))
        .mapToInt(entry -> entry.getValue()).sum();

    int aggCount = relNodeCount.entrySet().stream()
        .filter(entry -> entry.getKey().getSimpleName().endsWith("Aggregate"))
        .mapToInt(entry -> entry.getValue()).sum();

    StringBuilder builder = new StringBuilder();
    builder.append(queryName + "\t");
    builder.append(findBestExpMillis + "\t");
    builder.append(multiQueryOptMillis + "\t");
    builder.append(rulesFired + "\t");
    builder.append(relSetCountTotal + "\t");
    builder.append(relNodeCountTotal + "\t");

    builder.append(joinCount + "\t");
    builder.append(aggCount + "\t");
    builder.append(reuseCandidateNum + "\t");
    builder.append(matNum + "\t");
    builder.append("infer=" + inferMissingTvrMillis + "\t");
    builder.append("analyze=" + analyzePatternsMillis + "\t");
    builder.append("kickOff=" + copyMillis + "\t");
    builder.append("propagateCosts=" + propagateCostMillis + "\t");
    phaseTime.forEach((k, v) -> builder.append(k.toString()).append("=")
        .append(v.toString()).append("\t"));
    builder.append("\n");

    relNodeCount.forEach((clazz, count) -> System.out
        .println(clazz.getSimpleName() + ": " + count));
    System.out.println("\nJoinCount: " + joinCount);
    System.out.println("AggregateCount: " + aggCount);
    System.out.println("Progressive Metrics summary:\n" + builder.toString());

    String summary = builder.toString();
    if (optimizerConfig
        .getBool(TvrUtils.PROGRESSIVE_METRICS_OUTPUT_ENABLE, false)) {
      Path targetPath =
          Paths.get(TvrUtils.getProgressiveMetricsOutputPath(optimizerConfig));

      try {
        FileOutputStream timerOut;
        if (!targetPath.toFile().exists()) {
          Files.write(targetPath, "".getBytes());
        }
        timerOut = new FileOutputStream(targetPath.toString(), true);
        timerOut.write(summary.getBytes());
        timerOut.close();
      } catch (IOException e) {
        LOG.error(
            "Fail to write metrics to file " + targetPath, e);
      }
    }

    return summary;
  }

  public static void deleteDefaultMetricsFile() {
    Path targetPath = Paths.get(TvrUtils.PROGRESSIVE_METRICS_OUTPUT_PATH_DEFAULT);
    try {
      if (targetPath.toFile().exists()) {
        Files.delete(targetPath);
      }
    } catch (IOException e) {
      LOG.error("Fail to delete file " + targetPath, e);
    }
  }

  public Map<String, Integer> getNodeDetails() {
    Map<String, Integer> m = new LinkedHashMap<>();
    m.put("relSet num" , relSetCountTotal);
    m.put("relNode num" , relNodeCountTotal);
    m.put("reuse candidate num" , reuseCandidateNum);
    relNodeCount.forEach((clazz, count) -> m.put(clazz.getSimpleName(), count));
    return m;
  }
}
