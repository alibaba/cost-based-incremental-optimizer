package org.apache.calcite.tools.visualizer;

import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class VisualizerTvrInfo {

  public String tvrId;
  // Map<TvrSemantics, RelSet>
  public Map<String, Collection<String>> tvrSets;
  // Map<TvrProperty, TvrMetaSet>
  public Map<String, Collection<String>> tvrPropertyLinks;

  public VisualizerTvrInfo() {}

  public VisualizerTvrInfo(String tvrId,
      Map<String, Collection<String>> tvrSets,
      Map<String, Collection<String>> tvrPropertyLinks) {
    this.tvrId = tvrId;
    this.tvrSets = tvrSets;
    this.tvrPropertyLinks = tvrPropertyLinks;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof VisualizerTvrInfo))
      return false;
    VisualizerTvrInfo that = (VisualizerTvrInfo) o;
    return tvrId == that.tvrId && Objects.equals(tvrSets, that.tvrSets)
        && Objects.equals(tvrPropertyLinks, that.tvrPropertyLinks);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tvrId, tvrSets, tvrPropertyLinks);
  }
}
