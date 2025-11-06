package com.slack.astra.graphApi;

import java.util.List;
import org.apache.commons.lang3.StringUtils;

public record Graph(List<Node> nodes, List<Edge> edges) {
  @Override
  public String toString() {
    return "Graph{\n"
        + "  nodes("
        + nodes.size()
        + ")=\n  "
        + StringUtils.join(nodes, ",\n  ")
        + ",\nedges("
        + edges.size()
        + ")=\n  "
        + StringUtils.join(edges, ",\n  ")
        + '}';
  }
}
