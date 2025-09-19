package com.slack.astra.graphApi;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Graph {
  List<Node> nodes;
  Set<Edge> edges;

  public Graph(ArrayList<Node> nodes, Set<Edge> edges) {
    this.nodes = nodes;
    this.edges = edges;
  }
}
