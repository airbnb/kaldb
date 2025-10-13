package com.slack.astra.graphApi;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.SortedMap;

// Represents a directed connection between two Nodes.
public record Edge(String sourceNodeId, String targetNodeId, SortedMap<String, String> metadata) {
  public Edge {
    checkNotNull(sourceNodeId, "sourceNodeId cannot be null");
    checkNotNull(targetNodeId, "targetNodeId cannot be null");
  }
}
