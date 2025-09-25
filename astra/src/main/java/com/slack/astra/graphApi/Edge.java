package com.slack.astra.graphApi;

import static com.google.common.base.Preconditions.checkNotNull;

// Represents a directed connection between two Nodes.
public record Edge(String sourceNodeId, String targetNodeId) {
  public Edge {
    checkNotNull(sourceNodeId, "sourceNodeId cannot be null");
    checkNotNull(targetNodeId, "targetNodeId cannot be null");
  }
}
