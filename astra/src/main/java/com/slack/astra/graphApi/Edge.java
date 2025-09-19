package com.slack.astra.graphApi;

// Represents a directed connection between two Nodes.
public final class Edge {
  private final String sourceNodeId, targetNodeId;

  Edge(String sourceNodeId, String targetNodeId) {
    if (sourceNodeId == null) throw new NullPointerException("sourceNodeId == null");
    if (targetNodeId == null) throw new NullPointerException("targetNodeId == null");
    this.sourceNodeId = sourceNodeId;
    this.targetNodeId = targetNodeId;
  }

  public String getSourceNodeId() {
    return this.sourceNodeId;
  }

  public String getTargetNodeId() {
    return this.targetNodeId;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Edge that)) return false;
    return sourceNodeId.equals(that.sourceNodeId) && targetNodeId.equals(that.targetNodeId);
  }

  @Override
  public int hashCode() {
    int h = 1;
    h *= 1000003;
    h ^= sourceNodeId.hashCode();
    h *= 1000003;
    h ^= targetNodeId.hashCode();

    return h;
  }
}
