package com.slack.astra.graphApi;

import java.util.Locale;

/** Represents an edge between two Nodes. */
public final class Edge {
  String parent, child;

  Edge(Builder builder) {
    parent = builder.parent;
    child = builder.child;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public String parent() {
    return parent;
  }

  public String child() {
    return child;
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  public static final class Builder {
    String parent, child;

    Builder() {}

    Builder(Edge source) {
      this.parent = source.parent;
      this.child = source.child;
    }

    public Builder parent(String parent) {
      if (parent == null) throw new NullPointerException("parent == null");
      this.parent = parent.toLowerCase(Locale.ROOT);
      return this;
    }

    public Builder child(String child) {
      if (child == null) throw new NullPointerException("child == null");
      this.child = child.toLowerCase(Locale.ROOT);
      return this;
    }

    public Edge build() {
      String missing = "";
      if (parent == null) missing += " parent";
      if (child == null) missing += " child";
      if (!missing.isEmpty()) throw new IllegalStateException("Missing :" + missing);

      return new Edge(this);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Edge)) return false;
    Edge that = (Edge) o;
    return parent.equals(that.parent) && child.equals(that.child);
  }

  @Override
  public int hashCode() {
    int h = 1;
    h *= 1000003;
    h ^= parent.hashCode();
    h *= 1000003;
    h ^= child.hashCode();

    return h;
  }
}
