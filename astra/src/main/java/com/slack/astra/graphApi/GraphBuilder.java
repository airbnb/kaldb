package com.slack.astra.graphApi;

import com.slack.astra.zipkinApi.ZipkinSpanResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GraphBuilder constructs service dependency graphs from Zipkin span data.
 *
 * <p>This class processes distributed tracing spans to build a graph representation showing
 * relationships between services. It creates nodes representing services and edges representing
 * parent-child relationships between spans.
 *
 * <p>The builder supports configurable node metadata extraction through GraphConfig, allowing
 * customization of which span tags are used to populate node metadata.
 */
public class GraphBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(GraphBuilder.class);
  private final GraphConfig config;
  private final Boolean defaultBehaviour;

  /**
   * Constructs a GraphBuilder with the specified configuration.
   *
   * @param config The GraphConfig to use for node metadata extraction. If GraphConfig.DEFAULT, uses
   *     service name from a span's remote endpoint.
   */
  GraphBuilder(GraphConfig config) {
    this.config = config;
    this.defaultBehaviour = this.config == GraphConfig.DEFAULT;
  }

  /**
   * Builds a dependency graph from a list of Zipkin spans.
   *
   * <p>This method processes spans to create nodes (services) and edges (dependencies) representing
   * the service communication graph. Each span becomes a node, and parent-child relationships
   * between spans become edges in the graph.
   *
   * @param spans List of Zipkin spans to process
   * @return Graph containing nodes and edges representing service dependencies
   */
  public Graph buildFromSpans(List<ZipkinSpanResponse> spans) {
    // Storage for unique nodes keyed by node ID
    HashMap<String, Node> nodes = new HashMap<>();
    // Mapping from span ID to the corresponding node ID
    HashMap<String, String> spanIdToNodeId = new HashMap<>();
    // Track parent-child relationships: parent span ID -> list of child node IDs
    HashMap<String, ArrayList<String>> childrenByParentSpan = new HashMap<>();

    // First pass: create nodes and build parent-child relationships
    for (ZipkinSpanResponse span : spans) {
      if (span.getId() == null) {
        LOG.warn("Document={} cannot have missing id", span);
        continue;
      }

      // Create a node from this span's metadata
      Node childNode = createChildNodeFromSpan(span);
      nodes.putIfAbsent(childNode.getId(), childNode);
      spanIdToNodeId.put(span.getId(), childNode.getId());

      // Track parent-child relationships for edge creation
      String parentId = span.getParentId();
      if (parentId != null) {
        childrenByParentSpan
            .computeIfAbsent(parentId, k -> new ArrayList<>())
            .add(childNode.getId());
      }
    }

    // Second pass: create edges from parent-child relationships
    HashSet<Edge> edges = buildEdges(nodes, childrenByParentSpan, spanIdToNodeId);

    return new Graph(new ArrayList<>(nodes.values()), edges);
  }

  /**
   * Creates a Node from a Zipkin span using configured metadata extraction.
   *
   * <p>In default behavior, extracts service name from the remote endpoint on the span. Otherwise,
   * uses the GraphConfig's tag mapping to populate node metadata from span tags.
   *
   * @param span The Zipkin span to convert to a node
   * @return Node with metadata extracted from the span
   */
  private Node createChildNodeFromSpan(ZipkinSpanResponse span) {
    Map<String, String> tags = span.getTags();
    TreeMap<String, String> metadata = new TreeMap<String, String>();

    if (this.defaultBehaviour) {
      metadata.put("service", span.getRemoteEndpoint().getServiceName());
    } else {
      for (String key : this.config.getNodeMetadataTagMapping().keySet()) {
        metadata.put(key, config.resolve(tags, key));
      }
    }

    return new Node(metadata);
  }

  /**
   * Builds edges representing parent-child relationships between nodes.
   *
   * <p>Creates edges by connecting parent spans to their child spans, establishing the dependency
   * relationships in the service graph. Logs warnings for any missing parent or child nodes.
   *
   * @param nodes Map of node IDs to Node objects
   * @param childrenByParentSpan Map of parent span IDs to lists of child node IDs
   * @param spanIdToNodeId Map of span IDs to corresponding node IDs
   * @return Set of edges representing service dependencies
   */
  private HashSet<Edge> buildEdges(
      HashMap<String, Node> nodes,
      HashMap<String, ArrayList<String>> childrenByParentSpan,
      HashMap<String, String> spanIdToNodeId) {
    HashSet<Edge> edges = new HashSet<>();

    // Iterate through every parent
    for (HashMap.Entry<String, ArrayList<String>> entry : childrenByParentSpan.entrySet()) {
      String parentSpanId = entry.getKey();
      // Get the node for the parent span
      String parentNodeId = spanIdToNodeId.get(parentSpanId);
      Node parentNode = parentNodeId != null ? nodes.get(parentNodeId) : null;

      // Process every child of the parent
      for (String childNodeId : entry.getValue()) {
        Node childNode = nodes.get(childNodeId);

        if (parentNode != null && childNode != null) {
          edges.add(new Edge.Builder().parent(parentNode.getId()).child(childNode.getId()).build());
        } else {
          LOG.warn(
              "Missing node for parentSpanId={} (parentNodeId={}) or childNodeId={}",
              parentSpanId,
              parentNodeId,
              childNodeId);
        }
      }
    }

    return edges;
  }
}
