package com.slack.astra.graphApi;

import com.slack.astra.zipkinApi.ZipkinSpanResponse;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
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

  /**
   * Constructs a GraphBuilder with the specified configuration.
   *
   * @param config The GraphConfig to use for node metadata extraction. If GraphConfig.DEFAULT, uses
   *     service name from a span's remote endpoint.
   */
  public GraphBuilder(GraphConfig config) {
    this.config = config;
  }

  /**
   * Builds a dependency graph from a list of Zipkin spans.
   *
   * <p>This method processes spans to create nodes (services) and edges (dependencies) representing
   * the service communication graph. Each span becomes a node, and parent-child relationships
   * between spans become edges in the graph. Logs warnings for any missing parent or child nodes.
   *
   * @param spans List of Zipkin spans to process
   * @return Graph containing nodes and edges representing service dependencies
   */
  public Graph buildFromSpans(List<ZipkinSpanResponse> spans) {
    // First pass: build mapping between spanId -> Node
    Map<String, Node> spanIdToNode =
        spans.stream()
            .filter(span -> span.getId() != null)
            .collect(Collectors.toMap(ZipkinSpanResponse::getId, this::createChildNodeFromSpan));

    // Second pass: build mapping between parentNodeId -> list of childNodeIds
    Map<String, List<String>> parentNodeIdToChildNodeIds =
        spans.stream()
            .filter(span -> span.getId() != null && span.getParentId() != null)
            .map(
                span -> {
                  Node parentNode = spanIdToNode.get(span.getParentId());
                  Node childNode = spanIdToNode.get(span.getId());

                  /*
                  Currently, we're dropping edges where the parent or child is missing. There might be cases where we
                  want to retain one-sided edges for diagnostic purposes or attach warnings/errors to the graph itself,
                  in which case this logic will need updating.
                  */
                  if (parentNode != null && childNode != null) {
                    return Map.entry(parentNode.getId(), childNode.getId());
                  } else {
                    LOG.warn(
                        "Missing parent or child node for parentSpanId={} and childSpanId={}",
                        span.getParentId(),
                        span.getId());
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .collect(
                Collectors.groupingBy(
                    Map.Entry::getKey,
                    Collectors.mapping(Map.Entry::getValue, Collectors.toList())));

    // Create edges from parent-child relationships
    Set<Edge> edges = buildEdges(parentNodeIdToChildNodeIds);
    // Dedupe nodes
    Set<Node> nodes = new HashSet<>(spanIdToNode.values());

    return new Graph(new ArrayList<>(nodes), new ArrayList<>(edges));
  }

  /**
   * Creates a Node from a Zipkin span using configured metadata extraction. Calls out to the
   * config's createMetadataFromSpan function to generate node metadata from a span.
   *
   * @param span The Zipkin span to convert to a node
   * @return Node with metadata extracted from the span
   */
  private Node createChildNodeFromSpan(ZipkinSpanResponse span) {
    return new Node(config.createMetadataFromSpan(span));
  }

  /**
   * Builds edges representing parent-child relationships between nodes.
   *
   * <p>Creates edges by connecting parent nodes to their child nodes, establishing the dependency
   * relationships in the service graph.
   *
   * @param parentNodeIdToChildNodeIds parentNodeIds and their list of childNodeIds
   * @return Set of edges representing service dependencies
   */
  private Set<Edge> buildEdges(Map<String, List<String>> parentNodeIdToChildNodeIds) {
    return parentNodeIdToChildNodeIds.entrySet().stream()
        .flatMap(
            entry ->
                entry.getValue().stream().map(childNodeId -> new Edge(entry.getKey(), childNodeId)))
        .collect(Collectors.toSet());
  }
}
