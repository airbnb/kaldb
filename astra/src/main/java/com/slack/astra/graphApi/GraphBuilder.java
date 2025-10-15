package com.slack.astra.graphApi;

import com.slack.astra.zipkinApi.ZipkinSpanResponse;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
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
  GraphBuilder(GraphConfig config) {
    this.config = config;
  }

  /**
   * Filter for selecting nodes/edges in the graph based on metadata criteria.
   *
   * <p>The filter uses OR logic: a node matches if ANY of the filter criteria match. Each filter
   * option is a field name (e.g., "operation", "service") mapped to a list of allowed values for
   * that field.
   *
   * <p>Examples: {"operation": ["http.request"]} - matches nodes with operation="http.request"
   * {"operation": ["http.request", "grpc.request"]} - matches nodes with either operation
   * {"operation": ["http.request"], "service": ["api-gateway.prod"]} - matches nodes with
   * operation="http.request" OR service="api-gateway.prod" {} or null - empty filter matches all
   * nodes (no filtering)
   *
   * <p>The filter checks both node metadata and edge metadata. If a field exists in either, it can
   * be used for filtering.
   *
   * @param options Map of field names to lists of allowed values. If null or empty, all nodes
   *     match.
   */
  public record Filter(Map<String, List<String>> options) {
    public boolean matches(SpanNode node) {
      // Empty or null filter means no filtering - match all nodes
      if (options == null || options.isEmpty()) {
        return true;
      }

      for (Map.Entry<String, List<String>> entry : this.options().entrySet()) {
        String fieldName = entry.getKey();
        List<String> allowedValues = entry.getValue();

        // Skip if allowedValues is null or empty
        if (allowedValues == null || allowedValues.isEmpty()) {
          continue;
        }

        // Get the actual value from node or edge metadata
        String actualValue = node.nodeMetadata().get(fieldName);
        if (actualValue == null) {
          actualValue = node.edgeMetadata().get(fieldName);
        }

        // If field doesn't exist in either metadata, skip this filter option
        if (actualValue == null) {
          continue;
        }

        // Check if actual value matches any of the allowed values
        if (allowedValues.contains(actualValue)) {
          return true;
        }
      }

      return false;
    }
  }

  public record SpanNode(
      String id, SortedMap<String, String> nodeMetadata, SortedMap<String, String> edgeMetadata) {}

  /**
   * Builds a filtered dependency graph from a list of Zipkin spans.
   *
   * <p>This method processes spans to create nodes and edges representing service dependencies,
   * filtered by the provided criteria. It starts DFS from every node matching the filter, creating
   * edges between matching nodes while traversing through non-matching intermediate nodes.
   *
   * @param spans List of Zipkin spans to process
   * @param filter Filter to apply when building the graph
   * @return Graph containing nodes and edges representing filtered service dependencies
   */
  public Graph buildFromSpans(List<ZipkinSpanResponse> spans, Optional<Filter> filter) {
    Map<String, SpanNode> spanIdToSpanNodes = new HashMap<>();
    Map<String, List<SpanNode>> parentSpanIdToChildSpanNodes = new HashMap<>();

    for (ZipkinSpanResponse span : spans) {
      if (span.getId() == null) {
        continue;
      }

      SpanNode node =
          new SpanNode(
              span.getId(),
              config.createMetadataFromSpan(span, GraphConfig.EntityType.NODE),
              config.createMetadataFromSpan(span, GraphConfig.EntityType.EDGE));

      spanIdToSpanNodes.put(span.getId(), node);

      String parentId = span.getParentId();
      if (parentId != null) {
        parentSpanIdToChildSpanNodes.computeIfAbsent(parentId, k -> new ArrayList<>()).add(node);
      }
    }

    Set<Edge> edges = new HashSet<>();
    Set<Node> nodes = new HashSet<>();

    if (filter.isPresent()) {
      // Start DFS from every node matching the filter since there is no guarantee we have a single
      // root in this trace.
      spanIdToSpanNodes.values().stream()
          .filter(node -> filter.get().matches(node))
          .forEach(
              node ->
                  dfsFilter(
                      node,
                      node.id(),
                      filter.get(),
                      edges,
                      nodes,
                      spanIdToSpanNodes,
                      parentSpanIdToChildSpanNodes));
    } else {
      // No filter - build graph with all edges and collect nodes along the way
      spanIdToSpanNodes.values().stream()
          .forEach(
              node -> {
                List<SpanNode> children =
                    parentSpanIdToChildSpanNodes.getOrDefault(node.id(), List.of());
                children.stream()
                    .forEach(
                        childNode -> {
                          Node source = new Node(node.nodeMetadata());
                          Node target = new Node(childNode.nodeMetadata());
                          nodes.add(source);
                          nodes.add(target);
                          edges.add(
                              new Edge(source.getId(), target.getId(), childNode.edgeMetadata()));
                        });
              });
    }

    return new Graph(new ArrayList<>(nodes), new ArrayList<>(edges));
  }

  /**
   * State object for iterative DFS traversal.
   *
   * @param node The current span node being processed
   * @param lastViableNodeId ID of the most recent node that matched the filter
   */
  private record DfsState(SpanNode node, String lastViableNodeId) {}

  /**
   * Performs iterative DFS to find edges between nodes matching the filter.
   *
   * <p>This method explores the span tree starting from a given node, creating edges only between
   * nodes that match the filter criteria. Non-matching intermediate nodes are traversed but don't
   * appear in the final graph - their children are connected directly to the last matching
   * ancestor.
   *
   * <p>Uses an explicit stack instead of recursion to avoid stack overflow with deep trace graphs.
   *
   * @param startNode The node to start traversal from
   * @param initialLastViableNodeId ID of the starting viable node (usually the startNode's ID)
   * @param filter Filter to determine which nodes should appear in the final graph
   * @param edges Output set to collect edges between matching nodes
   * @param nodes Output set to collect matching nodes
   * @param spanIdToSpanNodes Lookup map from span ID to SpanNode
   * @param parentSpanIdToChildSpanNodes Map from parent span ID to list of child SpanNodes
   */
  private void dfsFilter(
      SpanNode startNode,
      String initialLastViableNodeId,
      Filter filter,
      Set<Edge> edges,
      Set<Node> nodes,
      Map<String, SpanNode> spanIdToSpanNodes,
      Map<String, List<SpanNode>> parentSpanIdToChildSpanNodes) {
    // Shared visited set for this DFS traversal
    Set<String> visited = new HashSet<>();

    // Use explicit stack for iterative DFS to avoid stack overflow with deep traces
    Deque<DfsState> stack = new ArrayDeque<>();
    stack.push(new DfsState(startNode, initialLastViableNodeId));

    while (!stack.isEmpty()) {
      DfsState state = stack.pop();
      SpanNode node = state.node();
      String lastViableNodeId = state.lastViableNodeId();

      // Skip if already visited (prevents cycles and redundant work)
      if (visited.contains(node.id())) {
        continue;
      }

      visited.add(node.id());

      // Update the "viable" node - if current node matches filter, it becomes the new viable
      // ancestor
      String currentViableNodeId = filter.matches(node) ? node.id() : lastViableNodeId;

      // Process all children of the current node
      List<SpanNode> children = parentSpanIdToChildSpanNodes.getOrDefault(node.id(), List.of());
      for (SpanNode child : children) {
        if (filter.matches(child) && currentViableNodeId != null) {
          // Child matches filter - create edge from current viable node to this child
          SpanNode parent = spanIdToSpanNodes.get(currentViableNodeId);
          if (parent == null) {
            continue;
          }

          Node source = new Node(parent.nodeMetadata());
          Node target = new Node(child.nodeMetadata());

          nodes.add(source);
          nodes.add(target);
          edges.add(new Edge(source.getId(), target.getId(), child.edgeMetadata()));

          // Continue exploration with child as the new viable node
          stack.push(new DfsState(child, child.id()));
        } else {
          // Child doesn't match filter - traverse through it but keep current viable node
          // This allows edges to skip over non-matching intermediate nodes
          stack.push(new DfsState(child, currentViableNodeId));
        }
      }
    }
  }
}
