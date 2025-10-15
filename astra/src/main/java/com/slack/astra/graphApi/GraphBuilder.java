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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GraphBuilder constructs service dependency graphs from Zipkin span data.
 *
 * <p>This class processes distributed tracing spans to build a graph representation showing
 * relationships between operations. It creates nodes representing service operations and edges
 * representing parent-child relationships between spans.
 *
 * <p>The builder supports configurable node and edge metadata extraction through GraphConfig,
 * allowing customization of which span tags are used to populate each entity's metadata.
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
   * <p>The filter uses OR logic: a span matches if ANY of the filter criteria match. Each filter
   * option is a field name (e.g., "operation", "service") mapped to a list of allowed values for
   * that field.
   *
   * <p>Examples: {"operation": ["http.request"]} - matches spans with tag operation="http.request"
   * {"operation": ["http.request", "grpc.request"]} - matches spans with either operation tag
   * {"operation": ["http.request"], "kube.namespace": ["test-app-prod"]} - matches spans with
   * operation="http.request" OR kube.namespace="test-app-prod" {} or null - empty filter matches
   * all spans (no filtering)
   *
   * @param options Map of field names to lists of allowed values. If null or empty, all spans
   *     match.
   */
  public record Filter(Map<String, List<String>> options) {
    public boolean matches(ZipkinSpanResponse span) {
      // Empty or null filter means match all spans
      if (options == null || options.isEmpty()) {
        return true;
      }

      for (Map.Entry<String, List<String>> entry : this.options().entrySet()) {
        String fieldName = entry.getKey();
        List<String> allowedValues = entry.getValue();

        if (allowedValues == null || allowedValues.isEmpty()) {
          continue;
        }

        String actualValue = span.getTags().get(fieldName);
        if (actualValue == null) {
          continue;
        }

        // Return true if ANY filter matches
        if (allowedValues.contains(actualValue)) {
          return true;
        }
      }

      return false;
    }
  }

  /**
   * Builds an (optionally filtered) dependency graph from a list of Zipkin spans.
   *
   * <p>This method processes spans to create nodes and edges representing operation dependencies,
   * filtered by any specified criteria. If a filter is specified, it starts DFS from every span
   * matching the filter, creating edges between matching spans while traversing through
   * non-matching intermediate spans.
   *
   * @param spans List of Zipkin spans to process
   * @param filter Optional filter to apply when building the graph. If empty or null, returns every
   *     connection.
   * @return Graph containing nodes and edges representing operation dependencies
   */
  public Graph buildFromSpans(List<ZipkinSpanResponse> spans, Optional<Filter> filter) {
    Map<String, ZipkinSpanResponse> spanIdToSpans = new HashMap<>();
    Map<String, List<ZipkinSpanResponse>> parentSpanIdToChildSpans = new HashMap<>();

    for (ZipkinSpanResponse span : spans) {
      if (span.getId() == null) {
        continue;
      }
      spanIdToSpans.put(span.getId(), span);

      String parentId = span.getParentId();
      if (parentId != null) {
        parentSpanIdToChildSpans.computeIfAbsent(parentId, k -> new ArrayList<>()).add(span);
      }
    }

    Set<Edge> edges = new HashSet<>();
    Set<Node> nodes = new HashSet<>();

    if (filter.isPresent()) {
      // Start DFS from every node matching the filter since there is no guarantee of a single root
      // in a trace.
      spanIdToSpans.values().stream()
          .filter(span -> filter.get().matches(span))
          .forEach(
              span ->
                  dfsFilter(
                      span, filter.get(), edges, nodes, spanIdToSpans, parentSpanIdToChildSpans));
    } else {
      // No filter, build graph with all edges and collect nodes along the way
      spanIdToSpans.values().stream()
          .forEach(
              span -> {
                List<ZipkinSpanResponse> children =
                    parentSpanIdToChildSpans.getOrDefault(span.getId(), List.of());
                children.stream()
                    .forEach(childSpan -> this.createDependency(nodes, edges, span, childSpan));
              });
    }

    return new Graph(new ArrayList<>(nodes), new ArrayList<>(edges));
  }

  /**
   * State object for iterative DFS traversal.
   *
   * @param span The current span being processed
   * @param lastAncestorSpanId ID of the most recent ancestor that matched the filter
   */
  private record DFSState(ZipkinSpanResponse span, String lastAncestorSpanId) {}

  /**
   * Performs iterative DFS to find edges between spans matching the filter.
   *
   * <p>This method explores the span tree starting from a given span, creating edges only between
   * spans that match the filter criteria. Non-matching intermediate spans are traversed but don't
   * appear in the final graph - their children are connected directly to the last matching
   * ancestor.
   *
   * <p>Uses an explicit stack instead of recursion to avoid issues with deep trace graphs.
   *
   * @param startSpan The span to start traversal from
   * @param filter Filter to determine which nodes should appear in the final graph
   * @param edges Output set to collect edges between matching spans
   * @param nodes Output set to collect matching nodes
   * @param spanIdToSpans Lookup map from span ID to spans
   * @param parentSpanIdToChildSpans Map from parent span ID to list of child spans
   */
  private void dfsFilter(
      ZipkinSpanResponse startSpan,
      Filter filter,
      Set<Edge> edges,
      Set<Node> nodes,
      Map<String, ZipkinSpanResponse> spanIdToSpans,
      Map<String, List<ZipkinSpanResponse>> parentSpanIdToChildSpans) {
    Set<String> visited = new HashSet<>();
    Deque<DFSState> stack = new ArrayDeque<>();
    stack.push(new DFSState(startSpan, startSpan.getId()));

    while (!stack.isEmpty()) {
      DFSState state = stack.pop();
      ZipkinSpanResponse node = state.span();

      // Skip if already visited
      if (visited.contains(node.getId())) {
        continue;
      }
      visited.add(node.getId());

      // If the current node matches filter, it becomes the new ancestor, otherwise keep the last
      // one
      String currentAncestorNodeId =
          filter.matches(node) ? node.getId() : state.lastAncestorSpanId();

      // Process all children of the current span
      List<ZipkinSpanResponse> children =
          parentSpanIdToChildSpans.getOrDefault(node.getId(), List.of());
      for (ZipkinSpanResponse child : children) {
        if (filter.matches(child) && currentAncestorNodeId != null) {
          // Child matches filter, create edge from current ancestor to this child
          ZipkinSpanResponse parent = spanIdToSpans.get(currentAncestorNodeId);
          if (parent == null) {
            continue;
          }
          this.createDependency(nodes, edges, parent, child);

          // Continue exploration with child as the new ancestor
          stack.push(new DFSState(child, child.getId()));
        } else {
          // Child doesn't match filter, traverse through it but keep current ancestor, skipping
          // over non-matching intermediate spans.
          stack.push(new DFSState(child, currentAncestorNodeId));
        }
      }
    }
  }

  private void createDependency(
      Set<Node> nodes, Set<Edge> edges, ZipkinSpanResponse parent, ZipkinSpanResponse child) {
    Node source = new Node(config.createMetadataFromSpan(parent, GraphConfig.EntityType.NODE));
    Node target = new Node(config.createMetadataFromSpan(child, GraphConfig.EntityType.NODE));

    nodes.add(source);
    nodes.add(target);

    edges.add(
        new Edge(
            source.getId(),
            target.getId(),
            config.createMetadataFromSpan(child, GraphConfig.EntityType.EDGE)));
  }
}
