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
   * filtered by any specified criteria. If a filter is specified, it collects all filtered spans
   * first, then for each filtered span, finds its transitive matching children (filtered spans
   * reachable through non-filtered intermediate spans) and creates edges between them.
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
    Map<String, Node> nodeIdToNodes = new HashMap<>();

    buildFilteredGraph(filter, edges, nodeIdToNodes, parentSpanIdToChildSpans, spanIdToSpans);
    return new Graph(new ArrayList<>(nodeIdToNodes.values()), new ArrayList<>(edges));
  }

  /**
   * Builds a filtered graph by creating edges between filtered spans.
   *
   * <p>This method processes all filtered spans and for each one, finds its transitive matching
   * children (filtered spans reachable through any number of non-filtered intermediate spans) and
   * creates edges between them. This approach minimizes duplicate traversal work compared to
   * starting a DFS from each filtered span independently.
   *
   * @param filter Map from span ID to spans that match the filter criteria
   * @param edges Output set to collect edges between matching spans
   * @param nodeIdToNodes Output map to collect matching nodes
   * @param parentSpanIdToChildSpans Map from parent span ID to list of child spans
   */
  private void buildFilteredGraph(
      Optional<Filter> filter,
      Set<Edge> edges,
      Map<String, Node> nodeIdToNodes,
      Map<String, List<ZipkinSpanResponse>> parentSpanIdToChildSpans,
      Map<String, ZipkinSpanResponse> spanIdToSpans) {
    Map<String, ZipkinSpanResponse> filteredSpanIdToSpans = new HashMap<>();
    if (filter.isPresent()) {
      // Collect all spans matching the filter
      for (ZipkinSpanResponse span : spanIdToSpans.values()) {
        if (filter.get().matches(span)) filteredSpanIdToSpans.put(span.getId(), span);
      }
    } else {
      // No filter, build graph with all edges
      // filteredSpanIdToSpans is read only after this point which is why a copy is not needed here.
      filteredSpanIdToSpans = spanIdToSpans;
    }

    Map<String, List<String>> nodeIdToSpanIds = new HashMap<>();
    for (ZipkinSpanResponse span : spanIdToSpans.values()) {
      nodeIdToSpanIds.computeIfAbsent(getNodeId(span), k -> new ArrayList<>()).add(span.getId());
    }

    for (ZipkinSpanResponse parentSpan : filteredSpanIdToSpans.values()) {
      List<String> childSpanIds =
          collectTransitiveChildren(
              parentSpan,
              filteredSpanIdToSpans.keySet(),
              parentSpanIdToChildSpans,
              nodeIdToSpanIds,
              spanIdToSpans);
      for (String childSpanId : childSpanIds) {
        addEdge(nodeIdToNodes, edges, parentSpan, filteredSpanIdToSpans.get(childSpanId));
      }
    }
  }

  /**
   * Collects all filtered spans that are transitive children of a given span.
   *
   * <p>A transitive child is a filtered span that can be reached from the start span by traversing
   * through any number of non-filtered intermediate spans. This method uses an iterative approach
   * with a work queue to find all such children.
   *
   * @param startSpan The span to start traversal from
   * @param filteredSpanIds Set of span IDs that match the filter
   * @param parentSpanIdToChildSpans Map from parent span ID to list of child spans
   * @return List of span IDs for all transitive matching children
   */
  private List<String> collectTransitiveChildren(
      ZipkinSpanResponse startSpan,
      Set<String> filteredSpanIds,
      Map<String, List<ZipkinSpanResponse>> parentSpanIdToChildSpans,
      Map<String, List<String>> nodeIdToSpanIds,
      Map<String, ZipkinSpanResponse> spanIdToSpans) {
    List<String> results = new ArrayList<>();
    Deque<String> stack = new ArrayDeque<>();
    Set<String> visitedSpans = new HashSet<>();
    Set<String> visitedNodes = new HashSet<>();

    stack.push(startSpan.getId());
    visitedSpans.add(startSpan.getId());

    while (!stack.isEmpty()) {
      String spanId = stack.pop();
      ZipkinSpanResponse span = spanIdToSpans.get(spanId);
      if (span == null) continue;

      String nodeId = getNodeId(span);
      if (!visitedNodes.add(nodeId)) continue;

      for (String siblingSpanId : nodeIdToSpanIds.getOrDefault(nodeId, List.of(spanId))) {
        for (ZipkinSpanResponse child :
            parentSpanIdToChildSpans.getOrDefault(siblingSpanId, List.of())) {
          if (!visitedSpans.add(child.getId())) continue;

          if (filteredSpanIds.contains(child.getId())) {
            results.add(child.getId());
          } else {
            stack.push(child.getId());
          }
        }
      }
    }

    return results;
  }

  private void addEdge(
      Map<String, Node> nodeIdToNodes,
      Set<Edge> edges,
      ZipkinSpanResponse parent,
      ZipkinSpanResponse child) {
    Node source = new Node(config.createMetadataFromSpan(parent, GraphConfig.EntityType.NODE));
    Node target = new Node(config.createMetadataFromSpan(child, GraphConfig.EntityType.NODE));
    nodeIdToNodes.putIfAbsent(source.getId(), source);
    nodeIdToNodes.putIfAbsent(target.getId(), target);
    edges.add(
        new Edge(
            source.getId(),
            target.getId(),
            config.createMetadataFromSpan(child, GraphConfig.EntityType.EDGE)));
  }

  private String getNodeId(ZipkinSpanResponse span) {
    return Node.generateIdFromMetadata(
        config.createMetadataFromSpan(span, GraphConfig.EntityType.NODE));
  }
}
