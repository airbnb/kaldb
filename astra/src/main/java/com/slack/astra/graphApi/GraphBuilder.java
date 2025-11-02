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
import java.util.function.Function;
import java.util.stream.Collectors;
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
    Set<Node> nodes = new HashSet<>();
    buildFilteredGraph(filter, edges, nodes, parentSpanIdToChildSpans, spanIdToSpans);

    return new Graph(new ArrayList<>(nodes), new ArrayList<>(edges));
  }

  /**
   * Builds a filtered graph by creating edges between filtered spans.
   *
   * <p>This method processes all filtered spans and for each one, finds its transitive matching
   * children (filtered spans reachable through any number of non-filtered intermediate spans) and
   * creates edges between them. This approach minimizes duplicate traversal work compared to
   * starting a DFS from each filtered span independently.
   *
   * @param filter Optional filter to apply when building the graph. If empty or null, returns every
   *     connection.
   * @param edges Output set to collect distinct edges between matching spans
   * @param nodes Output set to collect distinct nodes
   * @param parentSpanIdToChildSpans Map from parent span ID to list of child spans
   * @param spanIdToSpans Map from span ID to its full span
   */
  private void buildFilteredGraph(
      Optional<Filter> filter,
      Set<Edge> edges,
      Set<Node> nodes,
      Map<String, List<ZipkinSpanResponse>> parentSpanIdToChildSpans,
      Map<String, ZipkinSpanResponse> spanIdToSpans) {
    Map<String, ZipkinSpanResponse> spansToProcess = new HashMap<>();
    if (filter.isPresent()) {
      // Collect all spans matching the filter
      spansToProcess =
          spanIdToSpans.values().stream()
              .filter(filter.get()::matches)
              .collect(Collectors.toMap(ZipkinSpanResponse::getId, Function.identity()));
    } else {
      // No filter, build graph with all edges.
      // After this point, spansToProcess is read only, which is why a copy of spanIdToSpans is
      // unnecessary
      spansToProcess = spanIdToSpans;
    }

    // Build mapping of logical nodes to all its representing span IDs
    Map<String, List<String>> spansByNodeId =
        spanIdToSpans.values().stream()
            .collect(
                Collectors.groupingBy(
                    this::getNodeId,
                    Collectors.mapping(ZipkinSpanResponse::getId, Collectors.toList())));

    for (ZipkinSpanResponse parentSpan : spansToProcess.values()) {
      // Find all descendant spans (may skip through non-matching intermediate spans if a filter
      // exists)
      List<String> childSpanIds =
          collectTransitiveChildren(
              parentSpan,
              spansToProcess.keySet(),
              parentSpanIdToChildSpans,
              spansByNodeId,
              spanIdToSpans);

      // Create an edge from parent to each transitive child
      for (String childSpanId : childSpanIds) {
        createDependency(nodes, edges, parentSpan, spansToProcess.get(childSpanId));
      }
    }
  }

  /**
   * Collects all filtered spans that are transitive children of a given starting span.
   *
   * <p>A transitive child is defined as a filtered span that can be reached from the start span by
   * traversing through any number of non-filtered intermediate spans.
   *
   * <p>This method treats all spans representing the same logical node as equivalent. When one span
   * for a node is processed, all of its sibling spans (those sharing the same node ID) and their
   * child relationships are processed together. This ensures complete coverage of that node’s
   * downstream relationships without redundant traversal.
   *
   * <p>The traversal is iterative (using a stack) and guards against cycles via visited sets.
   *
   * @param startSpan The span to start traversal from
   * @param spanIdsToProcess Set of span IDs that match an optional filter
   * @param parentSpanIdToChildSpans Map from parent span ID to list of child spans
   * @param spansByNodeId Map of a logical node to all its representative spans
   * @param spanIdToSpans Map from span ID to its full span
   * @return List of span IDs for all transitive matching children
   */
  private List<String> collectTransitiveChildren(
      ZipkinSpanResponse startSpan,
      Set<String> spanIdsToProcess,
      Map<String, List<ZipkinSpanResponse>> parentSpanIdToChildSpans,
      Map<String, List<String>> spansByNodeId,
      Map<String, ZipkinSpanResponse> spanIdToSpans) {
    List<String> results = new ArrayList<>();
    Deque<String> work = new ArrayDeque<>();
    Set<String> visitedSpans = new HashSet<>(); // Tracks individual spans to prevent cycles
    Set<String> visitedNodes = new HashSet<>(); // Tracks logical nodes to avoid redundant traversal

    work.push(startSpan.getId());
    visitedSpans.add(startSpan.getId());

    while (!work.isEmpty()) {
      String spanId = work.pop();
      ZipkinSpanResponse span = spanIdToSpans.get(spanId);

      if (span == null) continue;
      String nodeId = getNodeId(span);

      if (!visitedNodes.add(nodeId)) continue;

      for (String siblingSpanId : spansByNodeId.getOrDefault(nodeId, List.of(spanId))) {
        for (ZipkinSpanResponse child :
            parentSpanIdToChildSpans.getOrDefault(siblingSpanId, List.of())) {

          if (!visitedSpans.add(child.getId())) continue;

          if (spanIdsToProcess.contains(child.getId())) {
            // Found a filtered child - add to results and stop traversing this branch
            // because the child will be handled in its own iteration
            results.add(child.getId());
          } else {
            // Non-filtered intermediate span - continue traversing through it
            work.push(child.getId());
          }
        }
      }
    }

    return results;
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

  private String getNodeId(ZipkinSpanResponse span) {
    return Node.generateIdFromMetadata(
        config.createMetadataFromSpan(span, GraphConfig.EntityType.NODE));
  }
}
