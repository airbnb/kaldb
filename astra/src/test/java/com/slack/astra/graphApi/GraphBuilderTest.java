package com.slack.astra.graphApi;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.zipkinApi.ZipkinSpanResponse;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GraphBuilderTest {
  private GraphBuilder defaultGraphBuilder;
  private GraphBuilder configuredGraphBuilder;

  @BeforeEach
  void setUp() throws IOException {
    defaultGraphBuilder = new GraphBuilder(GraphConfig.DEFAULT);

    // Load custom config from YAML file
    Path configPath =
        new File(
                Objects.requireNonNull(
                        getClass()
                            .getClassLoader()
                            .getResource("test-dependency-graph-config.yaml"))
                    .getFile())
            .toPath();
    GraphConfig customConfig = GraphConfig.load(configPath);
    configuredGraphBuilder = new GraphBuilder(customConfig);
  }

  @Test
  void buildFromSpans_emptyList_returnsEmptyGraph() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();
    Graph graph = defaultGraphBuilder.buildFromSpans(spans, Optional.empty());

    assertThat(graph.nodes()).isEmpty();
    assertThat(graph.edges()).isEmpty();
  }

  @Test
  void buildFromSpans_parentChildSpans_createsNodesWithEdge() {
    List<ZipkinSpanResponse> spans =
        List.of(
            TestUtils.createSpanWithTags(
                "parent1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "operation_name", "op1",
                    "resource", "res1")),
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app",
                    "app2",
                    "kube.namespace",
                    "ns2",
                    "operation_name",
                    "http.request",
                    "resource",
                    "res2",
                    "tag.http.target.canonical_path",
                    "/v2/res2")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.empty());

    assertThat(graph.nodes()).hasSize(2);
    assertThat(graph.edges()).hasSize(1);

    // Generate expected node IDs using the static method
    SortedMap<String, String> parentMetadata =
        new TreeMap<>(
            Map.of(
                "app", "app1",
                "namespace", "ns1",
                "resource", "res1"));
    String expectedParentId = Node.generateIdFromMetadata(parentMetadata);

    SortedMap<String, String> childMetadata =
        new TreeMap<>(
            Map.of(
                "app", "app2",
                "namespace", "ns2",
                "resource", "/v2/res2"));

    // uses canonical path as resource
    SortedMap<String, String> edgeMetadata = new TreeMap<>(Map.of("operation", "http.request"));
    String expectedChildId = Node.generateIdFromMetadata(childMetadata);
    Edge edge = graph.edges().iterator().next();
    assertThat(edge.sourceNodeId()).isEqualTo(expectedParentId);
    assertThat(edge.targetNodeId()).isEqualTo(expectedChildId);
    assertThat(edge.metadata()).isEqualTo(edgeMetadata);
  }

  @Test
  void buildFromSpans_spanWithNullId_skipsSpan() {
    List<ZipkinSpanResponse> spans =
        List.of(
            // valid span
            TestUtils.createSpanWithTags(
                "span1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "operation_name", "op1",
                    "resource", "res1")),
            // invalid span
            TestUtils.createSpanWithTags(
                null,
                "trace1",
                null,
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "operation_name", "op2",
                    "resource", "res2")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.empty());

    assertThat(graph.edges()).hasSize(0);
    // No nodes because there are no edges
    assertThat(graph.nodes()).hasSize(0);
  }

  @Test
  void buildFromSpans_multipleChildrenSameParent_createsMultipleEdges() {
    List<ZipkinSpanResponse> spans =
        List.of(
            TestUtils.createSpanWithTags(
                "parent1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "operation_name", "op1",
                    "resource", "res1")),
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "operation_name", "op2",
                    "resource", "res2")),
            TestUtils.createSpanWithTags(
                "child2",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app3",
                    "kube.namespace", "ns3",
                    "operation_name", "op3",
                    "resource", "res3")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.empty());

    assertThat(graph.nodes()).hasSize(3);
    assertThat(graph.edges()).hasSize(2);

    SortedMap<String, String> parentMetadata =
        new TreeMap<>(
            Map.of(
                "app", "app1",
                "namespace", "ns1",
                "resource", "res1"));
    String expectedParentId = Node.generateIdFromMetadata(parentMetadata);

    SortedMap<String, String> child1Metadata =
        new TreeMap<>(
            Map.of(
                "app", "app2",
                "namespace", "ns2",
                "resource", "res2"));
    String expectedChild1Id = Node.generateIdFromMetadata(child1Metadata);

    SortedMap<String, String> child2Metadata =
        new TreeMap<>(
            Map.of(
                "app", "app3",
                "namespace", "ns3",
                "resource", "res3"));
    String expectedChild2Id = Node.generateIdFromMetadata(child2Metadata);

    // verify both edges have the same parent
    List<Edge> edges = graph.edges();
    assertThat(edges.stream().allMatch(edge -> edge.sourceNodeId().equals(expectedParentId)))
        .isTrue();

    // metadata of the edges
    assertThat(edges.get(0).metadata()).isEqualTo(new TreeMap<>(Map.of("operation", "op2")));
    assertThat(edges.get(1).metadata()).isEqualTo(new TreeMap<>(Map.of("operation", "op3")));

    // verify different children
    List<String> childIds = List.of(edges.stream().map(Edge::targetNodeId).toArray(String[]::new));
    assertThat(childIds).containsExactlyInAnyOrder(expectedChild1Id, expectedChild2Id);
  }

  @Test
  void buildFromSpans_duplicateEdges_deduplicatesEdges() {
    List<ZipkinSpanResponse> spans =
        List.of(
            TestUtils.createSpanWithTags(
                "parent1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "operation_name", "op1",
                    "resource", "res1")),
            // two different child spans that reference the same parent
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "operation_name", "op2",
                    "resource", "res2")),
            // second span with same child node ID but different span ID - should create deduplicate
            // edge
            TestUtils.createSpanWithTags(
                "child2",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "operation_name", "op2",
                    "resource", "res2")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.empty());

    // should have 2 nodes (parent and child - child nodes are deduplicated)
    assertThat(graph.nodes()).hasSize(2);

    // should have only 1 edge despite multiple spans creating the same parent-child relationship
    assertThat(graph.edges()).hasSize(1);
    assertThat(graph.edges().get(0).metadata())
        .isEqualTo(new TreeMap<>(Map.of("operation", "op2")));

    SortedMap<String, String> parentMetadata =
        new TreeMap<>(
            Map.of(
                "app", "app1",
                "namespace", "ns1",
                "resource", "res1"));
    String expectedParentId = Node.generateIdFromMetadata(parentMetadata);

    SortedMap<String, String> childMetadata =
        new TreeMap<>(
            Map.of(
                "app", "app2",
                "namespace", "ns2",
                "resource", "res2"));
    String expectedChildId = Node.generateIdFromMetadata(childMetadata);

    Edge edge = graph.edges().getFirst();
    assertThat(edge.sourceNodeId()).isEqualTo(expectedParentId);
    assertThat(edge.targetNodeId()).isEqualTo(expectedChildId);
  }

  @Test
  void buildFromSpans_complexHierarchy_buildsCorrectGraph() {
    List<ZipkinSpanResponse> spans =
        List.of(
            // root span
            TestUtils.createSpanWithTags(
                "root",
                "trace1",
                null,
                Map.of(
                    "kube.app", "root_app",
                    "kube.namespace", "root_ns",
                    "operation_name", "root_op",
                    "resource", "root_res")),
            // first level children
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "root",
                Map.of(
                    "kube.app", "child1_app",
                    "kube.namespace", "child1_ns",
                    "operation_name", "child1_op",
                    "resource", "child1_res")),
            TestUtils.createSpanWithTags(
                "child2",
                "trace1",
                "root",
                Map.of(
                    "kube.app", "child2_app",
                    "kube.namespace", "child2_ns",
                    "operation_name", "child2_op",
                    "resource", "child2_res")),
            // second level child
            TestUtils.createSpanWithTags(
                "grandchild",
                "trace1",
                "child1",
                Map.of(
                    "kube.app",
                    "gc_app",
                    "kube.namespace",
                    "gc_ns",
                    "operation_name",
                    "gc_op",
                    "resource",
                    "gc_res")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.empty());

    assertThat(graph.nodes()).hasSize(4);
    assertThat(graph.edges()).hasSize(3);

    SortedMap<String, String> rootMetadata =
        new TreeMap<>(
            Map.of(
                "app", "root_app",
                "namespace", "root_ns",
                "resource", "root_res"));
    String expectedRootId = Node.generateIdFromMetadata(rootMetadata);

    SortedMap<String, String> child1Metadata =
        new TreeMap<>(
            Map.of(
                "app", "child1_app",
                "namespace", "child1_ns",
                "resource", "child1_res"));
    String expectedChild1Id = Node.generateIdFromMetadata(child1Metadata);

    SortedMap<String, String> child2Metadata =
        new TreeMap<>(
            Map.of(
                "app", "child2_app",
                "namespace", "child2_ns",
                "resource", "child2_res"));
    String expectedChild2Id = Node.generateIdFromMetadata(child2Metadata);

    SortedMap<String, String> grandchildMetadata =
        new TreeMap<>(
            Map.of(
                "app", "gc_app",
                "namespace", "gc_ns",
                "resource", "gc_res"));
    String expectedGrandchildId = Node.generateIdFromMetadata(grandchildMetadata);

    List<Edge> edges = graph.edges();

    assertThat(edges)
        .anyMatch(
            edge ->
                edge.sourceNodeId().equals(expectedRootId)
                    && edge.targetNodeId().equals(expectedChild1Id)
                    && edge.metadata().equals(new TreeMap<>(Map.of("operation", "child1_op"))));

    assertThat(edges)
        .anyMatch(
            edge ->
                edge.sourceNodeId().equals(expectedRootId)
                    && edge.targetNodeId().equals(expectedChild2Id)
                    && edge.metadata().equals(new TreeMap<>(Map.of("operation", "child2_op"))));

    assertThat(edges)
        .anyMatch(
            edge ->
                edge.sourceNodeId().equals(expectedChild1Id)
                    && edge.targetNodeId().equals(expectedGrandchildId)
                    && edge.metadata().equals(new TreeMap<>(Map.of("operation", "gc_op"))));
  }

  @Test
  void buildFromSpans_withFilter_includesOnlyMatchingNodesWithEdges() {
    List<ZipkinSpanResponse> spans =
        List.of(
            TestUtils.createSpanWithTags(
                "parent1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "operation_name", "http.request",
                    "resource", "res1")),
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "operation_name", "http.request",
                    "resource", "res2")),
            TestUtils.createSpanWithTags(
                "child2",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app3",
                    "kube.namespace", "ns3",
                    "operation_name", "op3",
                    "resource", "res3")));

    // Filter to only include nodes with operation "http.request"
    GraphBuilder.Filter filter = new GraphBuilder.Filter(Map.of("operation", "http.request"));
    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.of(filter));

    // Should only include parent1 and child1 nodes (both match filter and have edge between them)
    assertThat(graph.nodes()).hasSize(2);
    assertThat(graph.edges()).hasSize(1);

    SortedMap<String, String> parentMetadata =
        new TreeMap<>(
            Map.of(
                "app", "app1",
                "namespace", "ns1",
                "resource", "res1"));
    String expectedParentId = Node.generateIdFromMetadata(parentMetadata);

    SortedMap<String, String> child1Metadata =
        new TreeMap<>(
            Map.of(
                "app", "app2",
                "namespace", "ns2",
                "resource", "res2"));
    String expectedChild1Id = Node.generateIdFromMetadata(child1Metadata);

    Edge edge = graph.edges().get(0);
    assertThat(edge.sourceNodeId()).isEqualTo(expectedParentId);
    assertThat(edge.targetNodeId()).isEqualTo(expectedChild1Id);
    assertThat(edge.metadata()).isEqualTo(new TreeMap<>(Map.of("operation", "http.request")));
  }

  @Test
  void buildFromSpans_withFilter_skipsIntermediateNonMatchingNodes() {
    List<ZipkinSpanResponse> spans =
        List.of(
            // root - operation: http.request
            TestUtils.createSpanWithTags(
                "root",
                "trace1",
                null,
                Map.of(
                    "kube.app", "root_app",
                    "kube.namespace", "root_ns",
                    "operation_name", "http.request",
                    "resource", "root_res")),
            // intermediate child - operation: op2 (doesn't match filter)
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "root",
                Map.of(
                    "kube.app", "child1_app",
                    "kube.namespace", "child1_ns",
                    "operation_name", "op2",
                    "resource", "child1_res")),
            // grandchild - operation: http.request (matches filter)
            TestUtils.createSpanWithTags(
                "grandchild",
                "trace1",
                "child1",
                Map.of(
                    "kube.app", "gc_app",
                    "kube.namespace", "gc_ns",
                    "operation_name", "http.request",
                    "resource", "gc_res")));

    // Filter to only include nodes with operation "http.request"
    GraphBuilder.Filter filter = new GraphBuilder.Filter(Map.of("operation", "http.request"));
    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.of(filter));

    // Should include root and grandchild nodes, skipping intermediate child1
    assertThat(graph.nodes()).hasSize(2);
    assertThat(graph.edges()).hasSize(1);

    SortedMap<String, String> rootMetadata =
        new TreeMap<>(
            Map.of(
                "app", "root_app",
                "namespace", "root_ns",
                "resource", "root_res"));
    String expectedRootId = Node.generateIdFromMetadata(rootMetadata);

    SortedMap<String, String> grandchildMetadata =
        new TreeMap<>(
            Map.of(
                "app", "gc_app",
                "namespace", "gc_ns",
                "resource", "gc_res"));
    String expectedGrandchildId = Node.generateIdFromMetadata(grandchildMetadata);

    // Should have edge directly from root to grandchild (skipping intermediate)
    Edge edge = graph.edges().get(0);
    assertThat(edge.sourceNodeId()).isEqualTo(expectedRootId);
    assertThat(edge.targetNodeId()).isEqualTo(expectedGrandchildId);
    assertThat(edge.metadata()).isEqualTo(new TreeMap<>(Map.of("operation", "http.request")));
  }

  @Test
  void buildFromSpans_withFilter_noMatchingNodes_returnsEmptyGraph() {
    List<ZipkinSpanResponse> spans =
        List.of(
            TestUtils.createSpanWithTags(
                "parent1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "operation_name", "op1",
                    "resource", "res1")),
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "operation_name", "op2",
                    "resource", "res2")));

    // Filter that doesn't match any nodes
    GraphBuilder.Filter filter =
        new GraphBuilder.Filter(Map.of("operation", "nonexistent.operation"));
    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.of(filter));

    assertThat(graph.nodes()).isEmpty();
    assertThat(graph.edges()).isEmpty();
  }

  @Test
  void buildFromSpans_withFilter_multipleMatchingPaths() {
    List<ZipkinSpanResponse> spans =
        List.of(
            // root - http.request
            TestUtils.createSpanWithTags(
                "root",
                "trace1",
                null,
                Map.of(
                    "kube.app", "root_app",
                    "kube.namespace", "root_ns",
                    "operation_name", "http.request",
                    "resource", "root_res")),
            // child1 - http.request
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "root",
                Map.of(
                    "kube.app", "child1_app",
                    "kube.namespace", "child1_ns",
                    "operation_name", "http.request",
                    "resource", "child1_res")),
            // child2 - http.request
            TestUtils.createSpanWithTags(
                "child2",
                "trace1",
                "root",
                Map.of(
                    "kube.app", "child2_app",
                    "kube.namespace", "child2_ns",
                    "operation_name", "http.request",
                    "resource", "child2_res")));

    // Filter to only include nodes with operation "http.request"
    GraphBuilder.Filter filter = new GraphBuilder.Filter(Map.of("operation", "http.request"));
    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.of(filter));

    // Should include all 3 nodes since they all match
    assertThat(graph.nodes()).hasSize(3);
    // Should have 2 edges: root->child1 and root->child2
    assertThat(graph.edges()).hasSize(2);

    SortedMap<String, String> rootMetadata =
        new TreeMap<>(
            Map.of(
                "app", "root_app",
                "namespace", "root_ns",
                "resource", "root_res"));
    String expectedRootId = Node.generateIdFromMetadata(rootMetadata);

    // Both edges should originate from root
    assertThat(graph.edges().stream().allMatch(edge -> edge.sourceNodeId().equals(expectedRootId)))
        .isTrue();
  }

  @Test
  void buildFromSpans_withFilter_multipleDisconnectedSubtreesWithDifferentDepths() {
    List<ZipkinSpanResponse> spans =
        List.of(
            // First subtree: intermediate1 (parent missing) -> child1 -> grandchild1
            TestUtils.createSpanWithTags(
                "intermediate1",
                "trace1",
                "missingParent1", // parent doesn't exist in span list
                Map.of(
                    "kube.app", "int1_app",
                    "kube.namespace", "int1_ns",
                    "operation_name", "op1",
                    "resource", "int1_res")),
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "intermediate1",
                Map.of(
                    "kube.app", "child1_app",
                    "kube.namespace", "child1_ns",
                    "operation_name", "http.request",
                    "resource", "child1_res")),
            TestUtils.createSpanWithTags(
                "grandchild1",
                "trace1",
                "child1",
                Map.of(
                    "kube.app", "gc1_app",
                    "kube.namespace", "gc1_ns",
                    "operation_name", "http.request",
                    "resource", "gc1_res")),
            // Second subtree: non-matching nodes -> matching -> more non-matching -> matching leaf
            // intermediate2a (parent missing) -> intermediate2b -> intermediate2c -> matching1 ->
            // intermediate2d -> intermediate2e -> matching2
            TestUtils.createSpanWithTags(
                "intermediate2a",
                "trace1",
                "missingParent2", // parent doesn't exist in span list
                Map.of(
                    "kube.app", "int2a_app",
                    "kube.namespace", "int2a_ns",
                    "operation_name", "op2a",
                    "resource", "int2a_res")),
            TestUtils.createSpanWithTags(
                "intermediate2b",
                "trace1",
                "intermediate2a",
                Map.of(
                    "kube.app", "int2b_app",
                    "kube.namespace", "int2b_ns",
                    "operation_name", "op2b",
                    "resource", "int2b_res")),
            TestUtils.createSpanWithTags(
                "intermediate2c",
                "trace1",
                "intermediate2b",
                Map.of(
                    "kube.app", "int2c_app",
                    "kube.namespace", "int2c_ns",
                    "operation_name", "op2c",
                    "resource", "int2c_res")),
            TestUtils.createSpanWithTags(
                "matching1",
                "trace1",
                "intermediate2c",
                Map.of(
                    "kube.app", "match1_app",
                    "kube.namespace", "match1_ns",
                    "operation_name", "http.request",
                    "resource", "match1_res")),
            TestUtils.createSpanWithTags(
                "intermediate2d",
                "trace1",
                "matching1",
                Map.of(
                    "kube.app", "int2d_app",
                    "kube.namespace", "int2d_ns",
                    "operation_name", "op2d",
                    "resource", "int2d_res")),
            TestUtils.createSpanWithTags(
                "intermediate2e",
                "trace1",
                "intermediate2d",
                Map.of(
                    "kube.app", "int2e_app",
                    "kube.namespace", "int2e_ns",
                    "operation_name", "op2e",
                    "resource", "int2e_res")),
            TestUtils.createSpanWithTags(
                "matching2",
                "trace1",
                "intermediate2e",
                Map.of(
                    "kube.app", "match2_app",
                    "kube.namespace", "match2_ns",
                    "operation_name", "http.request",
                    "resource", "match2_res")));

    // Filter to only include nodes with operation "http.request"
    GraphBuilder.Filter filter = new GraphBuilder.Filter(Map.of("operation", "http.request"));
    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.of(filter));

    // Should include 4 matching nodes from both disconnected subtrees
    assertThat(graph.nodes()).hasSize(4);
    // Should have 2 edges: child1->grandchild1 and matching2->leaf2 (skipping multiple non-matching
    // nodes)
    assertThat(graph.edges()).hasSize(2);

    SortedMap<String, String> child1Metadata =
        new TreeMap<>(
            Map.of(
                "app", "child1_app",
                "namespace", "child1_ns",
                "resource", "child1_res"));
    String expectedChild1Id = Node.generateIdFromMetadata(child1Metadata);

    SortedMap<String, String> grandchild1Metadata =
        new TreeMap<>(
            Map.of(
                "app", "gc1_app",
                "namespace", "gc1_ns",
                "resource", "gc1_res"));
    String expectedGrandchild1Id = Node.generateIdFromMetadata(grandchild1Metadata);

    SortedMap<String, String> matching1Metadata =
        new TreeMap<>(
            Map.of(
                "app", "match1_app",
                "namespace", "match1_ns",
                "resource", "match1_res"));
    String expectedMatching1Id = Node.generateIdFromMetadata(matching1Metadata);

    SortedMap<String, String> matching2Metadata =
        new TreeMap<>(
            Map.of(
                "app", "match2_app",
                "namespace", "match2_ns",
                "resource", "match2_res"));
    String expectedMatching2Id = Node.generateIdFromMetadata(matching2Metadata);

    // Verify both disconnected edges exist
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.sourceNodeId().equals(expectedChild1Id)
                    && edge.targetNodeId().equals(expectedGrandchild1Id));

    // This edge should skip multiple non-matching intermediate nodes
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.sourceNodeId().equals(expectedMatching1Id)
                    && edge.targetNodeId().equals(expectedMatching2Id));
  }

  @Test
  void buildFromSpans_withMultipleFilterOptions_matchesAnyOption() {
    List<ZipkinSpanResponse> spans =
        List.of(
            // Parent with operation: http.request
            TestUtils.createSpanWithTags(
                "parent1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "operation_name", "http.request",
                    "resource", "res1")),
            // Child with operation: grpc.request (different operation, but same namespace)
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns1",
                    "operation_name", "grpc.request",
                    "resource", "res2")),
            // Grandchild with operation: other (doesn't match either filter option)
            TestUtils.createSpanWithTags(
                "grandchild1",
                "trace1",
                "child1",
                Map.of(
                    "kube.app", "app3",
                    "kube.namespace", "ns3",
                    "operation_name", "other.operation",
                    "resource", "res3")));

    // Filter with two options: matches nodes with either http.request OR grpc.request
    GraphBuilder.Filter filter =
        new GraphBuilder.Filter(Map.of("operation", "http.request", "namespace", "ns1"));
    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.of(filter));

    // Should include parent1 and child1 (both match at least one filter option)
    assertThat(graph.nodes()).hasSize(2);
    assertThat(graph.edges()).hasSize(1);

    SortedMap<String, String> parentMetadata =
        new TreeMap<>(
            Map.of(
                "app", "app1",
                "namespace", "ns1",
                "resource", "res1"));
    String expectedParentId = Node.generateIdFromMetadata(parentMetadata);

    SortedMap<String, String> childMetadata =
        new TreeMap<>(
            Map.of(
                "app", "app2",
                "namespace", "ns1",
                "resource", "res2"));
    String expectedChildId = Node.generateIdFromMetadata(childMetadata);

    Edge edge = graph.edges().get(0);
    assertThat(edge.sourceNodeId()).isEqualTo(expectedParentId);
    assertThat(edge.targetNodeId()).isEqualTo(expectedChildId);
    assertThat(edge.metadata()).isEqualTo(new TreeMap<>(Map.of("operation", "grpc.request")));
  }
}
