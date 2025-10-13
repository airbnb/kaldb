package com.slack.astra.graphApi;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.zipkinApi.ZipkinEndpointResponse;
import com.slack.astra.zipkinApi.ZipkinSpanResponse;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    Graph graph = defaultGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes()).isEmpty();
    assertThat(graph.edges()).isEmpty();
  }

  @Test
  void buildFromSpans_singleSpanWithoutParent_createsSingleNodeWithoutEdges() {
    List<ZipkinSpanResponse> spans =
        List.of(
            TestUtils.createSpanWithTags(
                "span1",
                "trace1",
                null,
                Map.of(
                    "kube.app",
                    "app1",
                    "kube.namespace",
                    "ns1",
                    "operation_name",
                    "op1",
                    "resource",
                    "res1")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes()).hasSize(1);
    Node node = graph.nodes().getFirst();

    // Verify the node ID matches the expected hash
    SortedMap<String, String> expectedNodeMetadata =
        new TreeMap<>(
            Map.of(
                "app", "app1",
                "namespace", "ns1",
                "resource", "res1"));
    String expectedId = Node.generateIdFromMetadata(expectedNodeMetadata);

    assertThat(node.getId()).isEqualTo(expectedId);
    assertThat(node.getMetadata()).isEqualTo(expectedNodeMetadata);

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
                    "kube.operation", "op1",
                    "resource", "res1")),
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "kube.operation", "op2",
                    "resource", "res2")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

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
                "resource", "res2"));

    SortedMap<String, String> edgeMetadata = new TreeMap<>(Map.of("operation", "op2"));
    String expectedChildId = Node.generateIdFromMetadata(childMetadata);
    Edge edge = graph.edges().iterator().next();
    assertThat(edge.sourceNodeId()).isEqualTo(expectedParentId);
    assertThat(edge.targetNodeId()).isEqualTo(expectedChildId);
    assertThat(edge.metadata()).isEqualTo(edgeMetadata);
  }

  @Test
  void buildFromSpans_httpRequestSpan_usesCanonicalPathAsResource() {
    List<ZipkinSpanResponse> spans =
        List.of(
            TestUtils.createSpanWithTags(
                "span1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "kube.operation", "http.request",
                    "resource", "original_resource",
                    "tag.operation.canonical_path", "/api/users")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes()).hasSize(1);
    Node node = graph.nodes().getFirst();
    assertThat(node.getMetadata().get("resource")).isEqualTo("/api/users");
  }

  @Test
  void buildFromSpans_httpRequestSpanWithoutCanonicalPath_usesOriginalResource() {
    List<ZipkinSpanResponse> spans =
        List.of(
            TestUtils.createSpanWithTags(
                "span1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "kube.operation", "http.request",
                    "resource", "original_resource")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes()).hasSize(1);
    Node node = graph.nodes().getFirst();
    assertThat(node.getMetadata().get("resource")).isEqualTo("original_resource");
  }

  @Test
  void buildFromSpans_missingTags_usesDefaultValues() {
    List<ZipkinSpanResponse> spans =
        List.of(TestUtils.createSpanWithTags("span1", "trace1", null, Map.of()));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes()).hasSize(1);
    Node node = graph.nodes().getFirst();

    SortedMap<String, String> expectedMetadata =
        new TreeMap<>(
            Map.of(
                "app", "unknown_app",
                "namespace", "unknown_namespace",
                "resource", "unknown_resource"));
    String expectedId = Node.generateIdFromMetadata(expectedMetadata);

    assertThat(node.getMetadata()).isEqualTo(expectedMetadata);
    assertThat(node.getId()).isEqualTo(expectedId);
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
                    "kube.operation", "op1",
                    "resource", "res1")),
            // invalid span
            TestUtils.createSpanWithTags(
                null,
                "trace1",
                null,
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "kube.operation", "op2",
                    "resource", "res2")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes()).hasSize(1);
    Node node = graph.nodes().getFirst();
    assertThat(node.getMetadata().get("app")).isEqualTo("app1");
  }

  @Test
  void buildFromSpans_childSpanWithNonExistentParent_createsChildNodeWithoutEdge() {
    List<ZipkinSpanResponse> spans =
        List.of(
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "nonexistent_parent",
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "kube.operation", "op1",
                    "resource", "res1")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes()).hasSize(1);
    assertThat(graph.edges()).isEmpty();
  }

  @Test
  void buildFromSpans_duplicateNodes_deduplicatesNodes() {
    List<ZipkinSpanResponse> spans =
        List.of(
            // two spans that would create the same node
            TestUtils.createSpanWithTags(
                "span1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "kube.operation", "op1",
                    "resource", "res1")),
            TestUtils.createSpanWithTags(
                "span2",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "kube.operation", "op1",
                    "resource", "res1")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    // should only have one unique node
    assertThat(graph.nodes()).hasSize(1);
    assertThat(graph.edges()).isEmpty();
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
                    "kube.operation", "op1",
                    "resource", "res1")),
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "kube.operation", "op2",
                    "resource", "res2")),
            TestUtils.createSpanWithTags(
                "child2",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app3",
                    "kube.namespace", "ns3",
                    "kube.operation", "op3",
                    "resource", "res3")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

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
                    "kube.operation", "op1",
                    "resource", "res1")),
            // two different child spans that reference the same parent
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "kube.operation", "op2",
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
                    "kube.operation", "op2",
                    "resource", "res2")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

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
                    "kube.operation", "root_op",
                    "resource", "root_res")),
            // first level children
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "root",
                Map.of(
                    "kube.app", "child1_app",
                    "kube.namespace", "child1_ns",
                    "kube.operation", "child1_op",
                    "resource", "child1_res")),
            TestUtils.createSpanWithTags(
                "child2",
                "trace1",
                "root",
                Map.of(
                    "kube.app", "child2_app",
                    "kube.namespace", "child2_ns",
                    "kube.operation", "child2_op",
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
                    "kube.operation",
                    "gc_op",
                    "resource",
                    "gc_res")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

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
  void buildFromSpans_defaultConfig_usesRemoteEndpointServiceName() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();
    ZipkinSpanResponse span = new ZipkinSpanResponse("span1", "trace1");

    ZipkinEndpointResponse remoteEndpoint = new ZipkinEndpointResponse();
    remoteEndpoint.setServiceName("test-service");
    span.setRemoteEndpoint(remoteEndpoint);
    span.setTags(Map.of());

    spans.add(span);

    Graph graph = defaultGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes()).hasSize(1);
    Node node = graph.nodes().getFirst();
    assertThat(node.getMetadata().get("service")).isEqualTo("test-service");
  }
}
