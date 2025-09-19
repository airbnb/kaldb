package com.slack.astra.graphApi;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.zipkinApi.ZipkinEndpointResponse;
import com.slack.astra.zipkinApi.ZipkinSpanResponse;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
                getClass()
                    .getClassLoader()
                    .getResource("test-dependency-graph-config.yaml")
                    .getFile())
            .toPath();
    GraphConfig customConfig = GraphConfig.load(configPath);
    configuredGraphBuilder = new GraphBuilder(customConfig);
  }

  @Test
  void buildFromSpans_emptyList_returnsEmptyGraph() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();
    Graph graph = defaultGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).isEmpty();
    assertThat(graph.edges).isEmpty();
  }

  @Test
  void buildFromSpans_singleSpanWithoutParent_createsSingleNodeWithoutEdges() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();
    ZipkinSpanResponse span =
        createSpanWithTags(
            "span1",
            "trace1",
            null,
            Map.of(
                "kube.app",
                "app1",
                "kube.namespace",
                "ns1",
                "kube.operation",
                "op1",
                "resource",
                "res1"));
    spans.add(span);

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(1);
    Node node = graph.nodes.get(0);

    // Verify the node ID matches the expected hash
    SortedMap<String, String> expectedMetadata =
        new TreeMap<>(
            Map.of(
                "app", "app1",
                "namespace", "ns1",
                "operation", "op1",
                "resource", "res1"));
    String expectedId = Node.generateIdFromMetadata(expectedMetadata);

    assertThat(node.getId()).isEqualTo(expectedId);
    assertThat(node.getMetadata().get("app")).isEqualTo("app1");
    assertThat(node.getMetadata().get("namespace")).isEqualTo("ns1");
    assertThat(node.getMetadata().get("operation")).isEqualTo("op1");
    assertThat(node.getMetadata().get("resource")).isEqualTo("res1");

    assertThat(graph.edges).isEmpty();
  }

  @Test
  void buildFromSpans_parentChildSpans_createsNodesWithEdge() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();
    ZipkinSpanResponse parentSpan =
        createSpanWithTags(
            "parent1",
            "trace1",
            null,
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "op1",
                "resource", "res1"));

    ZipkinSpanResponse childSpan =
        createSpanWithTags(
            "child1",
            "trace1",
            "parent1",
            Map.of(
                "kube.app", "app2",
                "kube.namespace", "ns2",
                "kube.operation", "op2",
                "resource", "res2"));

    spans.add(parentSpan);
    spans.add(childSpan);

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(2);
    assertThat(graph.edges).hasSize(1);

    // Generate expected node IDs using the static method
    SortedMap<String, String> parentMetadata = new TreeMap<>();
    parentMetadata.put("app", "app1");
    parentMetadata.put("namespace", "ns1");
    parentMetadata.put("operation", "op1");
    parentMetadata.put("resource", "res1");
    String expectedParentId = Node.generateIdFromMetadata(parentMetadata);

    SortedMap<String, String> childMetadata = new TreeMap<>();
    childMetadata.put("app", "app2");
    childMetadata.put("namespace", "ns2");
    childMetadata.put("operation", "op2");
    childMetadata.put("resource", "res2");
    String expectedChildId = Node.generateIdFromMetadata(childMetadata);

    Edge edge = graph.edges.iterator().next();
    assertThat(edge.parent()).isEqualTo(expectedParentId);
    assertThat(edge.child()).isEqualTo(expectedChildId);
  }

  @Test
  void buildFromSpans_httpRequestSpan_usesCanonicalPathAsResource() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();
    ZipkinSpanResponse span =
        createSpanWithTags(
            "span1",
            "trace1",
            null,
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "http.request",
                "resource", "original_resource",
                "tag.operation.canonical_path", "/api/users"));
    spans.add(span);

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(1);
    Node node = graph.nodes.get(0);

    assertThat(node.getMetadata().get("resource")).isEqualTo("/api/users");
    assertThat(node.getMetadata().get("app")).isEqualTo("app1");
    assertThat(node.getMetadata().get("namespace")).isEqualTo("ns1");
    assertThat(node.getMetadata().get("operation")).isEqualTo("http.request");
  }

  @Test
  void buildFromSpans_httpRequestSpanWithoutCanonicalPath_usesOriginalResource() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();

    ZipkinSpanResponse span =
        createSpanWithTags(
            "span1",
            "trace1",
            null,
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "http.request",
                "resource", "original_resource"));
    spans.add(span);

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(1);
    Node node = graph.nodes.get(0);
    assertThat(node.getMetadata().get("resource")).isEqualTo("original_resource");
  }

  @Test
  void buildFromSpans_missingTags_usesDefaultValues() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();

    ZipkinSpanResponse span = createSpanWithTags("span1", "trace1", null, Map.of());
    spans.add(span);

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(1);
    Node node = graph.nodes.get(0);

    SortedMap<String, String> expectedMetadata = new TreeMap<>();
    expectedMetadata.put("app", "unknown_app");
    expectedMetadata.put("namespace", "unknown_namespace");
    expectedMetadata.put("operation", "unknown_operation");
    expectedMetadata.put("resource", "unknown_resource");
    String expectedId = Node.generateIdFromMetadata(expectedMetadata);

    assertThat(node.getMetadata().get("app")).isEqualTo("unknown_app");
    assertThat(node.getMetadata().get("namespace")).isEqualTo("unknown_namespace");
    assertThat(node.getMetadata().get("operation")).isEqualTo("unknown_operation");
    assertThat(node.getMetadata().get("resource")).isEqualTo("unknown_resource");
    assertThat(node.getId()).isEqualTo(expectedId);
  }

  @Test
  void buildFromSpans_spanWithNullId_skipsSpan() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();

    ZipkinSpanResponse validSpan =
        createSpanWithTags(
            "span1",
            "trace1",
            null,
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "op1",
                "resource", "res1"));

    ZipkinSpanResponse invalidSpan = new ZipkinSpanResponse(null, "trace1");
    invalidSpan.setTags(
        Map.of(
            "kube.app", "app2",
            "kube.namespace", "ns2",
            "kube.operation", "op2",
            "resource", "res2"));

    spans.add(validSpan);
    spans.add(invalidSpan);

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(1);
    Node node = graph.nodes.get(0);
    assertThat(node.getMetadata().get("app")).isEqualTo("app1");
  }

  @Test
  void buildFromSpans_childSpanWithNonExistentParent_createsChildNodeWithoutEdge() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();

    ZipkinSpanResponse childSpan =
        createSpanWithTags(
            "child1",
            "trace1",
            "nonexistent_parent",
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "op1",
                "resource", "res1"));

    spans.add(childSpan);

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(1);
    assertThat(graph.edges).isEmpty();
  }

  @Test
  void buildFromSpans_duplicateNodes_deduplicatesNodes() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();

    // two spans that would create the same node
    ZipkinSpanResponse span1 =
        createSpanWithTags(
            "span1",
            "trace1",
            null,
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "op1",
                "resource", "res1"));

    ZipkinSpanResponse span2 =
        createSpanWithTags(
            "span2",
            "trace1",
            null,
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "op1",
                "resource", "res1"));

    spans.add(span1);
    spans.add(span2);

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    // should only have one unique node
    assertThat(graph.nodes).hasSize(1);
    assertThat(graph.edges).isEmpty();
  }

  @Test
  void buildFromSpans_multipleChildrenSameParent_createsMultipleEdges() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();

    ZipkinSpanResponse parentSpan =
        createSpanWithTags(
            "parent1",
            "trace1",
            null,
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "op1",
                "resource", "res1"));

    ZipkinSpanResponse child1Span =
        createSpanWithTags(
            "child1",
            "trace1",
            "parent1",
            Map.of(
                "kube.app", "app2",
                "kube.namespace", "ns2",
                "kube.operation", "op2",
                "resource", "res2"));

    ZipkinSpanResponse child2Span =
        createSpanWithTags(
            "child2",
            "trace1",
            "parent1",
            Map.of(
                "kube.app", "app3",
                "kube.namespace", "ns3",
                "kube.operation", "op3",
                "resource", "res3"));

    spans.add(parentSpan);
    spans.add(child1Span);
    spans.add(child2Span);

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(3);
    assertThat(graph.edges).hasSize(2);

    SortedMap<String, String> parentMetadata = new TreeMap<>();
    parentMetadata.put("app", "app1");
    parentMetadata.put("namespace", "ns1");
    parentMetadata.put("operation", "op1");
    parentMetadata.put("resource", "res1");
    String expectedParentId = Node.generateIdFromMetadata(parentMetadata);

    SortedMap<String, String> child1Metadata = new TreeMap<>();
    child1Metadata.put("app", "app2");
    child1Metadata.put("namespace", "ns2");
    child1Metadata.put("operation", "op2");
    child1Metadata.put("resource", "res2");
    String expectedChild1Id = Node.generateIdFromMetadata(child1Metadata);

    SortedMap<String, String> child2Metadata = new TreeMap<>();
    child2Metadata.put("app", "app3");
    child2Metadata.put("namespace", "ns3");
    child2Metadata.put("operation", "op3");
    child2Metadata.put("resource", "res3");
    String expectedChild2Id = Node.generateIdFromMetadata(child2Metadata);

    // verify both edges have the same parent
    Set<Edge> edges = graph.edges;
    assertThat(edges.stream().allMatch(edge -> edge.parent().equals(expectedParentId))).isTrue();

    // verify different children
    Set<String> childIds = Set.of(edges.stream().map(Edge::child).toArray(String[]::new));
    assertThat(childIds).containsExactlyInAnyOrder(expectedChild1Id, expectedChild2Id);
  }

  @Test
  void buildFromSpans_duplicateEdges_deduplicatesEdges() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();

    ZipkinSpanResponse parentSpan =
        createSpanWithTags(
            "parent1",
            "trace1",
            null,
            Map.of(
                "kube.app", "app1",
                "kube.namespace", "ns1",
                "kube.operation", "op1",
                "resource", "res1"));

    // two different child spans that reference the same parent
    ZipkinSpanResponse child1Span =
        createSpanWithTags(
            "child1",
            "trace1",
            "parent1",
            Map.of(
                "kube.app", "app2",
                "kube.namespace", "ns2",
                "kube.operation", "op2",
                "resource", "res2"));

    // second span with same child node ID but different span ID - should create deduplicate edge
    ZipkinSpanResponse child2Span =
        createSpanWithTags(
            "child2",
            "trace1",
            "parent1",
            Map.of(
                "kube.app", "app2",
                "kube.namespace", "ns2",
                "kube.operation", "op2",
                "resource", "res2"));

    spans.add(parentSpan);
    spans.add(child1Span);
    spans.add(child2Span);

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    // should have 2 nodes (parent and child - child nodes are deduplicated)
    assertThat(graph.nodes).hasSize(2);

    // should have only 1 edge despite multiple spans creating the same parent-child relationship
    assertThat(graph.edges).hasSize(1);

    SortedMap<String, String> parentMetadata = new TreeMap<>();
    parentMetadata.put("app", "app1");
    parentMetadata.put("namespace", "ns1");
    parentMetadata.put("operation", "op1");
    parentMetadata.put("resource", "res1");
    String expectedParentId = Node.generateIdFromMetadata(parentMetadata);

    SortedMap<String, String> childMetadata = new TreeMap<>();
    childMetadata.put("app", "app2");
    childMetadata.put("namespace", "ns2");
    childMetadata.put("operation", "op2");
    childMetadata.put("resource", "res2");
    String expectedChildId = Node.generateIdFromMetadata(childMetadata);

    Edge edge = graph.edges.iterator().next();
    assertThat(edge.parent()).isEqualTo(expectedParentId);
    assertThat(edge.child()).isEqualTo(expectedChildId);
  }

  @Test
  void buildFromSpans_complexHierarchy_buildsCorrectGraph() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();

    // root span
    ZipkinSpanResponse rootSpan =
        createSpanWithTags(
            "root",
            "trace1",
            null,
            Map.of(
                "kube.app", "root_app",
                "kube.namespace", "root_ns",
                "kube.operation", "root_op",
                "resource", "root_res"));

    // first level children
    ZipkinSpanResponse child1Span =
        createSpanWithTags(
            "child1",
            "trace1",
            "root",
            Map.of(
                "kube.app", "child1_app",
                "kube.namespace", "child1_ns",
                "kube.operation", "child1_op",
                "resource", "child1_res"));

    ZipkinSpanResponse child2Span =
        createSpanWithTags(
            "child2",
            "trace1",
            "root",
            Map.of(
                "kube.app", "child2_app",
                "kube.namespace", "child2_ns",
                "kube.operation", "child2_op",
                "resource", "child2_res"));

    // second level child
    ZipkinSpanResponse grandchildSpan =
        createSpanWithTags(
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
                "gc_res"));

    spans.add(rootSpan);
    spans.add(child1Span);
    spans.add(child2Span);
    spans.add(grandchildSpan);

    Graph graph = configuredGraphBuilder.buildFromSpans(spans);

    assertThat(graph.nodes).hasSize(4);
    assertThat(graph.edges).hasSize(3);

    SortedMap<String, String> rootMetadata = new TreeMap<>();
    rootMetadata.put("app", "root_app");
    rootMetadata.put("namespace", "root_ns");
    rootMetadata.put("operation", "root_op");
    rootMetadata.put("resource", "root_res");
    String expectedRootId = Node.generateIdFromMetadata(rootMetadata);

    SortedMap<String, String> child1Metadata = new TreeMap<>();
    child1Metadata.put("app", "child1_app");
    child1Metadata.put("namespace", "child1_ns");
    child1Metadata.put("operation", "child1_op");
    child1Metadata.put("resource", "child1_res");
    String expectedChild1Id = Node.generateIdFromMetadata(child1Metadata);

    SortedMap<String, String> child2Metadata = new TreeMap<>();
    child2Metadata.put("app", "child2_app");
    child2Metadata.put("namespace", "child2_ns");
    child2Metadata.put("operation", "child2_op");
    child2Metadata.put("resource", "child2_res");
    String expectedChild2Id = Node.generateIdFromMetadata(child2Metadata);

    SortedMap<String, String> grandchildMetadata = new TreeMap<>();
    grandchildMetadata.put("app", "gc_app");
    grandchildMetadata.put("namespace", "gc_ns");
    grandchildMetadata.put("operation", "gc_op");
    grandchildMetadata.put("resource", "gc_res");
    String expectedGrandchildId = Node.generateIdFromMetadata(grandchildMetadata);

    Set<Edge> edges = graph.edges;

    // root -> child1
    assertThat(edges)
        .anyMatch(
            edge -> edge.parent().equals(expectedRootId) && edge.child().equals(expectedChild1Id));

    // root -> child2
    assertThat(edges)
        .anyMatch(
            edge -> edge.parent().equals(expectedRootId) && edge.child().equals(expectedChild2Id));

    // child1 -> grandchild
    assertThat(edges)
        .anyMatch(
            edge ->
                edge.parent().equals(expectedChild1Id)
                    && edge.child().equals(expectedGrandchildId));
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

    assertThat(graph.nodes).hasSize(1);
    Node node = graph.nodes.get(0);
    assertThat(node.getMetadata().get("service")).isEqualTo("test-service");
  }

  private ZipkinSpanResponse createSpanWithTags(
      String id, String traceId, String parentId, Map<String, String> tags) {
    ZipkinSpanResponse span = new ZipkinSpanResponse(id, traceId);
    span.setParentId(parentId);
    span.setTags(new HashMap<>(tags));

    // Set up a default remote endpoint for configured tests
    ZipkinEndpointResponse remoteEndpoint = new ZipkinEndpointResponse();
    remoteEndpoint.setServiceName("default-service");
    span.setRemoteEndpoint(remoteEndpoint);

    return span;
  }
}
