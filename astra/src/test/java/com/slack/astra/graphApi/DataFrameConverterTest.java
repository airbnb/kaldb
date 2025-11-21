package com.slack.astra.graphApi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DataFrameConverterTest {
  private DataFrameConverter dataFrameConverter;
  private GraphConfig graphConfig;

  @BeforeEach
  public void setup() throws IOException {
    Path configPath =
        new File(
                Objects.requireNonNull(
                        getClass()
                            .getClassLoader()
                            .getResource("test-dependency-graph-config.yaml"))
                    .getFile())
            .toPath();
    graphConfig = GraphConfig.load(configPath);
    dataFrameConverter = new DataFrameConverter(graphConfig);
  }

  @Test
  public void testEmptyGraph() {
    Graph emptyGraph = new Graph(List.of(), List.of());

    DataFrameConverter.DataFrameResponse response = dataFrameConverter.graphToDataFrame(emptyGraph);

    assertNotNull(response);
    assertNotNull(response.nodes());
    assertNotNull(response.edges());

    assertEquals(0, response.nodes().size());
    assertEquals(0, response.edges().size());
  }

  @Test
  public void testNodesConversion() {
    TreeMap<String, String> metadata1 =
        new TreeMap<>(Map.of("service", "service-1", "resource", "resource-1"));
    TreeMap<String, String> metadata2 =
        new TreeMap<>(Map.of("service", "service-2", "resource", "resource-2"));

    Node node1 = new Node(metadata1);
    Node node2 = new Node(metadata2);
    Graph graph = new Graph(List.of(node1, node2), List.of());

    DataFrameConverter.DataFrameResponse response = dataFrameConverter.graphToDataFrame(graph);

    List<Map<String, Object>> nodes = response.nodes();
    assertEquals(2, nodes.size());

    Map<String, Object> row1 = nodes.get(0);
    assertNotNull(row1.get("id"));
    assertEquals("service-1", row1.get("title"));
    assertEquals("resource-1", row1.get("subtitle"));

    Map<String, Object> row2 = nodes.get(1);
    assertNotNull(row2.get("id"));
    assertEquals("service-2", row2.get("title"));
    assertEquals("resource-2", row2.get("subtitle"));
  }

  @Test
  public void testEdgesConversion() {
    TreeMap<String, String> nodeMetadata1 =
        new TreeMap<>(Map.of("service", "service-1", "resource", "resource-1"));
    TreeMap<String, String> nodeMetadata2 =
        new TreeMap<>(Map.of("service", "service-2", "resource", "resource-2"));
    TreeMap<String, String> nodeMetadata3 =
        new TreeMap<>(Map.of("service", "service-3", "resource", "resource-3"));

    Node node1 = new Node(nodeMetadata1);
    Node node2 = new Node(nodeMetadata2);
    Node node3 = new Node(nodeMetadata3);

    TreeMap<String, String> edgeMetadata1 = new TreeMap<>(Map.of("operation", "http.request"));
    TreeMap<String, String> edgeMetadata2 = new TreeMap<>(Map.of("operation", "grpc.request"));

    Edge edge1 = new Edge(node1.getId(), node2.getId(), edgeMetadata1);
    Edge edge2 = new Edge(node2.getId(), node3.getId(), edgeMetadata2);

    Graph graph = new Graph(List.of(node1, node2, node3), List.of(edge1, edge2));

    DataFrameConverter.DataFrameResponse response = dataFrameConverter.graphToDataFrame(graph);

    List<Map<String, Object>> edges = response.edges();
    assertEquals(2, edges.size());

    Map<String, Object> edge1Row = edges.get(0);
    assertEquals("0", edge1Row.get("id"));
    assertEquals(node1.getId(), edge1Row.get("source"));
    assertEquals(node2.getId(), edge1Row.get("target"));
    assertEquals("http.request", edge1Row.get("mainstat"));

    Map<String, Object> edge2Row = edges.get(1);
    assertEquals("1", edge2Row.get("id"));
    assertEquals(node2.getId(), edge2Row.get("source"));
    assertEquals(node3.getId(), edge2Row.get("target"));
    assertEquals("grpc.request", edge2Row.get("mainstat"));
  }

  @Test
  public void testMissingMetadataField() {
    // Node 1 has both service and resource
    TreeMap<String, String> metadata1 =
        new TreeMap<>(Map.of("service", "service-1", "resource", "resource-1"));
    // Node 2 only has service (resource is missing)
    TreeMap<String, String> metadata2 = new TreeMap<>(Map.of("service", "service-2"));

    Node node1 = new Node(metadata1);
    Node node2 = new Node(metadata2);
    Graph graph = new Graph(List.of(node1, node2), List.of());

    DataFrameConverter.DataFrameResponse response = dataFrameConverter.graphToDataFrame(graph);

    List<Map<String, Object>> nodes = response.nodes();
    assertEquals(2, nodes.size());

    Map<String, Object> row1 = nodes.get(0);
    assertEquals("resource-1", row1.get("subtitle"));

    Map<String, Object> row2 = nodes.get(1);
    assertEquals("", row2.get("subtitle"));
  }

  @Test
  public void testComplexGraphConversion() {
    TreeMap<String, String> nodeMetadata1 =
        new TreeMap<>(Map.of("service", "api-gateway", "resource", "/api/users"));
    TreeMap<String, String> nodeMetadata2 =
        new TreeMap<>(Map.of("service", "auth-service", "resource", "/auth/validate"));
    TreeMap<String, String> nodeMetadata3 =
        new TreeMap<>(Map.of("service", "user-service", "resource", "/users/profile"));

    Node node1 = new Node(nodeMetadata1);
    Node node2 = new Node(nodeMetadata2);
    Node node3 = new Node(nodeMetadata3);

    TreeMap<String, String> edgeMetadata1 = new TreeMap<>(Map.of("operation", "http.request"));
    TreeMap<String, String> edgeMetadata2 = new TreeMap<>(Map.of("operation", "grpc.request"));

    Edge edge1 = new Edge(node1.getId(), node2.getId(), edgeMetadata1);
    Edge edge2 = new Edge(node1.getId(), node3.getId(), edgeMetadata2);

    Graph graph = new Graph(List.of(node1, node2, node3), List.of(edge1, edge2));

    DataFrameConverter.DataFrameResponse response = dataFrameConverter.graphToDataFrame(graph);

    List<Map<String, Object>> nodes = response.nodes();
    List<Map<String, Object>> edges = response.edges();

    assertEquals(3, nodes.size());
    assertEquals(2, edges.size());

    for (Map<String, Object> node : nodes) {
      assertNotNull(node.get("id"));
      assertTrue(node.containsKey("title"));
      assertTrue(node.containsKey("subtitle"));
    }

    for (Map<String, Object> edge : edges) {
      assertNotNull(edge.get("id"));
      assertNotNull(edge.get("source"));
      assertNotNull(edge.get("target"));
      assertTrue(edge.containsKey("mainstat"));
    }
  }
}
