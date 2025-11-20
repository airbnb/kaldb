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
    assertNotNull(response.data());
    assertEquals(2, response.data().size());

    DataFrameConverter.DataFrame nodesDataFrame = response.data().get(0);
    DataFrameConverter.DataFrame edgesDataFrame = response.data().get(1);

    assertEquals("nodes", nodesDataFrame.name());
    assertEquals("edges", edgesDataFrame.name());

    // id field
    assertEquals(1, nodesDataFrame.fields().size());
    // id, source, target fields
    assertEquals(3, edgesDataFrame.fields().size());

    assertNotNull(nodesDataFrame.meta());
    assertEquals("nodeGraph", nodesDataFrame.meta().get("preferredVisualisationType"));
    assertNotNull(edgesDataFrame.meta());
    assertEquals("nodeGraph", edgesDataFrame.meta().get("preferredVisualisationType"));
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

    DataFrameConverter.DataFrame nodesDataFrame = response.data().get(0);
    List<Map<String, Object>> nodeFields = nodesDataFrame.fields();

    Map<String, Object> titleField = findField(nodeFields, "title");
    assertNotNull(titleField);
    List<?> titleValues = (List<?>) titleField.get("values");
    assertEquals(2, titleValues.size());
    assertEquals("service-1", titleValues.get(0));
    assertEquals("service-2", titleValues.get(1));

    Map<String, Object> subtitleField = findField(nodeFields, "subtitle");
    assertNotNull(subtitleField);
    List<?> subtitleValues = (List<?>) subtitleField.get("values");
    assertEquals(2, subtitleValues.size());
    assertEquals("resource-1", subtitleValues.get(0));
    assertEquals("resource-2", subtitleValues.get(1));
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

    DataFrameConverter.DataFrame edgesDataFrame = response.data().get(1);
    List<Map<String, Object>> edgeFields = edgesDataFrame.fields();

    Map<String, Object> idField = findField(edgeFields, "id");
    assertNotNull(idField);
    List<?> idValues = (List<?>) idField.get("values");
    assertEquals(2, idValues.size());
    assertEquals("0", idValues.get(0));
    assertEquals("1", idValues.get(1));

    Map<String, Object> sourceField = findField(edgeFields, "source");
    assertNotNull(sourceField);
    List<?> sourceValues = (List<?>) sourceField.get("values");
    assertEquals(2, sourceValues.size());
    assertEquals(node1.getId(), sourceValues.get(0));
    assertEquals(node2.getId(), sourceValues.get(1));

    Map<String, Object> targetField = findField(edgeFields, "target");
    assertNotNull(targetField);
    List<?> targetValues = (List<?>) targetField.get("values");
    assertEquals(2, targetValues.size());
    assertEquals(node2.getId(), targetValues.get(0));
    assertEquals(node3.getId(), targetValues.get(1));

    Map<String, Object> mainstatField = findField(edgeFields, "mainstat");
    assertNotNull(mainstatField);
    List<?> mainstatValues = (List<?>) mainstatField.get("values");
    assertEquals(2, mainstatValues.size());
    assertEquals("http.request", mainstatValues.get(0));
    assertEquals("grpc.request", mainstatValues.get(1));
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

    DataFrameConverter.DataFrame nodesDataFrame = response.data().get(0);
    List<Map<String, Object>> nodeFields = nodesDataFrame.fields();

    Map<String, Object> subtitleField = findField(nodeFields, "subtitle");
    assertNotNull(subtitleField);
    List<?> subtitleValues = (List<?>) subtitleField.get("values");
    assertEquals(2, subtitleValues.size());
    assertEquals("resource-1", subtitleValues.get(0));
    assertEquals("", subtitleValues.get(1));
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

    DataFrameConverter.DataFrame nodesDataFrame = response.data().get(0);
    DataFrameConverter.DataFrame edgesDataFrame = response.data().get(1);

    List<Map<String, Object>> nodeFields = nodesDataFrame.fields();
    assertTrue(nodeFields.size() >= 3);

    for (Map<String, Object> field : nodeFields) {
      List<?> values = (List<?>) field.get("values");
      assertEquals(3, values.size());
    }

    List<Map<String, Object>> edgeFields = edgesDataFrame.fields();
    assertTrue(edgeFields.size() >= 4);

    for (Map<String, Object> field : edgeFields) {
      List<?> values = (List<?>) field.get("values");
      assertEquals(2, values.size());
    }
  }

  private Map<String, Object> findField(List<Map<String, Object>> fields, String fieldName) {
    return fields.stream()
        .filter(field -> fieldName.equals(field.get("name")))
        .findFirst()
        .orElse(null);
  }
}
