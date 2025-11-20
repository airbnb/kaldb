package com.slack.astra.graphApi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts Graph data to Grafana node graph data frame format.
 *
 * <p>Grafana's node graph visualization requires data in a specific format with separate node and
 * edge data frames. This converter transforms Astra's Graph structure into the required format,
 * using GraphConfig to map metadata fields to appropriate Grafana data frame fields.
 *
 * @see <a
 *     href="https://grafana.com/docs/grafana/latest/panels-visualizations/visualizations/node-graph/">Grafana
 *     Node Graph Documentation</a>
 */
public class DataFrameConverter {
  private static final Logger LOG = LoggerFactory.getLogger(DataFrameConverter.class);
  private final GraphConfig config;

  public DataFrameConverter(GraphConfig config) {
    this.config = config;
  }

  public DataFrameResponse graphToDataFrame(Graph graph) {
    List<DataFrame> data = new ArrayList<>();
    data.add(convertNodesToDataFrame(graph.nodes()));
    data.add(convertEdgesToDataFrame(graph.edges()));
    return new DataFrameResponse(data);
  }

  public record DataFrameResponse(List<DataFrame> data) {}

  public record DataFrame(
      String name, List<Map<String, Object>> fields, Map<String, String> meta) {}

  /**
   * Converts nodes to Grafana node data frame format.
   *
   * <p>Uses GraphConfig to determine which metadata fields map to which data frame fields.
   */
  private DataFrame convertNodesToDataFrame(List<Node> nodes) {
    List<Map<String, Object>> fields = new ArrayList<>();

    // Required field: id
    Map<String, Object> idField = new HashMap<>();
    idField.put("name", "id");
    idField.put("type", "string");
    idField.put("values", nodes.stream().map(Node::getId).toList());
    fields.add(idField);

    Map<String, List<Object>> fieldValues = new HashMap<>();
    nodes.forEach(
        node ->
            processMetadata(config.getNodeMetadataTagMapping(), fieldValues, node.getMetadata()));

    processMetadataFieldValues(GraphConfig.EntityType.NODE, fields, fieldValues, nodes.size());

    Map<String, String> meta = new HashMap<>();
    meta.put("preferredVisualisationType", "nodeGraph");

    return new DataFrame("nodes", fields, meta);
  }

  /**
   * Converts edges to Grafana edge data frame format.
   *
   * <p>Uses GraphConfig to determine which metadata fields map to which data frame fields.
   */
  private DataFrame convertEdgesToDataFrame(List<Edge> edges) {
    List<Map<String, Object>> fields = new ArrayList<>();

    // Required field: id
    Map<String, Object> idField = new HashMap<>();
    idField.put("name", "id");
    idField.put("type", "string");
    List<String> edgeIds = new ArrayList<>();
    for (int i = 0; i < edges.size(); i++) {
      edgeIds.add(String.valueOf(i));
    }
    idField.put("values", edgeIds);
    fields.add(idField);

    // Required field: source
    Map<String, Object> sourceField = new HashMap<>();
    sourceField.put("name", "source");
    sourceField.put("type", "string");
    sourceField.put("values", edges.stream().map(Edge::sourceNodeId).toList());
    fields.add(sourceField);

    // Required field: target
    Map<String, Object> targetField = new HashMap<>();
    targetField.put("name", "target");
    targetField.put("type", "string");
    targetField.put("values", edges.stream().map(Edge::targetNodeId).toList());
    fields.add(targetField);

    Map<String, List<Object>> fieldValues = new HashMap<>();
    edges.forEach(
        edge -> processMetadata(config.getEdgeMetadataTagMapping(), fieldValues, edge.metadata()));
    processMetadataFieldValues(GraphConfig.EntityType.EDGE, fields, fieldValues, edges.size());

    Map<String, String> meta = new HashMap<>();
    meta.put("preferredVisualisationType", "nodeGraph");

    return new DataFrame("edges", fields, meta);
  }

  private static void processMetadata(
      Map<String, GraphConfig.TagConfig> nodeMetadataMapping,
      Map<String, List<Object>> fieldValues,
      Map<String, String> metadata) {
    for (Map.Entry<String, GraphConfig.TagConfig> entry : nodeMetadataMapping.entrySet()) {
      String metadataKey = entry.getKey();
      String dataFrameField = entry.getValue().getDataFrameField();

      String value = metadata.getOrDefault(metadataKey, "");
      if (dataFrameField != null && !dataFrameField.isEmpty()) {
        fieldValues.computeIfAbsent(dataFrameField, k -> new ArrayList<>()).add(value);
      } else {
        fieldValues.computeIfAbsent("detail__" + metadataKey, k -> new ArrayList<>()).add(value);
      }
    }
  }

  private static void processMetadataFieldValues(
      GraphConfig.EntityType entityType,
      List<Map<String, Object>> fields,
      Map<String, List<Object>> fieldValues,
      int size)
      throws IllegalStateException {
    for (Map.Entry<String, List<Object>> entry : fieldValues.entrySet()) {
      String fieldName = entry.getKey();
      List<Object> values = entry.getValue();

      // Only add if we have values for all nodes
      if (values.size() == size) {
        Map<String, Object> field = new HashMap<>();
        field.put("name", fieldName);
        field.put("type", "string");
        field.put("values", values);
        fields.add(field);
      } else {
        LOG.error(
            "Data frame value size mismatch for {}. Expected size {}, got {}",
            entityType,
            size,
            values.size());
        throw new IllegalStateException(
            "Generated data frame has an invalid size for " + entityType);
      }
    }
  }
}
