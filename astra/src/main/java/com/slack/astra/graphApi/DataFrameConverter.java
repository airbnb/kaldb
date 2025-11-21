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
    return new DataFrameResponse(
        convertNodesToDataFrame(graph.nodes()), convertEdgesToDataFrame(graph.edges()));
  }

  public record DataFrameResponse(
      List<Map<String, Object>> nodes, List<Map<String, Object>> edges) {}

  private List<Map<String, Object>> convertNodesToDataFrame(List<Node> nodes) {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (Node node : nodes) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", node.getId());

      // Process metadata using config
      for (Map.Entry<String, GraphConfig.TagConfig> entry :
          config.getNodeMetadataTagMapping().entrySet()) {
        String metadataKey = entry.getKey();
        String dataFrameField = entry.getValue().getDataFrameField();
        String value = node.getMetadata().getOrDefault(metadataKey, "");

        if (dataFrameField != null && !dataFrameField.isEmpty()) {
          row.put(dataFrameField, value);
        } else {
          row.put("detail__" + metadataKey, value);
        }
      }
      rows.add(row);
    }
    return rows;
  }

  private List<Map<String, Object>> convertEdgesToDataFrame(List<Edge> edges) {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < edges.size(); i++) {
      Edge edge = edges.get(i);
      Map<String, Object> row = new HashMap<>();
      row.put("id", String.valueOf(i));
      row.put("source", edge.sourceNodeId());
      row.put("target", edge.targetNodeId());

      // Process metadata using config
      for (Map.Entry<String, GraphConfig.TagConfig> entry :
          config.getEdgeMetadataTagMapping().entrySet()) {
        String metadataKey = entry.getKey();
        String dataFrameField = entry.getValue().getDataFrameField();
        String value = edge.metadata().getOrDefault(metadataKey, "");

        if (dataFrameField != null && !dataFrameField.isEmpty()) {
          row.put(dataFrameField, value);
        } else {
          row.put("detail__" + metadataKey, value);
        }
      }
      rows.add(row);
    }
    return rows;
  }
}
