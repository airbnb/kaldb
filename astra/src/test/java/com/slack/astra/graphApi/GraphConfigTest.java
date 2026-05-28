package com.slack.astra.graphApi;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.zipkinApi.ZipkinSpanResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class GraphConfigTest {
  @Test
  public void testLoadValidYamlConfig(@TempDir Path tempDir) throws IOException {
    String yamlContent =
        """
              node_metadata_tag_mapping:
                service:
                  default_key:
                    - service.name
                  default_value: unknown_service
                  rules:
                    - field: cluster.name
                      value: prod
                      override_key:
                        - prod.service.name
                    - field: cluster.name
                      value: staging
                      override_key:
                        - test.service.name
                cluster:
                  default_key:
                    - cluster.name
                  default_value: unknown_cluster
              edge_metadata_tag_mapping:
                operation:
                  default_key:
                    - operation_name
                  default_value: unknown_operation
                  rules:
                    - field: cluster.name
                      value: prod
                      override_key:
                        - operation.prod
                product_context:
                  is_annotation: true
                  default_key:
                    - tag.product_context_root_function
                    - tag.product_context
                    - tag.product_context_criticality
                  key_delimiter: ":"
                  default_value: ""
              """;

    Path configFile = tempDir.resolve("test-config.yaml");
    Files.writeString(configFile, yamlContent);

    GraphConfig config = GraphConfig.load(configFile);

    assertThat(config).isNotNull();
    assertThat(config.getNodeMetadataTagMapping()).hasSize(2);
    assertThat(config.getEdgeMetadataTagMapping()).hasSize(2);

    GraphConfig.TagConfig serviceConfig = config.getNodeMetadataTagMapping().get("service");
    assertThat(serviceConfig.getDefaultKey()).containsExactly("service.name");
    assertThat(serviceConfig.getDefaultValue()).isEqualTo("unknown_service");
    assertThat(serviceConfig.getRules()).hasSize(2);

    GraphConfig.RuleConfig rule1 = serviceConfig.getRules().getFirst();
    assertThat(rule1.getOverrideKey()).containsExactly("prod.service.name");
    assertThat(rule1.getField()).isEqualTo("cluster.name");
    assertThat(rule1.getValue()).isEqualTo("prod");

    GraphConfig.RuleConfig rule2 = serviceConfig.getRules().get(1);
    assertThat(rule2.getOverrideKey()).containsExactly("test.service.name");
    assertThat(rule2.getField()).isEqualTo("cluster.name");
    assertThat(rule2.getValue()).isEqualTo("staging");

    GraphConfig.TagConfig clusterConfig = config.getNodeMetadataTagMapping().get("cluster");
    assertThat(clusterConfig.getDefaultKey()).containsExactly("cluster.name");
    assertThat(clusterConfig.getDefaultValue()).isEqualTo("unknown_cluster");
    assertThat(clusterConfig.getRules()).hasSize(0);

    GraphConfig.TagConfig connectionTypeConfig =
        config.getEdgeMetadataTagMapping().get("operation");
    assertThat(connectionTypeConfig.getDefaultKey()).containsExactly("operation_name");
    assertThat(connectionTypeConfig.getDefaultValue()).isEqualTo("unknown_operation");
    assertThat(connectionTypeConfig.getRules()).hasSize(1);
    assertThat(connectionTypeConfig.isAnnotation()).isFalse();

    GraphConfig.TagConfig productContextConfig =
        config.getEdgeMetadataTagMapping().get("product_context");
    assertThat(productContextConfig.isAnnotation()).isTrue();
    assertThat(productContextConfig.getDefaultKey())
        .containsExactly(
            "tag.product_context_root_function",
            "tag.product_context",
            "tag.product_context_criticality");
    assertThat(productContextConfig.getKeyDelimiter()).isEqualTo(":");
  }

  @Test
  public void testLoadNonExistentFile() throws IOException {
    GraphConfig config = GraphConfig.load(Path.of("/non/existent/file.yaml"));
    assertThat(config).isEqualTo(GraphConfig.DEFAULT);
  }

  @Test
  public void testLoadEmptyConfigFile() throws IOException {
    GraphConfig config = GraphConfig.load(Path.of(""));
    assertThat(config).isEqualTo(GraphConfig.DEFAULT);
  }

  @Test
  public void testLoadInvalidYaml(@TempDir Path tempDir) throws IOException {
    String invalidYaml =
        """
        invalid: yaml: content:
          - broken
        """;

    Path configFile = tempDir.resolve("invalid-config.yaml");
    Files.writeString(configFile, invalidYaml);

    GraphConfig config = GraphConfig.load(configFile);
    assertThat(config).isEqualTo(GraphConfig.DEFAULT);
  }

  @Test
  public void testResolveWithDefaultValue() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
             node_metadata_tag_mapping:
               app:
                 default_key:
                   - app.name
                 default_value: unknown_app
               namespace:
                 default_key:
                   - namespace.name
                 default_value: unknown_namespace
             edge_metadata_tag_mapping:
               operation:
                 default_key:
                   - operation_name
                 default_value: unknown_operation
             """);
    Map<String, String> tags = new HashMap<>();
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    // Node
    String result = config.resolve(span, "app", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("unknown_app");

    // Edge
    result = config.resolve(span, "operation", GraphConfig.EntityType.EDGE);
    assertThat(result).isEqualTo("unknown_operation");
  }

  @Test
  public void testResolveWithUnknownField() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key:
                    - app.name
                  default_value: unknown_app
                namespace:
                  default_key:
                    - namespace.name
                  default_value: unknown_namespace
              edge_metadata_tag_mapping:
               operation:
                 default_key:
                   - operation_name
                 default_value: unknown_operation
              """);
    Map<String, String> tags = Map.of("some.tag", "some-value");
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    // Node
    String result = config.resolve(span, "some_field", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("unknown_some_field");

    // Edge
    result = config.resolve(span, "some_field", GraphConfig.EntityType.EDGE);
    assertThat(result).isEqualTo("unknown_some_field");
  }

  @Test
  public void testResolveWithMatchingRule() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key:
                    - app.name
                  default_value: unknown_app
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - prod.app.name
                    - field: cluster.name
                      value: east
                      override_key:
                        - east.app.name
                namespace:
                  default_key:
                    - namespace.name
                  default_value: unknown_namespace
              edge_metadata_tag_mapping:
                operation:
                  default_key:
                    - operation_name
                  default_value: unknown_operation
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - operation.prod
              """);
    Map<String, String> tags =
        Map.of(
            "app.name",
            "my-app",
            "namespace.name",
            "prod-ns",
            "prod.app.name",
            "my-app-in-prod",
            "operation.prod",
            "prod_operation");
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    // Node
    String result = config.resolve(span, "app", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("my-app-in-prod");

    // Edge
    result = config.resolve(span, "operation", GraphConfig.EntityType.EDGE);
    assertThat(result).isEqualTo("prod_operation");
  }

  @Test
  public void testResolveWithMatchingRuleButMissingOverrideKey() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key:
                    - app.name
                  default_value: unknown_app
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - prod.app.name
                    - field: cluster.name
                      value: east
                      override_key:
                        - east.app.name
                namespace:
                  default_key:
                    - namespace.name
                  default_value: unknown_namespace
              edge_metadata_tag_mapping:
                operation:
                  default_key:
                    - operation_name
                  default_value: unknown_operation
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - operation.prod
              """);
    Map<String, String> tags =
        Map.of(
            "app.name", "my-app", "namespace.name", "prod-ns", "operation_name", "some_operation");
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    // Node - rule matches but override key is missing, should return default value
    String result = config.resolve(span, "app", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("unknown_app");

    // Edge - rule matches but override key is missing, should return default value
    result = config.resolve(span, "operation", GraphConfig.EntityType.EDGE);
    assertThat(result).isEqualTo("unknown_operation");
  }

  @Test
  public void testResolveWithNonMatchingRule() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key:
                    - app.name
                  default_value: unknown_app
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - prod.app.name
                    - field: cluster.name
                      value: east
                      override_key:
                        - east.app.name
                namespace:
                  default_key:
                    - namespace.name
                  default_value: unknown_namespace
              edge_metadata_tag_mapping:
                operation:
                  default_key:
                    - operation_name
                  default_value: unknown_operation
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - operation.prod
              """);
    Map<String, String> tags =
        Map.of(
            "app.name", "my-app", "namespace.name", "dev-ns", "operation_name", "some_operation");
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    // Node
    String result = config.resolve(span, "app", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("my-app");

    // Edge
    result = config.resolve(span, "operation", GraphConfig.EntityType.EDGE);
    assertThat(result).isEqualTo("some_operation");
  }

  @Test
  public void testResolveWithMultipleRules() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key:
                    - app.name
                  default_value: unknown_app
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - prod.app.name
                    - field: cluster.name
                      value: east
                      override_key:
                        - east.app.name
                namespace:
                  default_key:
                    - namespace.name
                  default_value: unknown_namespace
              edge_metadata_tag_mapping:
                operation:
                  default_key:
                    - operation_name
                  default_value: unknown_operation
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - operation.prod
                    - field: cluster.name
                      value: east
                      override_key:
                        - operation.east
              """);
    Map<String, String> tags =
        Map.of(
            "app.name",
            "my-app",
            "namespace.name",
            "prod-ns",
            "cluster.name",
            "east",
            "prod.app.name",
            "my-app-in-prod",
            "east.app.name",
            "my-app-east",
            "operation_name",
            "some_operation",
            "operation.prod",
            "operation_prod",
            "operation.east",
            "operation_east");
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    // Node
    String result = config.resolve(span, "app", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("my-app-east");

    // Edge
    result = config.resolve(span, "operation", GraphConfig.EntityType.EDGE);
    assertThat(result).isEqualTo("operation_east");
  }

  @Test
  public void testResolveWithMultipleRulesNoMatch() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key:
                    - app.name
                  default_value: unknown_app
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - prod.app.name
                    - field: cluster.name
                      value: east
                      override_key:
                        - east.app.name
                namespace:
                  default_key:
                    - namespace.name
                  default_value: unknown_namespace
              edge_metadata_tag_mapping:
                operation:
                  default_key:
                    - operation_name
                  default_value: unknown_operation
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - operation.prod
                    - field: cluster.name
                      value: east
                      override_key:
                        - operation_east
              """);
    Map<String, String> tags =
        Map.of(
            "app.name",
            "my-app",
            "namespace.name",
            "dev-ns",
            "cluster.name",
            "west",
            "operation_name",
            "some_operation",
            "operation.prod",
            "operation_prod");
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    // Node
    String result = config.resolve(span, "app", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("my-app");

    // Edge
    result = config.resolve(span, "operation", GraphConfig.EntityType.EDGE);
    assertThat(result).isEqualTo("some_operation");
  }

  @Test
  public void testResolveWithMultipleDefaultKeys() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
                          node_metadata_tag_mapping:
                            service:
                              default_key:
                                - kube.app
                                - kube.namespace
                              default_value: unknown_service
                              key_delimiter: .
                          """);
    Map<String, String> tags = Map.of("kube.app", "my-app", "kube.namespace", "prod");
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    String result = config.resolve(span, "service", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("my-app.prod");
  }

  @Test
  public void testResolveWithMultipleDefaultKeys_missingKey() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
                          node_metadata_tag_mapping:
                            service:
                              default_key:
                                - kube.app
                                - kube.namespace
                              default_value: unknown_service
                              key_delimiter: .
                          """);
    Map<String, String> tags = Map.of("kube.app", "my-app");
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    String result = config.resolve(span, "service", GraphConfig.EntityType.NODE);
    // Should return default value when any key is missing
    assertThat(result).isEqualTo("unknown_service");
  }

  @Test
  public void testResolveWithSingleDefaultKey() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
                          node_metadata_tag_mapping:
                            service:
                              default_key:
                                - kube.app
                              default_value: unknown_service
                          """);
    Map<String, String> tags = Map.of("kube.app", "my-app");
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    String result = config.resolve(span, "service", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("my-app");
  }

  @Test
  public void testResolveWithMultipleOverrideKeys() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
                          node_metadata_tag_mapping:
                            service:
                              default_key:
                                - kube.app
                                - kube.namespace
                              default_value: unknown_service
                              key_delimiter: .
                              rules:
                                - field: operation_name
                                  value: http.request
                                  override_key:
                                    - tag.http.target.host
                                    - tag.http.method
                          """);
    Map<String, String> tags =
        Map.of(
            "kube.app",
            "my-app",
            "kube.namespace",
            "prod",
            "operation_name",
            "http.request",
            "tag.http.target.host",
            "api.example.com",
            "tag.http.method",
            "GET");
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    String result = config.resolve(span, "service", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("api.example.com.GET");
  }

  @Test
  public void testResolveWithMultipleOverrideKeys_missingKey() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
                          node_metadata_tag_mapping:
                            service:
                              default_key:
                                - kube.app
                                - kube.namespace
                              default_value: unknown_service
                              key_delimiter: .
                              rules:
                                - field: operation_name
                                  value: http.request
                                  override_key:
                                    - tag.http.target.host
                                    - tag.http.method
                          """);
    Map<String, String> tags =
        Map.of(
            "kube.app",
            "my-app",
            "kube.namespace",
            "prod",
            "operation_name",
            "http.request",
            "tag.http.target.host",
            "api.example.com");
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    String result = config.resolve(span, "service", GraphConfig.EntityType.NODE);
    // Should return default value since rule matched but override key is missing tag.http.method
    assertThat(result).isEqualTo("unknown_service");
  }

  @Test
  public void testCreateNodeMetadataFromSpan_defaultConfig_usesRemoteEndpointServiceName() {
    GraphConfig config = GraphConfig.DEFAULT;
    ZipkinSpanResponse span =
        TestUtils.createSpanWithTags("span1", "trace1", null, Map.of("some.tag", "some-value"));

    SortedMap<String, String> metadata =
        config.createMetadataFromSpan(span, GraphConfig.EntityType.NODE);
    assertThat(metadata).hasSize(1);
    assertThat(metadata.get("service")).isEqualTo("default-service");
  }

  @Test
  public void testCreatNodeMetadataFromSpan_customConfig_usesTagMapping() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
      node_metadata_tag_mapping:
        app:
          default_key:
            - app.name
          default_value: unknown_app
        namespace:
          default_key:
            - namespace.name
          default_value: unknown_namespace
        resource:
          default_key:
            - resource.name
          default_value: unknown_resource
      """);

    Map<String, String> tags =
        Map.of(
            "app.name", "my-app",
            "namespace.name", "my-namespace",
            "resource.name", "my-resource");
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    SortedMap<String, String> metadata =
        config.createMetadataFromSpan(span, GraphConfig.EntityType.NODE);

    assertThat(metadata).hasSize(3);
    assertThat(metadata.get("app")).isEqualTo("my-app");
    assertThat(metadata.get("namespace")).isEqualTo("my-namespace");
    assertThat(metadata.get("resource")).isEqualTo("my-resource");
  }

  @Test
  public void testResolveWithMultipleMatchingRules_emptyValue_fallbackToNextRule()
      throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
                node_metadata_tag_mapping:
                  resource:
                    default_key:
                      - resource
                    default_value: unknown_resource
                    rules:
                      - field: operation_name
                        value: http.request
                        override_key:
                          - http.url
                      - field: operation_name
                        value: http.request
                        override_key:
                          - tag.http.target.canonical_path
                """);

    // Case 1: tag.http.target.canonical_path is empty, should fallback to http.url
    Map<String, String> tags1 =
        Map.of(
            "operation_name",
            "http.request",
            "tag.http.target.canonical_path",
            "",
            "http.url",
            "/api/v1/users",
            "resource",
            "default_resource");
    ZipkinSpanResponse span1 = TestUtils.createSpanWithTags("span1", "trace1", null, tags1);

    String result1 = config.resolve(span1, "resource", GraphConfig.EntityType.NODE);
    assertThat(result1).isEqualTo("/api/v1/users");

    // Case 2: Both are empty, should fallback to default value
    Map<String, String> tags2 =
        Map.of(
            "operation_name",
            "http.request",
            "tag.http.target.canonical_path",
            "",
            "http.url",
            "",
            "resource",
            "default_resource");
    ZipkinSpanResponse span2 = TestUtils.createSpanWithTags("span2", "trace1", null, tags2);

    String result2 = config.resolve(span2, "resource", GraphConfig.EntityType.NODE);
    assertThat(result2).isEqualTo("unknown_resource");

    // Case 3: tag.http.target.canonical_path has value, should use it
    Map<String, String> tags4 =
        Map.of(
            "operation_name",
            "http.request",
            "tag.http.target.canonical_path",
            "/api/canonical",
            "http.url",
            "/api/v1/users",
            "resource",
            "default_resource");
    ZipkinSpanResponse span4 = TestUtils.createSpanWithTags("span4", "trace1", null, tags4);

    String result4 = config.resolve(span4, "resource", GraphConfig.EntityType.NODE);
    assertThat(result4).isEqualTo("/api/canonical");

    // Case 4: tag.http.target.canonical_path is missing, should fallback to http.url
    Map<String, String> tags5 =
        Map.of(
            "operation_name",
            "http.request",
            "http.url",
            "/api/v1/users",
            "resource",
            "default_resource");
    ZipkinSpanResponse span5 = TestUtils.createSpanWithTags("span5", "trace1", null, tags5);

    String result5 = config.resolve(span5, "resource", GraphConfig.EntityType.NODE);
    assertThat(result5).isEqualTo("/api/v1/users");

    // Case 5: Both override keys are missing, should fallback to default value
    Map<String, String> tags6 =
        Map.of("operation_name", "http.request", "resource", "default_resource");
    ZipkinSpanResponse span6 = TestUtils.createSpanWithTags("span6", "trace1", null, tags6);

    String result6 = config.resolve(span6, "resource", GraphConfig.EntityType.NODE);
    assertThat(result6).isEqualTo("unknown_resource");
  }

  @Test
  public void testResolveProject_withSpanServiceDefaultAndTagOverride() throws IOException {
    GraphConfig config =
        GraphConfig.load(
"""
                node_metadata_tag_mapping:
                  project:
                    default_key:
                      - service_name
                    default_value: unknown_project
                    use_default_key_on_empty_override: false
                    rules:
                      - field: operation_name
                        value: http.request
                        override_key:
                          - tag.http.target.service
""");

    // Case 1: operation_name != "http.request" → falls back to service_name (remote endpoint)
    Map<String, String> tags1 = Map.of("operation_name", "grpc.request");
    ZipkinSpanResponse span1 = TestUtils.createSpanWithTags("span1", "trace1", null, tags1);
    String result1 = config.resolve(span1, "project", GraphConfig.EntityType.NODE);
    assertThat(result1).isEqualTo("default-service");

    // Case 2: operation_name = "http.request" with tag.http.target.service → uses override key
    Map<String, String> tags2 =
        Map.of("operation_name", "http.request", "tag.http.target.service", "my-project");
    ZipkinSpanResponse span2 = TestUtils.createSpanWithTags("span2", "trace1", null, tags2);
    String result2 = config.resolve(span2, "project", GraphConfig.EntityType.NODE);
    assertThat(result2).isEqualTo("my-project");

    // Case 3: operation_name = "http.request" but tag.http.target.service missing → returns default
    // value
    Map<String, String> tags3 = Map.of("operation_name", "http.request");
    ZipkinSpanResponse span3 = TestUtils.createSpanWithTags("span3", "trace1", null, tags3);
    String result3 = config.resolve(span3, "project", GraphConfig.EntityType.NODE);
    assertThat(result3).isEqualTo("unknown_project");
  }

  @Test
  public void testResolveWithUseDefaultKeyOnEmptyOverride_true_fallsBackToDefaultKey()
      throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
                node_metadata_tag_mapping:
                  service:
                    default_key:
                      - kube.app
                      - kube.namespace
                    default_value: unknown_service
                    key_delimiter: .
                    use_default_key_on_empty_override: true
                    rules:
                      - field: operation_name
                        value: http.request
                        override_key:
                          - tag.http.target.host
                """);

    // Rule matches but override key is missing — falls back to defaultKey
    Map<String, String> tags1 =
        Map.of("kube.app", "my-app", "kube.namespace", "prod", "operation_name", "http.request");
    ZipkinSpanResponse span1 = TestUtils.createSpanWithTags("span1", "trace1", null, tags1);
    assertThat(config.resolve(span1, "service", GraphConfig.EntityType.NODE))
        .isEqualTo("my-app.prod");

    // No rule matches — also falls back to defaultKey
    Map<String, String> tags2 =
        Map.of("kube.app", "my-app", "kube.namespace", "prod", "operation_name", "grpc.request");
    ZipkinSpanResponse span2 = TestUtils.createSpanWithTags("span2", "trace1", null, tags2);
    assertThat(config.resolve(span2, "service", GraphConfig.EntityType.NODE))
        .isEqualTo("my-app.prod");
  }

  @Test
  public void testCreateEdgeMetadataFromSpan_defaultConfig_isEmpty() {
    GraphConfig config = GraphConfig.DEFAULT;
    ZipkinSpanResponse span =
        TestUtils.createSpanWithTags("span1", "trace1", null, Map.of("some.tag", "some-value"));

    SortedMap<String, String> metadata =
        config.createMetadataFromSpan(span, GraphConfig.EntityType.EDGE);
    assertThat(metadata).isEmpty();
  }

  @Test
  public void testCreatEdgeMetadataFromSpan_customConfig_usesTagMapping() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
                  edge_metadata_tag_mapping:
                    operation:
                      default_key:
                        - operation_name
                      default_value: unknown_operation
                  """);

    Map<String, String> tags = Map.of("operation_name", "some_operation");
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    SortedMap<String, String> metadata =
        config.createMetadataFromSpan(span, GraphConfig.EntityType.EDGE);

    assertThat(metadata).hasSize(1);
    assertThat(metadata.get("operation")).isEqualTo("some_operation");
  }

  @Test
  public void testCreateMetadataFromSpan_edgeExcludesAnnotationFields() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
            edge_metadata_tag_mapping:
              operation:
                default_key: [operation_name]
                default_value: unknown_operation
              product_context:
                is_annotation: true
                default_key:
                  - tag.product_context_root_function
                  - tag.product_context
                  - tag.product_context_criticality
                key_delimiter: ":"
                default_value: ""
            """);

    ZipkinSpanResponse span =
        TestUtils.createSpanWithTags(
            "s1",
            "t1",
            null,
            Map.of(
                "operation_name", "http.request",
                "tag.product_context_root_function", "FOO",
                "tag.product_context", "FOO__BAR__BAZ",
                "tag.product_context_criticality", "1"));

    SortedMap<String, String> metadata =
        config.createMetadataFromSpan(span, GraphConfig.EntityType.EDGE);

    assertThat(metadata).containsOnlyKeys("operation");
    assertThat(metadata.get("operation")).isEqualTo("http.request");
  }

  @Test
  public void testResolveAnnotationsForSpan_allThreeTags_returnsJoinedString() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
            edge_metadata_tag_mapping:
              product_context:
                is_annotation: true
                default_key:
                  - tag.product_context_root_function
                  - tag.product_context
                  - tag.product_context_criticality
                key_delimiter: ":"
                default_value: ""
            """);

    ZipkinSpanResponse span =
        TestUtils.createSpanWithTags(
            "s1",
            "t1",
            null,
            Map.of(
                "tag.product_context_root_function", "FOO",
                "tag.product_context", "FOO__BAR__BAZ",
                "tag.product_context_criticality", "1"));

    SortedMap<String, String> annotations =
        config.resolveAnnotationsForSpan(span, id -> null, new HashMap<>());

    assertThat(annotations).containsEntry("product_context", "FOO:FOO__BAR__BAZ:1");
  }

  @Test
  public void testResolveAnnotationsForSpan_missingOneTag_walksUpToAncestor() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
            edge_metadata_tag_mapping:
              product_context:
                is_annotation: true
                default_key:
                  - tag.product_context_root_function
                  - tag.product_context
                  - tag.product_context_criticality
                key_delimiter: ":"
                default_value: ""
            """);

    ZipkinSpanResponse parent =
        TestUtils.createSpanWithTags(
            "parent",
            "t1",
            null,
            Map.of(
                "tag.product_context_root_function", "FOO",
                "tag.product_context", "FOO__BAR__BAZ",
                "tag.product_context_criticality", "1"));

    // Missing criticality — treated as not carrying PC, inherits from parent.
    ZipkinSpanResponse child =
        TestUtils.createSpanWithTags(
            "child",
            "t1",
            "parent",
            Map.of(
                "tag.product_context_root_function", "QUX",
                "tag.product_context", "QUX__QUUX__CORGE"));

    Map<String, ZipkinSpanResponse> spanMap = Map.of("parent", parent, "child", child);
    SortedMap<String, String> annotations =
        config.resolveAnnotationsForSpan(child, spanMap::get, new HashMap<>());

    assertThat(annotations).containsEntry("product_context", "FOO:FOO__BAR__BAZ:1");
  }

  @Test
  public void testResolveAnnotationsForSpan_noPcAnywhere_returnsEmptyMap() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
            edge_metadata_tag_mapping:
              product_context:
                is_annotation: true
                default_key:
                  - tag.product_context_root_function
                  - tag.product_context
                  - tag.product_context_criticality
                key_delimiter: ":"
                default_value: ""
            """);

    ZipkinSpanResponse span =
        TestUtils.createSpanWithTags("s1", "t1", null, Map.of("operation_name", "db.query"));

    SortedMap<String, String> annotations =
        config.resolveAnnotationsForSpan(span, id -> null, new HashMap<>());

    assertThat(annotations).isEmpty();
  }

  @Test
  public void testResolveAnnotationsForSpan_stopsAtNearestAncestor() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
            edge_metadata_tag_mapping:
              category:
                is_annotation: true
                default_key: [tag.category]
                default_value: ""
            """);

    // grandparent has annotation "cat_gp", parent has annotation "cat_p", child has none.
    // Child should inherit "cat_p" (nearest), not "cat_gp".
    ZipkinSpanResponse grandparent =
        TestUtils.createSpanWithTags("gp", "t1", null, Map.of("tag.category", "cat_gp"));
    ZipkinSpanResponse parent =
        TestUtils.createSpanWithTags("p", "t1", "gp", Map.of("tag.category", "cat_p"));
    ZipkinSpanResponse child = TestUtils.createSpanWithTags("c", "t1", "p", Map.of());

    Map<String, ZipkinSpanResponse> spanMap = Map.of("gp", grandparent, "p", parent, "c", child);
    SortedMap<String, String> annotations =
        config.resolveAnnotationsForSpan(child, spanMap::get, new HashMap<>());

    assertThat(annotations).containsEntry("category", "cat_p");
  }

  @Test
  public void testResolveAnnotationsForSpan_cycleInParentChain_terminates() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
            edge_metadata_tag_mapping:
              category:
                is_annotation: true
                default_key: [tag.category]
                default_value: ""
            """);

    // A's parent is B, B's parent is A — cycle, neither carries the annotation
    ZipkinSpanResponse spanA = TestUtils.createSpanWithTags("A", "t1", "B", Map.of());
    ZipkinSpanResponse spanB = TestUtils.createSpanWithTags("B", "t1", "A", Map.of());

    Map<String, ZipkinSpanResponse> spanMap = Map.of("A", spanA, "B", spanB);
    SortedMap<String, String> annotations =
        config.resolveAnnotationsForSpan(spanA, spanMap::get, new HashMap<>());

    assertThat(annotations).isEmpty();
  }

  @Test
  public void testResolveAnnotationsForSpan_siblingsDifferentAncestorAnnotations_eachInheritsOwn()
      throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
            edge_metadata_tag_mapping:
              category:
                is_annotation: true
                default_key: [tag.category]
                default_value: ""
            """);

    ZipkinSpanResponse rootA =
        TestUtils.createSpanWithTags("rootA", "t1", null, Map.of("tag.category", "cat_a"));
    ZipkinSpanResponse childA = TestUtils.createSpanWithTags("childA", "t1", "rootA", Map.of());

    ZipkinSpanResponse rootB =
        TestUtils.createSpanWithTags("rootB", "t1", null, Map.of("tag.category", "cat_b"));
    ZipkinSpanResponse childB = TestUtils.createSpanWithTags("childB", "t1", "rootB", Map.of());

    Map<String, ZipkinSpanResponse> spanMap =
        Map.of("rootA", rootA, "childA", childA, "rootB", rootB, "childB", childB);
    Map<String, SortedMap<String, String>> annotationsBySpanId = new HashMap<>();

    assertThat(config.resolveAnnotationsForSpan(childA, spanMap::get, annotationsBySpanId))
        .containsEntry("category", "cat_a");
    assertThat(config.resolveAnnotationsForSpan(childB, spanMap::get, annotationsBySpanId))
        .containsEntry("category", "cat_b");
  }

  @Test
  public void testResolveAnnotationsForSpan_cycleWithAnnotations_eachSpanKeepsOwnValue()
      throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
            edge_metadata_tag_mapping:
              category:
                is_annotation: true
                default_key: [tag.category]
                default_value: ""
            """);

    // A's parent is B, B's parent is A — cycle, each carries its own annotation value.
    // Each should resolve to its own value, not inherit the other's.
    ZipkinSpanResponse spanA =
        TestUtils.createSpanWithTags("A", "t1", "B", Map.of("tag.category", "cat_a"));
    ZipkinSpanResponse spanB =
        TestUtils.createSpanWithTags("B", "t1", "A", Map.of("tag.category", "cat_b"));

    Map<String, ZipkinSpanResponse> spanMap = Map.of("A", spanA, "B", spanB);
    Map<String, SortedMap<String, String>> annotationsBySpanId = new HashMap<>();

    assertThat(config.resolveAnnotationsForSpan(spanA, spanMap::get, annotationsBySpanId))
        .containsEntry("category", "cat_a");
    assertThat(config.resolveAnnotationsForSpan(spanB, spanMap::get, annotationsBySpanId))
        .containsEntry("category", "cat_b");
  }
}
