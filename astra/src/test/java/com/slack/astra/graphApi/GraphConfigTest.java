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
            default_key: service.name
            default_value: unknown_service
            rules:
              - field: cluster.name
                value: prod
                override_key: prod.service.name
              - field: cluster.name
                value: staging
                override_key: test.service.name
          cluster:
            default_key: cluster.name
            default_value: unknown_cluster
        """;

    Path configFile = tempDir.resolve("test-config.yaml");
    Files.writeString(configFile, yamlContent);

    GraphConfig config = GraphConfig.load(configFile);

    assertThat(config).isNotNull();
    assertThat(config.getNodeMetadataTagMapping()).hasSize(2);

    GraphConfig.TagConfig serviceConfig = config.getNodeMetadataTagMapping().get("service");
    assertThat(serviceConfig.getDefaultKey()).isEqualTo("service.name");
    assertThat(serviceConfig.getDefaultValue()).isEqualTo("unknown_service");
    assertThat(serviceConfig.getRules()).hasSize(2);

    GraphConfig.RuleConfig rule1 = serviceConfig.getRules().getFirst();
    assertThat(rule1.getOverrideKey()).isEqualTo("prod.service.name");
    assertThat(rule1.getField()).isEqualTo("cluster.name");
    assertThat(rule1.getValue()).isEqualTo("prod");

    GraphConfig.RuleConfig rule2 = serviceConfig.getRules().get(1);
    assertThat(rule2.getOverrideKey()).isEqualTo("test.service.name");
    assertThat(rule2.getField()).isEqualTo("cluster.name");
    assertThat(rule2.getValue()).isEqualTo("staging");

    GraphConfig.TagConfig clusterConfig = config.getNodeMetadataTagMapping().get("cluster");
    assertThat(clusterConfig.getDefaultKey()).isEqualTo("cluster.name");
    assertThat(clusterConfig.getDefaultValue()).isEqualTo("unknown_cluster");
    assertThat(clusterConfig.getRules()).hasSize(0);
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
                 default_key: app.name
                 default_value: unknown_app
               namespace:
                 default_key: namespace.name
                 default_value: unknown_namespace
             """);
    Map<String, String> tags = new HashMap<>();

    String result = config.resolve(tags, "app");
    assertThat(result).isEqualTo("unknown_app");
  }

  @Test
  public void testResolveWithUnknownField() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key: app.name
                  default_value: unknown_app
                namespace:
                  default_key: namespace.name
                  default_value: unknown_namespace
              """);
    Map<String, String> tags = Map.of("some.tag", "some-value");

    String result = config.resolve(tags, "some_field");
    assertThat(result).isEqualTo("unknown_some_field");
  }

  @Test
  public void testResolveWithMatchingRule() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key: app.name
                  default_value: unknown_app
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key: prod.app.name
                    - field: cluster.name
                      value: east
                      override_key: east.app.name
                namespace:
                  default_key: namespace.name
                  default_value: unknown_namespace
              """);
    Map<String, String> tags =
        Map.of(
            "app.name", "my-app", "namespace.name", "prod-ns", "prod.app.name", "my-app-in-prod");

    String result = config.resolve(tags, "app");
    assertThat(result).isEqualTo("my-app-in-prod");
  }

  @Test
  public void testResolveWithMatchingRuleButMissingOverrideKey() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key: app.name
                  default_value: unknown_app
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key: prod.app.name
                    - field: cluster.name
                      value: east
                      override_key: east.app.name
                namespace:
                  default_key: namespace.name
                  default_value: unknown_namespace
              """);
    Map<String, String> tags = Map.of("app.name", "my-app", "namespace.name", "prod-ns");

    String result = config.resolve(tags, "app");
    assertThat(result).isEqualTo("my-app");
  }

  @Test
  public void testResolveWithNonMatchingRule() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key: app.name
                  default_value: unknown_app
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key: prod.app.name
                    - field: cluster.name
                      value: east
                      override_key: east.app.name
                namespace:
                  default_key: namespace.name
                  default_value: unknown_namespace
              """);
    Map<String, String> tags =
        Map.of(
            "app.name", "my-app",
            "namespace.name", "dev-ns");

    String result = config.resolve(tags, "app");
    assertThat(result).isEqualTo("my-app");
  }

  @Test
  public void testResolveWithMultipleRules() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key: app.name
                  default_value: unknown_app
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key: prod.app.name
                    - field: cluster.name
                      value: east
                      override_key: east.app.name
                namespace:
                  default_key: namespace.name
                  default_value: unknown_namespace
              """);
    Map<String, String> tags =
        Map.of(
            "app.name", "my-app",
            "namespace.name", "prod-ns",
            "cluster.name", "east",
            "prod.app.name", "my-app-in-prod",
            "east.app.name", "my-app-east");

    String result = config.resolve(tags, "app");
    assertThat(result).isEqualTo("my-app-east");
  }

  @Test
  public void testResolveWithMultipleRulesNoMatch() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key: app.name
                  default_value: unknown_app
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key: prod.app.name
                    - field: cluster.name
                      value: east
                      override_key: east.app.name
                namespace:
                  default_key: namespace.name
                  default_value: unknown_namespace
              """);
    Map<String, String> tags =
        Map.of(
            "app.name", "my-app",
            "namespace.name", "dev-ns",
            "cluster.name", "west");

    String result = config.resolve(tags, "app");
    assertThat(result).isEqualTo("my-app");
  }

  @Test
  public void testCreateMetadataFromSpan_defaultConfig_usesRemoteEndpointServiceName() {
    GraphConfig config = GraphConfig.DEFAULT;
    ZipkinSpanResponse span =
        TestUtils.createSpanWithTags("span1", "trace1", null, Map.of("some.tag", "some-value"));

    SortedMap<String, String> metadata = config.createMetadataFromSpan(span);
    assertThat(metadata).hasSize(1);
    assertThat(metadata.get("service")).isEqualTo("default-service");
  }

  @Test
  public void testCreateMetadataFromSpan_customConfig_usesTagMapping() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
      node_metadata_tag_mapping:
        app:
          default_key: app.name
          default_value: unknown_app
        namespace:
          default_key: namespace.name
          default_value: unknown_namespace
        resource:
          default_key: resource.name
          default_value: unknown_resource
      """);

    Map<String, String> tags =
        Map.of(
            "app.name", "my-app",
            "namespace.name", "my-namespace",
            "resource.name", "my-resource");
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    SortedMap<String, String> metadata = config.createMetadataFromSpan(span);

    assertThat(metadata).hasSize(3);
    assertThat(metadata.get("app")).isEqualTo("my-app");
    assertThat(metadata.get("namespace")).isEqualTo("my-namespace");
    assertThat(metadata.get("resource")).isEqualTo("my-resource");
  }
}
