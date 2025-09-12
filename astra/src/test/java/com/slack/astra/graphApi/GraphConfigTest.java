package com.slack.astra.graphApi;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class GraphConfigTest {

  @Test
  public void testTagConfigGettersAndSetters() {
    GraphConfig.TagConfig tagConfig = new GraphConfig.TagConfig();

    tagConfig.setDefaultKey("testKey");
    tagConfig.setDefaultValue("testValue");

    assertThat(tagConfig.getDefaultKey()).isEqualTo("testKey");
    assertThat(tagConfig.getDefaultValue()).isEqualTo("testValue");
  }

  @Test
  public void testRuleConfigGettersAndSetters() {
    GraphConfig.RuleConfig ruleConfig = new GraphConfig.RuleConfig();

    ruleConfig.setField("testField");
    ruleConfig.setValue("testValue");
    ruleConfig.setOverrideKey("overrideKey");

    assertThat(ruleConfig.getField()).isEqualTo("testField");
    assertThat(ruleConfig.getValue()).isEqualTo("testValue");
    assertThat(ruleConfig.getOverrideKey()).isEqualTo("overrideKey");
  }

  @Test
  public void testGraphConfigGettersAndSetters() {
    GraphConfig graphConfig = new GraphConfig();
    Map<String, GraphConfig.TagConfig> nodeMetadataTagMapping = new HashMap<>();

    graphConfig.setNodeMetadataTagMapping(nodeMetadataTagMapping);

    assertThat(graphConfig.getNodeMetadataTagMapping()).isEqualTo(nodeMetadataTagMapping);
  }

  @Test
  public void testLoadValidYamlConfig(@TempDir Path tempDir) throws IOException {
    String yamlContent =
        """
        nodeMetadataTagMapping:
          service:
            defaultKey: service.name
            defaultValue: unknown_service
            rules:
              - field: cluster.name
                value: prod
                overrideKey: prod.service.name
              - field: cluster.name
                value: staging
                overrideKey: test.service.name
          cluster:
            defaultKey: cluster.name
            defaultValue: unknown_cluster
        """;

    Path configFile = tempDir.resolve("test-config.yaml");
    Files.writeString(configFile, yamlContent);

    GraphConfig config = GraphConfig.load(configFile.toString());

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
  public void testLoadEmptyConfigFile() throws IOException {
    GraphConfig config = GraphConfig.load("");
    assertThat(config).isNull();
  }

  @Test
  public void testLoadNonExistentFile() throws IOException {
    GraphConfig config = GraphConfig.load("/non/existent/file.yaml");
    assertThat(config).isNull();
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

    GraphConfig config = GraphConfig.load(configFile.toString());
    assertThat(config).isNull();
  }

  @Test
  public void testResolveWithDefaultValue() {
    GraphConfig config = createTestConfig();
    Map<String, String> tags = new HashMap<>();

    String result = config.resolve(tags, "app");
    assertThat(result).isEqualTo("unknown_app");
  }

  @Test
  public void testResolveWithUnknownField() {
    GraphConfig config = createTestConfig();
    Map<String, String> tags = Map.of("some.tag", "some-value");

    String result = config.resolve(tags, "some_field");
    assertThat(result).isEqualTo("unknown_some_field");
  }

  @Test
  public void testResolveWithMatchingRule() {
    GraphConfig config = createTestConfigWithRules();
    Map<String, String> tags =
        Map.of(
            "app.name", "my-app", "namespace.name", "prod-ns", "prod.app.name", "my-app-in-prod");

    String result = config.resolve(tags, "app");
    assertThat(result).isEqualTo("my-app-in-prod");
  }

  @Test
  public void testResolveWithMatchingRuleButMissingOverrideKey() {
    GraphConfig config = createTestConfigWithRules();
    Map<String, String> tags = Map.of("app.name", "my-app", "namespace.name", "prod-ns");

    String result = config.resolve(tags, "app");
    assertThat(result).isEqualTo("my-app");
  }

  @Test
  public void testResolveWithNonMatchingRule() {
    GraphConfig config = createTestConfigWithRules();
    Map<String, String> tags =
        Map.of(
            "app.name", "my-app",
            "namespace.name", "dev-ns");

    String result = config.resolve(tags, "app");
    assertThat(result).isEqualTo("my-app");
  }

  @Test
  public void testResolveWithMultipleRules() {
    GraphConfig config = createTestConfigWithRules();
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
  public void testResolveWithMultipleRulesNoMatch() {
    GraphConfig config = createTestConfigWithRules();
    Map<String, String> tags =
        Map.of(
            "app.name", "my-app",
            "namespace.name", "dev-ns",
            "cluster.name", "west");

    String result = config.resolve(tags, "app");
    assertThat(result).isEqualTo("my-app");
  }

  private GraphConfig createTestConfig() {
    GraphConfig config = new GraphConfig();
    Map<String, GraphConfig.TagConfig> tagMapping = new HashMap<>();

    GraphConfig.TagConfig appConfig = new GraphConfig.TagConfig();
    appConfig.setDefaultKey("app.name");
    appConfig.setDefaultValue("unknown_app");
    tagMapping.put("app", appConfig);

    GraphConfig.TagConfig namespaceConfig = new GraphConfig.TagConfig();
    namespaceConfig.setDefaultKey("namespace.name");
    namespaceConfig.setDefaultValue("unknown_namespace");
    tagMapping.put("namespace", namespaceConfig);

    config.setNodeMetadataTagMapping(tagMapping);

    return config;
  }

  private GraphConfig createTestConfigWithRules() {
    GraphConfig config = createTestConfig();

    GraphConfig.RuleConfig rule1 = new GraphConfig.RuleConfig();
    rule1.setField("namespace.name");
    rule1.setValue("prod-ns");
    rule1.setOverrideKey("prod.app.name");

    GraphConfig.RuleConfig rule2 = new GraphConfig.RuleConfig();
    rule2.setField("cluster.name");
    rule2.setValue("east");
    rule2.setOverrideKey("east.app.name");

    config.getNodeMetadataTagMapping().get("app").setRules(List.of(rule1, rule2));

    return config;
  }
}
