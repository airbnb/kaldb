package com.slack.astra.zipkinApi;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.slack.astra.graphApi.GraphConfig;
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
  public void testMatchConfigGettersAndSetters() {
    GraphConfig.RuleConfig.MatchConfig matchConfig = new GraphConfig.RuleConfig.MatchConfig();

    matchConfig.setField("testField");
    matchConfig.setValue("testValue");

    assertThat(matchConfig.getField()).isEqualTo("testField");
    assertThat(matchConfig.getValue()).isEqualTo("testValue");
  }

  @Test
  public void testRuleConfigGettersAndSetters() {
    GraphConfig.RuleConfig ruleConfig = new GraphConfig.RuleConfig();
    GraphConfig.RuleConfig.MatchConfig matchConfig = new GraphConfig.RuleConfig.MatchConfig();

    ruleConfig.setMatch(matchConfig);
    ruleConfig.setOverrideKey("overrideKey");

    assertThat(ruleConfig.getMatch()).isEqualTo(matchConfig);
    assertThat(ruleConfig.getOverrideKey()).isEqualTo("overrideKey");
  }

  @Test
  public void testGraphConfigGettersAndSetters() {
    GraphConfig graphConfig = new GraphConfig();
    Map<String, GraphConfig.TagConfig> nodeMetadataTagMapping = new HashMap<>();
    Map<String, List<GraphConfig.RuleConfig>> rules = new HashMap<>();

    graphConfig.setNodeMetadataTagMapping(nodeMetadataTagMapping);

    assertThat(graphConfig.getNodeMetadataTagMapping()).isEqualTo(nodeMetadataTagMapping);
    assertThat(graphConfig.getRules()).isNull();
  }

  @Test
  public void testLoadValidYamlConfig(@TempDir Path tempDir) throws IOException {
    String yamlContent =
        """
        nodeMetadataTagMapping:
          service:
            defaultKey: service.name
            defaultValue: unknown_service
          cluster:
            defaultKey: cluster.name
            defaultValue: unknown_cluster
        rules:
          service:
            - match:
                field: cluster
                value: prod
              overrideKey: prod.service.name
        """;

    Path configFile = tempDir.resolve("test-config.yaml");
    Files.writeString(configFile, yamlContent);

    GraphConfig config = GraphConfig.load(configFile.toString());

    assertThat(config).isNotNull();
    assertThat(config.getNodeMetadataTagMapping()).hasSize(2);

    GraphConfig.TagConfig serviceConfig = config.getNodeMetadataTagMapping().get("service");
    assertThat(serviceConfig.getDefaultKey()).isEqualTo("service.name");
    assertThat(serviceConfig.getDefaultValue()).isEqualTo("unknown_service");

    GraphConfig.TagConfig clusterConfig = config.getNodeMetadataTagMapping().get("cluster");
    assertThat(clusterConfig.getDefaultKey()).isEqualTo("cluster.name");
    assertThat(clusterConfig.getDefaultValue()).isEqualTo("unknown_cluster");

    assertThat(config.getRules()).hasSize(1);
    List<GraphConfig.RuleConfig> serviceRules = config.getRules().get("service");
    assertThat(serviceRules).hasSize(1);

    GraphConfig.RuleConfig rule = serviceRules.get(0);
    assertThat(rule.getOverrideKey()).isEqualTo("prod.service.name");
    assertThat(rule.getMatch().getField()).isEqualTo("cluster");
    assertThat(rule.getMatch().getValue()).isEqualTo("prod");
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
    Map<String, String> tags = Map.of("namespace.name", "prod-ns");

    String result = config.resolve(tags, "app");
    assertThat(result).isEqualTo("unknown_app");
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
  public void testResolveWithNullMatch() {
    GraphConfig config = new GraphConfig();

    Map<String, GraphConfig.TagConfig> tagMapping = new HashMap<>();
    GraphConfig.TagConfig serviceConfig = new GraphConfig.TagConfig();
    serviceConfig.setDefaultKey("app.name");
    serviceConfig.setDefaultValue("unknown_app");
    tagMapping.put("app", serviceConfig);
    config.setNodeMetadataTagMapping(tagMapping);

    Map<String, List<GraphConfig.RuleConfig>> rules = new HashMap<>();
    GraphConfig.RuleConfig rule = new GraphConfig.RuleConfig();
    rule.setOverrideKey("prod.app.name");
    rule.setMatch(null);
    rules.put("app", List.of(rule));
    config.setRules(rules);

    Map<String, String> tags = Map.of("app.name", "my-app");

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

    Map<String, List<GraphConfig.RuleConfig>> rules = new HashMap<>();

    GraphConfig.RuleConfig.MatchConfig matchConfig = new GraphConfig.RuleConfig.MatchConfig();
    matchConfig.setField("namespace");
    matchConfig.setValue("prod-ns");

    GraphConfig.RuleConfig rule = new GraphConfig.RuleConfig();
    rule.setMatch(matchConfig);
    rule.setOverrideKey("prod.app.name");

    rules.put("app", List.of(rule));
    config.setRules(rules);

    return config;
  }
}
