package com.slack.astra.graphApi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GraphConfig {
  private static final Logger LOG = LoggerFactory.getLogger(GraphConfig.class);

  public static final GraphConfig DEFAULT = new GraphConfig(Map.of());

  /**
   * Represents how a single logical field on a node should be mapped to span tags. Each field has:
   * - a default key to look up in tags - a default fallback value if the key isn’t found - an
   * optional list of rules that can override the default key Example in YAML: resource:
   * default_key: resource default_value: unknown_resource rules: - field: operation_name value:
   * http.request override_key: tag.operation.canonical_path In the above example, the default span
   * tag for populating a node's resource field is "resource", however if a span's "operation_name"
   * == "http.request", then use the "tag.operation.canonical_path" override key to instead populate
   * a node's resource field.
   */
  public static final class TagConfig {
    private final String defaultKey;
    private final String defaultValue;
    private final List<RuleConfig> rules;

    @JsonCreator
    public TagConfig(
        @JsonProperty("default_key") String defaultKey,
        @JsonProperty("default_value") String defaultValue,
        @JsonProperty("rules") List<RuleConfig> rules) {
      this.defaultKey = defaultKey;
      this.defaultValue = defaultValue;
      this.rules = (rules == null) ? Collections.emptyList() : List.copyOf(rules);
    }

    public String getDefaultKey() {
      return defaultKey;
    }

    public String getDefaultValue() {
      return defaultValue;
    }

    public List<RuleConfig> getRules() {
      return rules;
    }
  }

  /**
   * Represents a conditional rule for overriding which tag key to use. Note: This logic does not
   * currently support multiple field matches under a single rule.
   */
  public static class RuleConfig {
    private final String field;
    private final String value;
    private final String overrideKey;

    @JsonCreator
    public RuleConfig(
        @JsonProperty("field") String field,
        @JsonProperty("value") String value,
        @JsonProperty("override_key") String overrideKey) {
      this.field = field;
      this.value = value;
      this.overrideKey = overrideKey;
    }

    public String getField() {
      return field;
    }

    public String getValue() {
      return value;
    }

    public String getOverrideKey() {
      return overrideKey;
    }
  }

  // Holds the entire mapping for logical field names to their configuration of defaults and rules.
  private final Map<String, TagConfig> nodeMetadataTagMapping;

  @JsonCreator
  public GraphConfig(
      @JsonProperty("node_metadata_tag_mapping") Map<String, TagConfig> nodeMetadataTagMapping) {
    this.nodeMetadataTagMapping = Map.copyOf(nodeMetadataTagMapping);
  }

  public Map<String, TagConfig> getNodeMetadataTagMapping() {
    return nodeMetadataTagMapping;
  }

  // Loads a GraphConfig from a YAML file on disk. On failure, log and return a null config.
  public static GraphConfig load(Path filePath) throws IOException {
    try {
      String yaml = Files.readString(filePath);
      return load(yaml);
    } catch (Exception e) {
      LOG.warn(
          "Failed to read dependency graph config from file path. Returning default config", e);
    }

    return DEFAULT;
  }

  public static GraphConfig load(String configYAML) throws IOException {
    if (!configYAML.isEmpty()) {
      try {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        return mapper.readValue(configYAML, GraphConfig.class);
      } catch (Exception e) {
        LOG.warn(
            "Failed to parse dependency graph config file contents. Returning default config", e);
      }
    }

    return DEFAULT;
  }

  /**
   * Resolves the actual tag value for a given logical field, using the provided span tags. Steps:
   * 1. Look up the TagConfig for this logical field (e.g. "resource"). 2. Default to using its
   * defaultKey + defaultValue. 3. If rules are defined: - Iterate through each rule in reverse
   * order. - If a rule’s field/value condition matches, switch keyToUse to overrideKey. 4. Finally,
   * look up the chosen key in tags. If missing, fall back to defaultValue. Note: This logic does
   * not currently support multiple field matches for a single rule.
   */
  public String resolve(Map<String, String> tags, String logicalField) {
    TagConfig baseCfg = nodeMetadataTagMapping.get(logicalField);

    // If the config doesn't define this logical field, just return from raw tags or fallback.
    if (baseCfg == null) {
      return tags.getOrDefault(logicalField, "unknown_" + logicalField);
    }

    // Later rules override earlier ones, so start from the back of the list and use the first one
    // that matches.
    String keyToUse =
        baseCfg.getRules().reversed().stream()
            .filter(
                rule ->
                    rule.getValue()
                        .equals(tags.getOrDefault(rule.getField(), "unknown_" + rule.getField())))
            .map(RuleConfig::getOverrideKey)
            .filter(tags::containsKey)
            .findFirst()
            .orElse(baseCfg.getDefaultKey());

    return tags.getOrDefault(keyToUse, baseCfg.getDefaultValue());
  }

    @Override
    public String toString() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
        } catch (Exception e) {
            return "GraphConfig{error serializing to string}";
        }
    }
}