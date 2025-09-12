package com.slack.astra.graphApi;

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

public class GraphConfig {
  private static final Logger LOG = LoggerFactory.getLogger(GraphConfig.class);

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
  public static class TagConfig {
    private String defaultKey;
    private String defaultValue;
    private List<RuleConfig> rules = Collections.emptyList();

    public String getDefaultKey() {
      return defaultKey;
    }

    public void setDefaultKey(String defaultKey) {
      this.defaultKey = defaultKey;
    }

    public String getDefaultValue() {
      return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
      this.defaultValue = defaultValue;
    }

    public List<RuleConfig> getRules() {
      return rules;
    }

    public void setRules(List<RuleConfig> rules) {
      this.rules = rules;
    }
  }

  /**
   * Represents a conditional rule for overriding which tag key to use. Note: This logic does not
   * currently support multiple field matches under a single rule.
   */
  public static class RuleConfig {
    private String field;
    private String value;
    private String overrideKey;

    public String getField() {
      return field;
    }

    public void setField(String field) {
      this.field = field;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

    public String getOverrideKey() {
      return overrideKey;
    }

    public void setOverrideKey(String overrideKey) {
      this.overrideKey = overrideKey;
    }
  }

  // Holds the entire mapping for logical field names to their configuration of defaults and rules.
  private Map<String, TagConfig> nodeMetadataTagMapping;

  public Map<String, TagConfig> getNodeMetadataTagMapping() {
    return nodeMetadataTagMapping;
  }

  public void setNodeMetadataTagMapping(Map<String, TagConfig> nodeMetadataTagMapping) {
    this.nodeMetadataTagMapping = nodeMetadataTagMapping;
  }

  // Loads a GraphConfig from a YAML file on disk. On failure, log and return a null config.
  public static GraphConfig load(String configFile) throws IOException {
    if (!configFile.isEmpty()) {
      try {
        Path path = Path.of(configFile);
        String yaml = Files.readString(path);

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        return mapper.readValue(yaml, GraphConfig.class);
      } catch (Exception e) {
        LOG.warn("Failed to read or parse dependency graph config file. Returning null config", e);
        return null;
      }
    }

    return null;
  }

  /**
   * Resolves the actual tag value for a given logical field, using the provided span tags. Steps:
   * 1. Look up the TagConfig for this logical field (e.g. "resource"). 2. Default to using its
   * defaultKey + defaultValue. 3. If rules are defined: - Iterate through each rule in order. - If
   * a rule’s field/value condition matches, switch keyToUse to overrideKey. - Continue processing
   * later rules (last matching rule wins). 4. Finally, look up the chosen key in tags. If missing,
   * fall back to defaultValue. Note: This logic does not currently support multiple field matches
   * for a single rule.
   */
  public String resolve(Map<String, String> tags, String logicalField) {
    TagConfig baseCfg = nodeMetadataTagMapping.get(logicalField);

    // If the config doesn't define this logical field, just return from raw tags or fallback.
    if (baseCfg == null) {
      return tags.getOrDefault(logicalField, "unknown_" + logicalField);
    }

    // Start with the default key + value.
    String keyToUse = baseCfg.getDefaultKey();
    String defaultValue = baseCfg.getDefaultValue();

    // Apply rules in order. Later matching rules override earlier ones.
    for (RuleConfig rule : baseCfg.getRules()) {
      // Get the value of the field defined in the rule.
      String matchVal = tags.getOrDefault(rule.getField(), "unknown_" + rule.getField());
      // If the rule value matches, override which key to use for resolution of the logical field
      // only if it exists in tags, otherwise keep the default.
      if (rule.getValue().equals(matchVal)) {
        if (tags.containsKey(rule.getOverrideKey())) {
          keyToUse = rule.getOverrideKey();
        }
      }
    }

    return tags.getOrDefault(keyToUse, defaultValue);
  }
}
