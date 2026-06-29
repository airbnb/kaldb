package com.slack.astra.graphApi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.slack.astra.zipkinApi.ZipkinSpanResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GraphConfig {
  private static final Logger LOG = LoggerFactory.getLogger(GraphConfig.class);

  public static final GraphConfig DEFAULT = new GraphConfig(Map.of(), Map.of());

  public enum EntityType {
    NODE,
    EDGE
  }

  /**
   * Represents how a single logical field on a node should be mapped to span tags.
   *
   * <p>Each field has: - default key (can be a list) to look up in tags (combined with delimiter if
   * multiple) - a default fallback value if the keys aren't found - an optional delimiter for
   * combining multiple key values - an optional list of rules that can override the default key
   */
  public static final class TagConfig {
    private final List<String> defaultKey;
    private final String defaultValue;
    private final String keyDelimiter;
    private final List<RuleConfig> rules;
    private final boolean useDefaultKeyOnEmptyOverride;
    private final boolean isAnnotation;

    @JsonCreator
    public TagConfig(
        @JsonProperty("default_key") List<String> defaultKey,
        @JsonProperty("default_value") String defaultValue,
        @JsonProperty("key_delimiter") String keyDelimiter,
        @JsonProperty("rules") List<RuleConfig> rules,
        @JsonProperty("use_default_key_on_empty_override") Boolean useDefaultKeyOnEmptyOverride,
        @JsonProperty("is_annotation") Boolean isAnnotation) {
      this.defaultKey = (defaultKey == null) ? Collections.emptyList() : List.copyOf(defaultKey);
      this.defaultValue = defaultValue;
      // Set default keyDelimiter to "." if null or empty
      this.keyDelimiter = (keyDelimiter == null || keyDelimiter.isEmpty()) ? "." : keyDelimiter;
      this.rules = (rules == null) ? Collections.emptyList() : List.copyOf(rules);
      this.useDefaultKeyOnEmptyOverride =
          (useDefaultKeyOnEmptyOverride == null) ? false : useDefaultKeyOnEmptyOverride;
      this.isAnnotation = (isAnnotation == null) ? false : isAnnotation;
    }

    public List<String> getDefaultKey() {
      return defaultKey;
    }

    public String getDefaultValue() {
      return defaultValue;
    }

    public String getKeyDelimiter() {
      return keyDelimiter;
    }

    public List<RuleConfig> getRules() {
      return rules;
    }

    public boolean isUseDefaultKeyOnEmptyOverride() {
      return useDefaultKeyOnEmptyOverride;
    }

    public boolean isAnnotation() {
      return isAnnotation;
    }
  }

  /**
   * Represents a conditional rule for overriding which tag key to use. Note: This logic does not
   * currently support multiple field matches under a single rule.
   */
  public static class RuleConfig {
    private final String field;
    private final String value;
    private final List<String> overrideKey;

    @JsonCreator
    public RuleConfig(
        @JsonProperty("field") String field,
        @JsonProperty("value") String value,
        @JsonProperty("override_key") List<String> overrideKey) {
      this.field = field;
      this.value = value;
      this.overrideKey = (overrideKey == null) ? Collections.emptyList() : List.copyOf(overrideKey);
    }

    public String getField() {
      return field;
    }

    public String getValue() {
      return value;
    }

    public List<String> getOverrideKey() {
      return overrideKey;
    }
  }

  // Holds the entire mapping for logical field names to their configuration of defaults and rules
  // for nodes.
  private final Map<String, TagConfig> nodeMetadataTagMapping;
  // Holds the entire mapping for logical field names to their configuration of defaults and rules
  // for nodes.
  private final Map<String, TagConfig> edgeMetadataTagMapping;

  @JsonCreator
  public GraphConfig(
      @JsonProperty("node_metadata_tag_mapping") Map<String, TagConfig> nodeMetadataTagMapping,
      @JsonProperty("edge_metadata_tag_mapping") Map<String, TagConfig> edgeMetadataTagMapping) {
    this.nodeMetadataTagMapping =
        (nodeMetadataTagMapping == null)
            ? Collections.emptyMap()
            : Map.copyOf(nodeMetadataTagMapping);
    this.edgeMetadataTagMapping =
        (edgeMetadataTagMapping == null)
            ? Collections.emptyMap()
            : Map.copyOf(edgeMetadataTagMapping);
  }

  public Map<String, TagConfig> getNodeMetadataTagMapping() {
    return nodeMetadataTagMapping;
  }

  public Map<String, TagConfig> getEdgeMetadataTagMapping() {
    return edgeMetadataTagMapping;
  }

  /**
   * Loads a GraphConfig from a YAML file on disk. On failure, log and return a null config.
   *
   * @param filePath Path of the config to load
   * @return GraphConfig containing mappings of node metadata fields -> corresponding span tags.
   */
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

  /**
   * Loads a GraphConfig from a YAML string. On failure, log and return a null config.
   *
   * @param configYAML YAML string of config contents.
   * @return GraphConfig containing mappings of node metadata fields -> corresponding span tags.
   */
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
   * Creates metadata for a graph entity (node or edge) from a span, using either default behavior
   * or configured mapping.
   *
   * @param span ZipkinSpanResponse containing the span data
   * @return SortedMap containing the metadata for the requested entity
   */
  public SortedMap<String, String> createMetadataFromSpan(
      ZipkinSpanResponse span, EntityType entityType) {
    SortedMap<String, String> metadata = new TreeMap<>();

    if (this == DEFAULT) {
      // We only care about default behavior for a node, edge metadata is optional.
      if (entityType == EntityType.NODE) {
        // use service name from remote endpoint
        metadata.put("service", span.getRemoteEndpoint().getServiceName());
      }
    } else {

      // Use configured tag mapping
      Set<String> keys =
          switch (entityType) {
            case EDGE -> this.edgeMetadataTagMapping.keySet();
            case NODE -> this.nodeMetadataTagMapping.keySet();
          };

      for (String key : keys) {
        // Annotation fields are resolved separately via createAnnotationsFromSpan and must not
        // influence edge identity.
        if (entityType == EntityType.EDGE && this.edgeMetadataTagMapping.get(key).isAnnotation()) {
          continue;
        }
        metadata.put(key, resolve(span, key, entityType));
      }
    }

    return metadata;
  }

  /**
   * Resolves the value for a given logical field from a span.
   *
   * <p>Steps: 1. Look up the TagConfig for this logical field (e.g. "resource"). 2. Check if any
   * rules match: - Iterate through each rule in reverse order. - If a rule's field/value condition
   * matches a span tag, try to resolve using the overrideKey. - If the resolved value is non-empty,
   * use it. - Otherwise, continue to the next matching rule. 3. If no rule produces a non-empty
   * value, use the defaultKey: - Reserved keys (e.g. "service_name", "name") are resolved against
   * top-level span fields; all other keys are looked up in span tags. - Combine the values with the
   * delimiter if multiple keys are present. - If any key is missing or empty, fall back to
   * defaultValue.
   *
   * <p>Note: This logic does not currently support multiple field matches for a single rule.
   *
   * @param span ZipkinSpanResponse containing the span data.
   * @param logicalField the node metadata field to resolve.
   * @return String the value of the logical metadata field after applying all GraphConfig rules.
   */
  public String resolve(ZipkinSpanResponse span, String logicalField, EntityType entityType) {
    Map<String, String> tags = span.getTags();

    TagConfig baseCfg =
        switch (entityType) {
          case EDGE -> this.edgeMetadataTagMapping.get(logicalField);
          case NODE -> this.nodeMetadataTagMapping.get(logicalField);
        };

    // If the config doesn't define this logical field, just return from raw tags or fallback.
    if (baseCfg == null) {
      return tags.getOrDefault(logicalField, "unknown_" + logicalField);
    }

    // Later rules override earlier ones, so start from the back of the list.
    // Try each matching rule until one produces a non-empty value.
    boolean anyRuleMatched = false;
    for (RuleConfig rule : baseCfg.getRules().reversed()) {
      if (rule.getValue()
          .equals(tags.getOrDefault(rule.getField(), "unknown_" + rule.getField()))) {
        anyRuleMatched = true;
        String resolved = resolveKeys(span, rule.getOverrideKey(), baseCfg.getKeyDelimiter());
        if (resolved != null && !resolved.isEmpty()) {
          return resolved;
        }
        // Continue to next matching rule if value is empty
      }
    }

    // All rules exhausted without a value.
    // Return the default_value if a rule matched and we don't want to fall back to the default_key.
    if (anyRuleMatched && !baseCfg.isUseDefaultKeyOnEmptyOverride()) {
      return baseCfg.getDefaultValue();
    }

    // Try the default_key if no rule matched or we want to fall back to the default_key
    // in case rules produced empty values.
    String defaultResolved = resolveKeys(span, baseCfg.getDefaultKey(), baseCfg.getKeyDelimiter());
    if (defaultResolved != null && !defaultResolved.isEmpty()) {
      return defaultResolved;
    }

    return baseCfg.getDefaultValue();
  }

  /**
   * Helper method to resolve a list of keys from the span. Reserved keys ("service_name", "name")
   * are resolved against top-level span fields; all other keys are resolved against span tags.
   *
   * @param span ZipkinSpanResponse containing the span data.
   * @param keys List of keys to look up.
   * @param delimiter Delimiter to use when combining multiple key values.
   * @return The resolved value, or null if any key is missing.
   */
  private String resolveKeys(ZipkinSpanResponse span, List<String> keys, String delimiter) {
    if (keys == null || keys.isEmpty()) {
      return null;
    }

    // Collect values for all parts in a key
    List<String> values = new java.util.ArrayList<>();
    for (String keyPart : keys) {
      String value =
          switch (keyPart) {
            case "service_name" ->
                span.getRemoteEndpoint() != null ? span.getRemoteEndpoint().getServiceName() : null;
            case "name" -> span.getName();
            default -> span.getTags().get(keyPart);
          };

      if (value == null) {
        // If any key is missing, return null
        return null;
      }
      values.add(value);
    }

    // Combine values with delimiter if present and multiple keys exist
    if (values.size() > 1 && delimiter != null) {
      return String.join(delimiter, values);
    }
    return values.get(0);
  }

  /**
   * Creates annotations for an edge from a span's own tags. Annotation fields (those marked {@code
   * is_annotation: true} in config) are resolved using the same key-lookup machinery as regular
   * metadata fields but are kept separate from identity-determining metadata.
   *
   * @param span The span to resolve annotations from.
   * @return SortedMap of annotation field name to resolved value. Fields not present on this span
   *     are absent from the map.
   */
  public SortedMap<String, String> createAnnotationsFromSpan(ZipkinSpanResponse span) {
    SortedMap<String, String> result = new TreeMap<>();
    for (String key : edgeMetadataTagMapping.keySet()) {
      if (!edgeMetadataTagMapping.get(key).isAnnotation()) continue;
      String value = resolve(span, key, EntityType.EDGE);
      if (value != null && !value.isEmpty()) {
        result.put(key, value);
      }
    }
    return result;
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
