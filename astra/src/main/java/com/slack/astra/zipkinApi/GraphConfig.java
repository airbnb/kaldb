package com.slack.astra.zipkinApi;

import com.fasterxml.jackson.databind.ObjectMapper;
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

  public static class TagConfig {
    private String defaultKey;
    private String defaultValue;

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
  }

  public static class RuleConfig {
    public static class MatchConfig {
      private String field;
      private String value;

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
    }

    private MatchConfig match;
    private String overrideKey;

    public MatchConfig getMatch() {
      return match;
    }

    public void setMatch(MatchConfig match) {
      this.match = match;
    }

    public String getOverrideKey() {
      return overrideKey;
    }

    public void setOverrideKey(String overrideKey) {
      this.overrideKey = overrideKey;
    }
  }

  private Map<String, TagConfig> nodeMetadataTagMapping;
  private Map<String, List<RuleConfig>> rules;

  public Map<String, TagConfig> getNodeMetadataTagMapping() {
    return nodeMetadataTagMapping;
  }

  public void setNodeMetadataTagMapping(Map<String, TagConfig> nodeMetadataTagMapping) {
    this.nodeMetadataTagMapping = nodeMetadataTagMapping;
  }

  public Map<String, List<RuleConfig>> getRules() {
    return rules;
  }

  public void setRules(Map<String, List<RuleConfig>> rules) {
    this.rules = rules;
  }

  public static GraphConfig load(String configFile) throws IOException {
    if (!configFile.isEmpty()) {
      try {
        Path path = Path.of(configFile);
        String yaml = Files.readString(path);

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(yaml, GraphConfig.class);
      } catch (Exception e) {
        LOG.warn("Failed to read or parse dependency graph config file. Returning null config", e);
        return null;
      }
    }

    return null;
  }

  public String resolve(Map<String, String> tags, String logicalField) {
    TagConfig baseCfg = nodeMetadataTagMapping.get(logicalField);
    if (baseCfg == null) {
      return tags.getOrDefault(logicalField, "unknown_" + logicalField);
    }

    String keyToUse = baseCfg.getDefaultKey();
    String defaultValue = baseCfg.getDefaultValue();

    if (rules != null) {
      List<RuleConfig> relevantRules = rules.getOrDefault(logicalField, Collections.emptyList());

      for (RuleConfig rule : relevantRules) {
        RuleConfig.MatchConfig match = rule.getMatch();
        if (match != null) {
          TagConfig matchCfg = nodeMetadataTagMapping.get(match.getField());

          if (matchCfg != null) {
            String matchVal = tags.getOrDefault(matchCfg.getDefaultKey(), matchCfg.getDefaultValue());
            if (match.getValue().equals(matchVal)) {
              keyToUse = rule.getOverrideKey();
              break; // only a single rule for a logicalField <> match field should be defined
            }
          }
        }
      }
    }

    return tags.getOrDefault(keyToUse, defaultValue);
  }
}
