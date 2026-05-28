package com.slack.astra.graphApi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

public class EdgeTest {

  @Test
  void builder_sourceNull_throwsNullPointerException() {
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> new Edge(null, "target", null))
        .withMessage("sourceNodeId cannot be null");
  }

  @Test
  void builder_targetNull_throwsNullPointerException() {
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> new Edge("source", null, null))
        .withMessage("targetNodeId cannot be null");
  }

  @Test
  void build_withValidSourceAndTarget_succeeds() {
    Edge e = new Edge("source", "target", null);

    assertThat(e.getSourceNodeId()).isEqualTo("source");
    assertThat(e.getTargetNodeId()).isEqualTo("target");
  }

  @Test
  void build_withValidSourceTargetAndMetadata_succeeds() {
    Edge e = new Edge("source", "target", new TreeMap<>(Map.of("operation", "default")));

    assertThat(e.getSourceNodeId()).isEqualTo("source");
    assertThat(e.getTargetNodeId()).isEqualTo("target");
    assertThat(e.getMetadata()).isNotEmpty();
  }

  @Test
  void addObservation_emptyAnnotations_incrementsCountAndLeavesAnnotationsEmpty() {
    Edge e = new Edge("source", "target", null);
    e.addObservation(Map.of());

    assertThat(e.getObservedCount()).isEqualTo(1);
    assertThat(e.getAnnotations()).isEmpty();
  }

  @Test
  void addObservation_sameFieldAndValueTwice_deduplicates() {
    Edge e = new Edge("source", "target", null);
    e.addObservation(Map.of("product_context", "CHECKOUT:CREATE_LISTING:1"));
    e.addObservation(Map.of("product_context", "CHECKOUT:CREATE_LISTING:1"));

    assertThat(e.getObservedCount()).isEqualTo(2);
    assertThat(e.getAnnotations().get("product_context")).hasSize(1);
    assertThat(e.getAnnotations().get("product_context"))
        .containsExactly("CHECKOUT:CREATE_LISTING:1");
  }

  @Test
  void addObservation_differentValuesSameField_accumulatesBoth() {
    Edge e = new Edge("source", "target", null);
    e.addObservation(Map.of("product_context", "CHECKOUT:CREATE_LISTING:1"));
    e.addObservation(Map.of("product_context", "SEARCH:SEARCH_LISTING:2"));

    assertThat(e.getObservedCount()).isEqualTo(2);
    assertThat(e.getAnnotations().get("product_context")).hasSize(2);
    assertThat(e.getAnnotations().get("product_context"))
        .containsExactlyInAnyOrder("CHECKOUT:CREATE_LISTING:1", "SEARCH:SEARCH_LISTING:2");
  }

  @Test
  void annotations_doNotAffectEqualsOrGenerateKey() {
    TreeMap<String, String> metadata = new TreeMap<>(Map.of("operation", "http.request"));
    Edge e1 = new Edge("source", "target", metadata);
    Edge e2 = new Edge("source", "target", metadata);

    e1.addObservation(Map.of("product_context", "CHECKOUT:CREATE_LISTING:1"));

    assertThat(e1).isEqualTo(e2);
    assertThat(Edge.generateKey("source", "target", metadata))
        .isEqualTo(Edge.generateKey("source", "target", metadata));
  }
}
