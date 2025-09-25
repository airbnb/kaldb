package com.slack.astra.graphApi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;

public class EdgeTest {

  @Test
  void builder_sourceNull_throwsNullPointerException() {
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> new Edge(null, "target"))
        .withMessage("sourceNodeId cannot be null");
  }

  @Test
  void builder_targetNull_throwsNullPointerException() {
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> new Edge("source", null))
        .withMessage("targetNodeId cannot be null");
  }

  @Test
  void build_withValidSourceAndTarget_succeeds() {
    Edge e = new Edge("source", "target");

    assertThat(e.sourceNodeId()).isEqualTo("source");
    assertThat(e.targetNodeId()).isEqualTo("target");
  }
}
