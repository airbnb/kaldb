package com.slack.astra.graphApi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;

public class EdgeTest {

  @Test
  void builder_sourceNull_throwsNullPointerException() {
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> new Edge(null, "target"))
        .withMessage("sourceNodeId == null");
  }

  @Test
  void builder_targetNull_throwsNullPointerException() {
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> new Edge("source", null))
        .withMessage("targetNodeId == null");
  }

  @Test
  void build_withValidSourceAndTarget_succeeds() {
    Edge e = new Edge("source", "target");

    assertThat(e.getSourceNodeId()).isEqualTo("source");
    assertThat(e.getTargetNodeId()).isEqualTo("target");
  }

  @Test
  void equals_differentType_returnsFalse() {
    Edge e = new Edge("source", "target");
    assertThat(e.equals(123)).isFalse();
  }

  @Test
  void equals_sameSourceAndChild_returnsTrue() {
    Edge e1 = new Edge("source", "target");
    Edge e2 = new Edge("source", "target");

    assertThat(e1).isEqualTo(e2);
  }

  @Test
  void equals_differentSource_returnsFalse() {
    Edge e1 = new Edge("source1", "target");
    Edge e2 = new Edge("source2", "target");

    assertThat(e1).isNotEqualTo(e2);
  }

  @Test
  void equals_differentChild_returnsFalse() {
    Edge e1 = new Edge("source", "target1");
    Edge e2 = new Edge("source", "target2");

    assertThat(e1).isNotEqualTo(e2);
  }
}
