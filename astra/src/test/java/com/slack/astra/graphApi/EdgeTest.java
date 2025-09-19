package com.slack.astra.graphApi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;

public class EdgeTest {

  @Test
  void builder_parentNull_throwsNullPointerException() {
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> new Edge.Builder().parent(null))
        .withMessage("parent == null");
  }

  @Test
  void builder_childNull_throwsNullPointerException() {
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> new Edge.Builder().child(null))
        .withMessage("child == null");
  }

  @Test
  void build_missingParent_throwsIllegalStateException() {
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> new Edge.Builder().child("child").build())
        .withMessage("Missing : parent");
  }

  @Test
  void build_missingChild_throwsIllegalStateException() {
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> new Edge.Builder().parent("parent").build())
        .withMessage("Missing : child");
  }

  @Test
  void build_withValidParentAndChild_succeeds() {
    Edge link = new Edge.Builder().parent("parent").child("child").build();

    assertThat(link.parent).isEqualTo("parent");
    assertThat(link.child).isEqualTo("child");
  }

  @Test
  void builderConstructor_copiesFromSourceDependencyLink() {
    Edge original = new Edge.Builder().parent("parent").child("child").build();
    Edge copy = new Edge.Builder(original).build();

    assertThat(copy.parent).isEqualTo("parent");
    assertThat(copy.child).isEqualTo("child");
    assertThat(copy).isEqualTo(original);
  }

  @Test
  void equals_sameObject_returnsTrue() {
    Edge link = new Edge.Builder().parent("parent").child("child").build();
    assertThat(link.equals(link)).isTrue();
  }

  @Test
  void equals_differentType_returnsFalse() {
    Edge link = new Edge.Builder().parent("parent").child("child").build();
    assertThat(link.equals(123)).isFalse();
  }

  @Test
  void equals_sameParentAndChild_returnsTrue() {
    Edge link1 = new Edge.Builder().parent("parent").child("child").build();
    Edge link2 = new Edge.Builder().parent("parent").child("child").build();

    assertThat(link1).isEqualTo(link2);
  }

  @Test
  void equals_differentParent_returnsFalse() {
    Edge link1 = new Edge.Builder().parent("parent1").child("child").build();
    Edge link2 = new Edge.Builder().parent("parent2").child("child").build();

    assertThat(link1).isNotEqualTo(link2);
  }

  @Test
  void equals_differentChild_returnsFalse() {
    Edge link1 = new Edge.Builder().parent("parent").child("child1").build();
    Edge link2 = new Edge.Builder().parent("parent").child("child2").build();

    assertThat(link1).isNotEqualTo(link2);
  }

  @Test
  void builder_overwriteParent_usesLatestValue() {
    Edge link = new Edge.Builder().parent("parent1").parent("parent2").child("child").build();

    assertThat(link.parent).isEqualTo("parent2");
  }

  @Test
  void builder_overwriteChild_usesLatestValue() {
    Edge link = new Edge.Builder().parent("parent").child("child1").child("child2").build();

    assertThat(link.child).isEqualTo("child2");
  }
}
