package com.slack.astra.chunkrollover;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class NeverRolloverChunkStrategyTest {
  @Test
  public void testShouldRolloverIsAlwaysFalse() {
    assertThat(new NeverRolloverChunkStrategy().shouldRollOver(Long.MAX_VALUE, Long.MAX_VALUE))
        .isFalse();
  }
}
