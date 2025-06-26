package com.slack.astra.metadata.partition;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class PartitionMetadataTest {

  @Test
  public void testPartitionMetadata() {
    final String partitionId = "1";
    final long maxCapacity = 5000000;
    PartitionMetadata partitionMetadata = new PartitionMetadata(partitionId, maxCapacity);

    assertThat(partitionMetadata.name).isEqualTo(partitionId);
    assertThat(partitionMetadata.partitionId).isEqualTo(partitionId);
    assertThat(partitionMetadata.maxCapacity).isEqualTo(maxCapacity);
  }

  @Test
  public void testEqualsAndHashCode() {
    PartitionMetadata partitionMetadata1 = new PartitionMetadata("1", 5000000);
    PartitionMetadata partitionMetadata2 = new PartitionMetadata("2", 5000000);

    assertThat(partitionMetadata1).isNotEqualTo(partitionMetadata2);

    Set<PartitionMetadata> set = new HashSet<>();
    set.add(partitionMetadata1);
    set.add(partitionMetadata2);
    assertThat(set.size()).isEqualTo(2);
    assertThat(set).containsOnly(partitionMetadata1, partitionMetadata2);
  }
}
