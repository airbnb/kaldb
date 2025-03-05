package com.slack.astra.metadata.partition;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class PartitionMetadataTest {

  @Test
  public void testPartitionMetadata() {
    final String partitionId = "1";
    final long utilizaion = 1000000;
    final boolean isPartitionShared = false;
    PartitionMetadata partitionMetadata =
        new PartitionMetadata(partitionId, utilizaion, isPartitionShared);

    assertThat(partitionMetadata.name).isEqualTo(partitionId);
    assertThat(partitionMetadata.partitionId).isEqualTo(partitionId);
    assertThat(partitionMetadata.utilization).isEqualTo(utilizaion);
    assertThat(partitionMetadata.isPartitionShared).isEqualTo(isPartitionShared);
  }

  @Test
  public void testEqualsAndHashCode() {
    PartitionMetadata partitionMetadata1 = new PartitionMetadata("1", 1000000, false);
    PartitionMetadata partitionMetadata2 = new PartitionMetadata("2", 1000000, false);
    PartitionMetadata partitionMetadata3 = new PartitionMetadata("1", 2000000, false);
    PartitionMetadata partitionMetadata4 = new PartitionMetadata("1", 1000000, true);

    assertThat(partitionMetadata1).isNotEqualTo(partitionMetadata2);
    assertThat(partitionMetadata1).isNotEqualTo(partitionMetadata3);
    assertThat(partitionMetadata1).isNotEqualTo(partitionMetadata4);

    Set<PartitionMetadata> set = new HashSet<>();
    set.add(partitionMetadata1);
    set.add(partitionMetadata2);
    set.add(partitionMetadata3);
    set.add(partitionMetadata4);
    assertThat(set.size()).isEqualTo(4);
    assertThat(set)
        .containsOnly(
            partitionMetadata1, partitionMetadata2, partitionMetadata3, partitionMetadata4);
  }
}
