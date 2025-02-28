package com.slack.astra.metadata.partition;

import com.slack.astra.metadata.core.AstraMetadata;
import java.util.Objects;

public class PartitionMetadata extends AstraMetadata {
  public final String partition_id;
  public long utilization;
  public boolean isPartitionShared;

  public PartitionMetadata(String partition) {
    super(String.format("partition_%s", partition));
    this.partition_id = partition;
    this.utilization = 0;
    this.isPartitionShared = false;
  }

  public PartitionMetadata getPartition() {
    return this;
  }

  public String getPartitionID() {
    return this.partition_id;
  }

  public long getUtilization() {
    return this.utilization;
  }

  public boolean getIsPartitionShared() {
    return this.isPartitionShared;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PartitionMetadata)) return false;
    if (!super.equals(o)) return false;
    PartitionMetadata that = (PartitionMetadata) o;
    return utilization == that.utilization
        && name.equals(that.name)
        && partition_id.equals(that.partition_id)
        && isPartitionShared == that.isPartitionShared;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), name, partition_id, utilization, isPartitionShared);
  }

  @Override
  public String toString() {
    return "PartitionMetadata{"
        + "name='"
        + name
        + ", partition_id='"
        + partition_id
        + ", utilization="
        + utilization
        + ", partition shared="
        + isPartitionShared
        + '}';
  }
}
