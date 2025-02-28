package com.slack.astra.metadata.partition;

import com.slack.astra.metadata.core.AstraMetadata;
import java.util.Objects;

public class PartitionMetadata extends AstraMetadata {
  public final String partition_id;
  public final long utilization;
  public final boolean isPartitionShared;

  public PartitionMetadata(String partition, long utilization, boolean isPartitionShared) {
    super(partition);
    this.partition_id = partition;
    this.utilization = utilization;
    this.isPartitionShared = isPartitionShared;
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
        && partition_id.equals(that.partition_id)
        && isPartitionShared == that.isPartitionShared;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), partition_id, utilization, isPartitionShared);
  }

  @Override
  public String toString() {
    return "PartitionMetadata{"
        + ", partition_id='"
        + partition_id
        + ", utilization="
        + utilization
        + ", partition shared="
        + isPartitionShared
        + '}';
  }
}
