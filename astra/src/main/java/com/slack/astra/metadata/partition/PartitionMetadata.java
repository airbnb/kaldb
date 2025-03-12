package com.slack.astra.metadata.partition;

import com.slack.astra.metadata.core.AstraMetadata;
import java.util.Objects;

/**
 * PartitionMetadata Object to track the utilization and isPartitionShared (set to true when
 * partition is shared between Tenants) in zookeeper
 */
public class PartitionMetadata extends AstraMetadata {
  public final String partitionId;
  public long utilization;
  public boolean isPartitionShared;

  public PartitionMetadata(String partition, long utilization, boolean isPartitionShared) {
    super(partition);
    this.partitionId = partition;
    this.utilization = utilization;
    this.isPartitionShared = isPartitionShared;
  }

  public PartitionMetadata getPartition() {
    return this;
  }

  public String getPartitionID() {
    return this.partitionId;
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
        && partitionId.equals(that.partitionId)
        && isPartitionShared == that.isPartitionShared;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), partitionId, utilization, isPartitionShared);
  }

  @Override
  public String toString() {
    return "PartitionMetadata{"
        + ", partitionId='"
        + partitionId
        + ", utilization="
        + utilization
        + ", partition shared="
        + isPartitionShared
        + '}';
  }
}
