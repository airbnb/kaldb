package com.slack.astra.metadata.partition;

import com.slack.astra.metadata.core.AstraMetadata;
import java.util.Objects;

/**
 * PartitionMetadata Object is used to track the maxCapacity of a kafka partition in Zookeeper.
 * dedicatedPartition field is set when this partition is dedicated to a single tenant. If false,
 * this partition is shared by a multiple tenant
 */
public class PartitionMetadata extends AstraMetadata {
  public final String partitionId;
  public long maxCapacity;

  public PartitionMetadata(String partition, long maxCapacity) {
    super(partition);
    this.partitionId = partition;
    this.maxCapacity = maxCapacity;
  }

  public PartitionMetadata getPartition() {
    return this;
  }

  public String getPartitionID() {
    return this.partitionId;
  }

  public long getMaxCapacity() {
    return this.maxCapacity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PartitionMetadata)) return false;
    if (!super.equals(o)) return false;
    PartitionMetadata that = (PartitionMetadata) o;
    return partitionId.equals(that.partitionId) && maxCapacity == that.maxCapacity;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), partitionId, maxCapacity);
  }

  @Override
  public String toString() {
    return "PartitionMetadata{"
        + ", partitionId='"
        + partitionId
        + ", maxCapacity="
        + maxCapacity
        + '}';
  }
}
