package com.slack.astra.metadata.partition;

import java.util.List;
import java.util.Objects;

/**
 * PartitionMetadata Object is used to track the provisionedCapacity of a kafka partition in
 * Zookeeper. dedicatedPartition field is set when this partition is dedicated to a single tenant.
 * If false, this partition is shared by a multiple tenant
 */
public class CalculatedPartitionMetadata {
  public final String partitionId;
  public final long provisionedCapacity;
  public final boolean dedicatedPartition;
  public final long maxCapacity;
  public final List<String> datasets;

  public CalculatedPartitionMetadata(
      String partition, long provisionedCapacity, long maxCapacity, boolean dedicatedPartition) {
    this(partition, provisionedCapacity, maxCapacity, dedicatedPartition, List.of());
  }

  public CalculatedPartitionMetadata(
      String partition,
      long provisionedCapacity,
      long maxCapacity,
      boolean dedicatedPartition,
      List<String> datasets) {
    this.partitionId = partition;
    this.dedicatedPartition = dedicatedPartition;
    this.provisionedCapacity = provisionedCapacity;
    this.maxCapacity = maxCapacity;
    this.datasets = datasets;
  }

  public String getPartitionID() {
    return this.partitionId;
  }

  public long getProvisionedCapacity() {
    return this.provisionedCapacity;
  }

  public boolean getDedicatedPartition() {
    return this.dedicatedPartition;
  }

  public long getMaxCapacity() {
    return this.maxCapacity;
  }

  public long getAvailableCapacity() {
    return maxCapacity - provisionedCapacity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CalculatedPartitionMetadata that = (CalculatedPartitionMetadata) o;
    return provisionedCapacity == that.provisionedCapacity
        && dedicatedPartition == that.dedicatedPartition
        && maxCapacity == that.maxCapacity
        && Objects.equals(partitionId, that.partitionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionId, provisionedCapacity, dedicatedPartition, maxCapacity);
  }

  @Override
  public String toString() {
    return "CalculatedPartitionMetadata{"
        + ", partitionId='"
        + partitionId
        + ", provisionedCapacity="
        + provisionedCapacity
        + ", maxCapacity="
        + maxCapacity
        + ", dedicated partition="
        + dedicatedPartition
        + '}';
  }
}
