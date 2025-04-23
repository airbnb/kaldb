package com.slack.astra.metadata.partition;

import com.slack.astra.metadata.core.AstraMetadataStore;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class PartitionMetadataStore extends AstraMetadataStore<PartitionMetadata> {
  public static final String PARTITION_MAP_METADATA_STORE_ZK_PATH = "/partition_map";

  public final long maxPartitionCapacity;
  public final int minNumberOfPartitions;

  // TODO: add to config
  public static final long PARTITION_START_TIME_OFFSET = 15 * 60 * 1000; // 15 minutes

  public PartitionMetadataStore(
      AsyncCuratorFramework curator,
      boolean shouldCache,
      long maxPartitionCapacity,
      int minNumberOfPartitions) {
    super(
        curator,
        CreateMode.PERSISTENT,
        shouldCache,
        new PartitionMetadataSerializer().toModelSerializer(),
        PARTITION_MAP_METADATA_STORE_ZK_PATH);
    this.maxPartitionCapacity = maxPartitionCapacity;
    this.minNumberOfPartitions = minNumberOfPartitions;
  }

  public static long getProvisionedCapacity(long requestedProvisionedCapacity, PartitionMetadata partitionMetadata) {
    return requestedProvisionedCapacity < 0
        ? requestedProvisionedCapacity
        : partitionMetadata.provisionedCapacity;
  }

  public static boolean isUnprovisioned(PartitionMetadata partitionMetadata) {
    return partitionMetadata.getProvisionedCapacity() == 0;
  }

  /**
   * Returns the partition count based of the throughput. Performs div operation, and takes the ceil
   * of the division. Also, checks if minimum partition requirement is met
   *
   * @param throughput     service throughput to calculate number of partitions for
   * @return integer number of partition count
   */
  public int partitionCountForThroughput(long throughput) {
    int numberOfPartitions =
        (int) Math.ceilDiv(throughput, maxPartitionCapacity);
    return numberOfPartitions < minNumberOfPartitions
        ? numberOfPartitions + (minNumberOfPartitions - numberOfPartitions)
        : numberOfPartitions;
  }

  public long getMaxCapacityOrDefaultTo(long requestedMaxCapacity) {
    return requestedMaxCapacity == 0
        ? maxPartitionCapacity
        : requestedMaxCapacity;
  }

  public boolean hasSpaceForAdditionalProvisioning(PartitionMetadata partitionMetadata, long perPartitionCapacity) {
    return partitionMetadata.provisionedCapacity + perPartitionCapacity
      <= maxPartitionCapacity;
    // maybe should use the partitionMetadata's max capacity instead
  }
}
