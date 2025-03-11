package com.slack.astra.metadata.partition;

import static com.slack.astra.server.ManagerApiGrpc.MAX_TIME;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class PartitionMetadataStore extends AstraMetadataStore<PartitionMetadata> {
  public static final String PARTITION_MAP_METADATA_STORE_ZK_PATH = "/partition_map";

  // TODO: Move to config file
  public static final int PARTITION_CAPACITY = 5000000;

  public static final int MINIMUM_NUMBER_OF_PARTITIONS = 2;

  public PartitionMetadataStore(AsyncCuratorFramework curator, boolean shouldCache) {
    super(
        curator,
        CreateMode.PERSISTENT,
        shouldCache,
        new PartitionMetadataSerializer().toModelSerializer(),
        PARTITION_MAP_METADATA_STORE_ZK_PATH);
  }

  /**
   * Returns the partition count based of the throughput. Performs div operation, and returns the
   * ceil of the division
   *
   * @param throughput service throughput to calculate number of partitions for
   * @return integer number of partition count
   */
  public static int getPartitionCount(long throughput) {
    return (int) Math.ceilDiv(throughput, PARTITION_CAPACITY);
  }

  /**
   * finds the available partitions based utilization and updates the utilization if partitions are
   * considered.
   *
   * @param requiredThroughput - targeted throughput
   * @param requireDedicatedPartition - if dedicated partitions are required
   * @return list of string of partition ids
   */
  public List<String> findPartition(long requiredThroughput, boolean requireDedicatedPartition)
      throws InterruptedException {
    List<String> partitionIdsList = new ArrayList<>();
    int numberOfPartitions = getPartitionCount(requiredThroughput);

    long perPartitionCapacity = requiredThroughput / numberOfPartitions;
    // we want minimum of two partitions assigned to a tenant for redundancy purpose
    numberOfPartitions =
        numberOfPartitions < MINIMUM_NUMBER_OF_PARTITIONS
            ? numberOfPartitions + (MINIMUM_NUMBER_OF_PARTITIONS - numberOfPartitions)
            : numberOfPartitions;

    List<PartitionMetadata> partitionMetadataList = this.listSync();
    partitionMetadataList.sort(
        Comparator.comparing(partitionMetadata -> partitionMetadata.partitionId));

    for (PartitionMetadata partitionMetadata : partitionMetadataList) {
      if (requireDedicatedPartition) {
        if (partitionMetadata.getUtilization() == 0 && !partitionMetadata.isPartitionShared) {
          partitionIdsList.add(partitionMetadata.getPartitionID());
          this.updateSync(
              new PartitionMetadata(partitionMetadata.partitionId, perPartitionCapacity, false));
        }
      } else {
        // add logic for shared partition here
        // partition has capacity and (partition is shared or (partition not shared and utilization
        // = 0))
        // TODO: to add schema check for shared tenants
        if (partitionMetadata.utilization + perPartitionCapacity <= PARTITION_CAPACITY
            && (partitionMetadata.isPartitionShared
                || (partitionMetadata.getUtilization() == 0
                    && !partitionMetadata.isPartitionShared))) {
          partitionIdsList.add(partitionMetadata.getPartitionID());
          this.updateSync(
              new PartitionMetadata(
                  partitionMetadata.partitionId,
                  partitionMetadata.utilization + perPartitionCapacity,
                  true));
        }
      }
      if (partitionIdsList.size() == numberOfPartitions) {
        // we have found the required number of partitions
        return partitionIdsList;
      }
    }
    Thread.sleep(500); // to handle local cache sync
    // if we reached here means we did not find required number of partitions
    for (String partitionId : partitionIdsList) {
      PartitionMetadata partitionMetadata = this.getSync(partitionId);
      long utilization = partitionMetadata.utilization - perPartitionCapacity;
      boolean isPartitionShared = utilization != 0 && partitionMetadata.isPartitionShared;
      this.updateSync(new PartitionMetadata(partitionId, utilization, isPartitionShared));
    }
    return List.of();
  }

  /**
   * fetch the latest active partition metadata update the partition utilization and
   * isPartitionShared status
   *
   * @param datasetMetadata tenant metadata object
   * @return previous active partitionMetadata
   */
  public DatasetPartitionMetadata clearPartitions(DatasetMetadata datasetMetadata)
      throws InterruptedException {

    Optional<DatasetPartitionMetadata> previousActiveDatasetPartition =
        datasetMetadata.getPartitionConfigs().stream()
            .filter(
                datasetPartitionMetadata ->
                    datasetPartitionMetadata.getEndTimeEpochMs() == MAX_TIME)
            .findFirst();

    if (previousActiveDatasetPartition.isEmpty()) {
      return null;
    }
    // we only consider the latest partition config to be updated
    for (String partitionId : previousActiveDatasetPartition.get().getPartitions()) {
      PartitionMetadata existingPartitionMetadata = this.getSync(partitionId);
      int partitionCount = getPartitionCount(datasetMetadata.getThroughputBytes());
      long newUtilization =
          existingPartitionMetadata.getUtilization()
              - datasetMetadata.getThroughputBytes() / partitionCount;

      boolean isPartitionShared =
          newUtilization != 0 && existingPartitionMetadata.isPartitionShared;
      this.updateSync(new PartitionMetadata(partitionId, newUtilization, isPartitionShared));
      Thread.sleep(500); // for local cache to sync
    }
    return previousActiveDatasetPartition.get();
  }
}
