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

  public static final int PARTITION_CAPACITY = 5000000;

  public PartitionMetadataStore(AsyncCuratorFramework curator, boolean shouldCache) {
    super(
        curator,
        CreateMode.PERSISTENT,
        shouldCache,
        new PartitionMetadataSerializer().toModelSerializer(),
        PARTITION_MAP_METADATA_STORE_ZK_PATH);
  }

  public static int getPartitionCount(long throughput) {
    return (int) Math.ceilDiv(throughput, PARTITION_CAPACITY);
  }

  public List<String> findPartition(long requiredThroughput, boolean requireDedicatedPartition) {
    List<String> partitionIdsList = new ArrayList<>();
    int numberOfPartitions = getPartitionCount(requiredThroughput);

    long perPartitionCapacity = requiredThroughput / numberOfPartitions;
    if (numberOfPartitions == 1) {
      // we want minimum of two partitions assigned to a tenant for redundancy purpose
      numberOfPartitions++;
    }

    List<PartitionMetadata> partitionMetadataList = this.listSync();
    partitionMetadataList.sort(
        Comparator.comparing(partitionMetadata -> partitionMetadata.partitionId));

    for (PartitionMetadata partitionMetadata : partitionMetadataList) {
      if (requireDedicatedPartition) {
        if (partitionMetadata.getUtilization() == 0 && !partitionMetadata.isPartitionShared) {
          partitionIdsList.add(partitionMetadata.getPartitionID());

          PartitionMetadata newPartitionMetadata =
              new PartitionMetadata(partitionMetadata.partitionId, perPartitionCapacity, false);
          this.updateSync(newPartitionMetadata);
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
          PartitionMetadata newPartitionMetadata =
              new PartitionMetadata(
                  partitionMetadata.partitionId,
                  partitionMetadata.utilization + perPartitionCapacity,
                  true);
          this.updateSync(newPartitionMetadata);
        }
      }
      if (partitionIdsList.size() == numberOfPartitions) {
        // we have found the required number of partitions
        return partitionIdsList;
      }
    }
    return List.of();
  }

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
      boolean newIsPartitionShared = existingPartitionMetadata.getIsPartitionShared();
      if (newUtilization == 0) {
        newIsPartitionShared = false;
      }
      this.updateSync(new PartitionMetadata(partitionId, newUtilization, newIsPartitionShared));
      Thread.sleep(500);
    }
    return previousActiveDatasetPartition.get();
  }
}
