package com.slack.astra.metadata.partition;

import com.slack.astra.metadata.core.AstraMetadataStore;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class PartitionMetadataStore extends AstraMetadataStore<PartitionMetadata> {
  public static final String PARTITION_MAP_METADATA_STORE_ZK_PATH = "/partition_map";

  public static final int PARTITION_CAPACITY = 5000000;

  public PartitionMetadataStore(AsyncCuratorFramework curator, boolean shouldCache)
      throws Exception {
    super(
        curator,
        CreateMode.PERSISTENT,
        shouldCache,
        new PartitionMetadataSerializer().toModelSerializer(),
        PARTITION_MAP_METADATA_STORE_ZK_PATH);
  }

  public List<String> findPartition(long requiredThroughput, boolean requireDedicatedPartition) {
    List<String> partitionIdsList = new ArrayList<>();
    int numberOfPartitions = (int) Math.ceilDiv(requiredThroughput, PARTITION_CAPACITY);

    long perPartitionCapacity = requiredThroughput / numberOfPartitions;
    if (numberOfPartitions == 1) {
      // we want minimum of two partitions assigned to a tenant for redundancy purpose
      numberOfPartitions++;
    }

    List<PartitionMetadata> partitionMetadataList = this.listSync();
    partitionMetadataList.sort(Comparator.comparing(partitionMetadata -> partitionMetadata.partition_id));

    for (PartitionMetadata partitionMetadata : partitionMetadataList) {
      if (requireDedicatedPartition) {
        if (partitionMetadata.getUtilization() == 0 && !partitionMetadata.isPartitionShared) {
          partitionIdsList.add(partitionMetadata.getPartitionID());

          PartitionMetadata newPartitionMetadata = new PartitionMetadata(
                  partitionMetadata.partition_id,
                  perPartitionCapacity,
                  partitionMetadata.isPartitionShared
          );
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
          PartitionMetadata newPartitionMetadata = new PartitionMetadata(
                  partitionMetadata.partition_id,
                  partitionMetadata.utilization + perPartitionCapacity,
                  true
          );
          this.updateSync(newPartitionMetadata);
        }
      }
      if (partitionIdsList.size() == numberOfPartitions) {
        // we have found the required number of partitions
        break;
      }
    }
    return partitionIdsList;
  }
}
