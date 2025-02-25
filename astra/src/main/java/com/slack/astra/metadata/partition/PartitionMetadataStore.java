package com.slack.astra.metadata.partition;

import com.slack.astra.metadata.core.AstraMetadataStore;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class PartitionMetadataStore extends AstraMetadataStore<PartitionMetadata> {
  public static final String PARTITION_MAP_METADATA_STORE_ZK_PATH = "/partition_map";

  public PartitionMetadataStore(AsyncCuratorFramework curator, boolean shouldCache)
      throws Exception {
    super(
        curator,
        CreateMode.PERSISTENT,
        shouldCache,
        new PartitionMetadataSerializer().toModelSerializer(),
        PARTITION_MAP_METADATA_STORE_ZK_PATH);
  }
}
