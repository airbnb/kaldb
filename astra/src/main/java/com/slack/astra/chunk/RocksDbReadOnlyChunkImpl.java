package com.slack.astra.chunk;

import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.logstore.rocksdb.query.RocksdbIndexSearcherImpl;
import com.slack.astra.logstore.search.LogIndexSearcher;
import com.slack.astra.metadata.cache.CacheNodeAssignment;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheSlotMetadataStore;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.curator.x.async.AsyncCuratorFramework;

public class RocksDbReadOnlyChunkImpl<T> extends ReadOnlyChunkImpl<T> {
  public RocksDbReadOnlyChunkImpl(
      AsyncCuratorFramework curatorFramework,
      MeterRegistry meterRegistry,
      BlobStore blobStore,
      SearchContext searchContext,
      String s3Bucket,
      String dataDirectoryPrefix,
      String replicaSet,
      CacheSlotMetadataStore cacheSlotMetadataStore,
      ReplicaMetadataStore replicaMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      SearchMetadataStore searchMetadataStore)
      throws Exception {
    super(
        curatorFramework,
        meterRegistry,
        blobStore,
        searchContext,
        s3Bucket,
        dataDirectoryPrefix,
        replicaSet,
        cacheSlotMetadataStore,
        replicaMetadataStore,
        snapshotMetadataStore,
        searchMetadataStore);
  }

  public RocksDbReadOnlyChunkImpl(
      AsyncCuratorFramework curatorFramework,
      MeterRegistry meterRegistry,
      BlobStore blobStore,
      SearchContext searchContext,
      String s3Bucket,
      String dataDirectoryPrefix,
      String replicaSet,
      CacheSlotMetadataStore cacheSlotMetadataStore,
      ReplicaMetadataStore replicaMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      SearchMetadataStore searchMetadataStore,
      CacheNodeAssignmentStore cacheNodeAssignmentStore,
      CacheNodeAssignment assignment,
      SnapshotMetadata snapshotMetadata)
      throws Exception {
    super(
        curatorFramework,
        meterRegistry,
        blobStore,
        searchContext,
        s3Bucket,
        dataDirectoryPrefix,
        replicaSet,
        cacheSlotMetadataStore,
        replicaMetadataStore,
        snapshotMetadataStore,
        searchMetadataStore,
        cacheNodeAssignmentStore,
        assignment,
        snapshotMetadata);
  }

  @Override
  public void setupDynamicAssignmentSearcher(Path dataDirectory) throws Exception {
    //    Path schemaPath = Path.of(dataDirectory.toString(), ReadWriteChunk.SCHEMA_FILE_NAME);
    //    if (!Files.exists(schemaPath)) {
    //      throw new RuntimeException("We expect a schema.json file to exist within the index");
    //    }
    //    setChunkSchema(ChunkSchema.deserializeFile(schemaPath));

    setChunkSchema(null);
    setLogSearcher((LogIndexSearcher<T>) new RocksdbIndexSearcherImpl(dataDirectory));
  }

  @Override
  public void setUpS3StreamingSearcher(BlobStore blobStore, String chunkId)
      throws RuntimeException, IOException {
    setChunkSchema(null);
    throw new UnsupportedOperationException("Streaming on s3 rocksdb is not supported");
    //        this.logSearcher =
    //            (LogIndexSearcher<T>)
    //                new LogIndexSearcherImpl(
    //                    LogIndexSearcherImpl.searcherManagerFromChunkId(chunkInfo.chunkId,
    // blobStore),
    //                    chunkSchema.fieldDefMap);
  }

  @Override
  public void setUpStaticChunkLogSearcher(SnapshotMetadata snapshotMetadata, Path dataDirectory)
      throws Exception {
    //      Path schemaPath = Path.of(dataDirectory.toString(), ReadWriteChunk.SCHEMA_FILE_NAME);
    //      if (!Files.exists(schemaPath)) {
    //        throw new RuntimeException("We expect a schema.json file to exist within the
    // index");
    //      }
    //      this.chunkSchema = ChunkSchema.deserializeFile(schemaPath);
    setChunkSchema(null);

    setChunkInfo(ChunkInfo.fromSnapshotMetadata(snapshotMetadata));
    setLogSearcher((LogIndexSearcher<T>) new RocksdbIndexSearcherImpl(dataDirectory));
  }
}
