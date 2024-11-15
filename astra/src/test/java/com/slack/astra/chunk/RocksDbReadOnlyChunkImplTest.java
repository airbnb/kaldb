package com.slack.astra.chunk;

import static com.slack.astra.chunk.ReadOnlyChunkImpl.CHUNK_ASSIGNMENT_TIMER;
import static com.slack.astra.chunk.ReadOnlyChunkImpl.CHUNK_EVICTION_TIMER;
import static com.slack.astra.util.AggregatorFactoriesUtil.createGenericDateHistogramAggregatorFactoriesBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.blobfs.S3TestUtils;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.SearchQuery;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.metadata.cache.CacheNodeAssignment;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheSlotMetadata;
import com.slack.astra.metadata.cache.CacheSlotMetadataStore;
import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.replica.ReplicaMetadata;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.search.SearchMetadata;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import com.slack.astra.testlib.MessageUtil;
import com.slack.astra.util.QueryBuilderUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.rocksdb.EnvOptions;
import org.rocksdb.SstFileWriter;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class RocksDbReadOnlyChunkImplTest {
  private static final String TEST_S3_BUCKET = "read-only-chunk-impl-test";

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;
  private BlobStore blobStore;

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(TEST_S3_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  @BeforeEach
  public void startup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    S3AsyncClient s3AsyncClient =
        S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
    blobStore = new BlobStore(s3AsyncClient, TEST_S3_BUCKET);
  }

  @AfterEach
  public void shutdown() throws IOException {
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldHandleRocksdbChunkLifecycle() throws Exception {
    AstraConfigs.AstraConfig AstraConfig = makeCacheConfig();
    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldHandleChunkLivecycle")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    AsyncCuratorFramework curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    ReplicaMetadataStore replicaMetadataStore = new ReplicaMetadataStore(curatorFramework);
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(curatorFramework);
    SearchMetadataStore searchMetadataStore = new SearchMetadataStore(curatorFramework, true);
    CacheSlotMetadataStore cacheSlotMetadataStore = new CacheSlotMetadataStore(curatorFramework);

    String replicaId = "foo";
    String snapshotId = "bar";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(curatorFramework, replicaId, snapshotId);
    initializeZkSnapshot(curatorFramework, snapshotId, 0);
    initializeBlobStorageWithRocksdbIndex(snapshotId);

    SearchContext searchContext =
        SearchContext.fromConfig(AstraConfig.getCacheConfig().getServerConfig());
    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new RocksDbReadOnlyChunkImpl<>(
            curatorFramework,
            meterRegistry,
            blobStore,
            searchContext,
            AstraConfig.getS3Config().getS3Bucket(),
            AstraConfig.getCacheConfig().getDataDirectory(),
            AstraConfig.getCacheConfig().getReplicaSet(),
            cacheSlotMetadataStore,
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore);

    // wait for chunk to register
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    assignReplicaToChunk(cacheSlotMetadataStore, replicaId, readOnlyChunk);

    // ensure that the chunk was marked LIVE
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 1);
    assertThat(readOnlyChunk.getChunkMetadataState())
        .isEqualTo(Metadata.CacheSlotMetadata.CacheSlotState.LIVE);

    SearchResult<LogMessage> logMessageSearchResult =
        readOnlyChunk.query(
            new SearchQuery(
                "Message1",
                Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                Instant.now().toEpochMilli(),
                500,
                Collections.emptyList(),
                QueryBuilderUtil.generateQueryBuilder(
                    String.format("keyField:%s:%s", PRIMARY_KEY_HEX, SECONDARY_KEY_HEX),
                    Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                    Instant.now().toEpochMilli()),
                null,
                createGenericDateHistogramAggregatorFactoriesBuilder()));
    assertThat(logMessageSearchResult.hits.size()).isEqualTo(2);
    // assertThat(logMessageSearchResult.hits.get(0).getId()).isEqualTo("Message1");

    await()
        .until(
            () ->
                meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count()
                    == 1);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_EVICTION_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_EVICTION_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(0);

    // ensure we registered a search node for this cache slot
    await().until(() -> searchMetadataStore.listSync().size() == 1);
    assertThat(searchMetadataStore.listSync().get(0).snapshotName).isEqualTo(snapshotId);

    assertThat(searchMetadataStore.listSync().get(0).url).isEqualTo("gproto+http://localhost:8080");
    assertThat(searchMetadataStore.listSync().get(0).name)
        .isEqualTo(SearchMetadata.generateSearchContextSnapshotId(snapshotId, "localhost"));

    // mark the chunk for eviction
    CacheSlotMetadata cacheSlotMetadata =
        cacheSlotMetadataStore.getSync(searchContext.hostname, readOnlyChunk.slotId);
    cacheSlotMetadataStore
        .updateNonFreeCacheSlotState(
            cacheSlotMetadata, Metadata.CacheSlotMetadata.CacheSlotState.EVICT)
        .get(1, TimeUnit.SECONDS);

    // ensure that the evicted chunk was released
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    // ensure the search metadata node was unregistered
    await().until(() -> searchMetadataStore.listSync().size() == 0);

    SearchResult<LogMessage> logMessageEmptySearchResult =
        readOnlyChunk.query(
            new SearchQuery(
                MessageUtil.TEST_DATASET_NAME,
                Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                Instant.now().toEpochMilli(),
                500,
                Collections.emptyList(),
                QueryBuilderUtil.generateQueryBuilder(
                    "*:*",
                    Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                    Instant.now().toEpochMilli()),
                null,
                createGenericDateHistogramAggregatorFactoriesBuilder()));
    assertThat(logMessageEmptySearchResult).isEqualTo(SearchResult.empty());
    assertThat(readOnlyChunk.info()).isNull();

    assertThat(meterRegistry.get(CHUNK_EVICTION_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(1);
    assertThat(meterRegistry.get(CHUNK_EVICTION_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(1);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(0);

    curatorFramework.unwrap().close();
  }

  @Test
  public void shouldHandleRocksdbChunkLifeSST() throws Exception {
    AstraConfigs.AstraConfig AstraConfig = makeCacheConfig();
    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldHandleChunkLivecycle")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    AsyncCuratorFramework curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    ReplicaMetadataStore replicaMetadataStore = new ReplicaMetadataStore(curatorFramework);
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(curatorFramework);
    SearchMetadataStore searchMetadataStore = new SearchMetadataStore(curatorFramework, true);
    CacheSlotMetadataStore cacheSlotMetadataStore = new CacheSlotMetadataStore(curatorFramework);

    String replicaId = "foo";
    String snapshotId = "bar";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(curatorFramework, replicaId, snapshotId);
    initializeZkSnapshot(curatorFramework, snapshotId, 0);
    initializeBlobStorageWithLocalSST(snapshotId);

    SearchContext searchContext =
        SearchContext.fromConfig(AstraConfig.getCacheConfig().getServerConfig());
    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new RocksDbReadOnlyChunkImpl<>(
            curatorFramework,
            meterRegistry,
            blobStore,
            searchContext,
            AstraConfig.getS3Config().getS3Bucket(),
            AstraConfig.getCacheConfig().getDataDirectory(),
            AstraConfig.getCacheConfig().getReplicaSet(),
            cacheSlotMetadataStore,
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore);

    // wait for chunk to register
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    assignReplicaToChunk(cacheSlotMetadataStore, replicaId, readOnlyChunk);

    // ensure that the chunk was marked LIVE
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 1);
    assertThat(readOnlyChunk.getChunkMetadataState())
        .isEqualTo(Metadata.CacheSlotMetadata.CacheSlotState.LIVE);

    await()
        .until(
            () ->
                meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count()
                    == 1);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_EVICTION_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_EVICTION_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(0);

    // ensure we registered a search node for this cache slot
    await().until(() -> searchMetadataStore.listSync().size() == 1);
    assertThat(searchMetadataStore.listSync().get(0).snapshotName).isEqualTo(snapshotId);

    assertThat(searchMetadataStore.listSync().get(0).url).isEqualTo("gproto+http://localhost:8080");
    assertThat(searchMetadataStore.listSync().get(0).name)
        .isEqualTo(SearchMetadata.generateSearchContextSnapshotId(snapshotId, "localhost"));

    // mark the chunk for eviction
    CacheSlotMetadata cacheSlotMetadata =
        cacheSlotMetadataStore.getSync(searchContext.hostname, readOnlyChunk.slotId);
    cacheSlotMetadataStore
        .updateNonFreeCacheSlotState(
            cacheSlotMetadata, Metadata.CacheSlotMetadata.CacheSlotState.EVICT)
        .get(1, TimeUnit.SECONDS);

    // ensure that the evicted chunk was released
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    // ensure the search metadata node was unregistered
    await().until(() -> searchMetadataStore.listSync().size() == 0);

    SearchResult<LogMessage> logMessageEmptySearchResult =
        readOnlyChunk.query(
            new SearchQuery(
                MessageUtil.TEST_DATASET_NAME,
                Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                Instant.now().toEpochMilli(),
                500,
                Collections.emptyList(),
                QueryBuilderUtil.generateQueryBuilder(
                    "*:*",
                    Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                    Instant.now().toEpochMilli()),
                null,
                createGenericDateHistogramAggregatorFactoriesBuilder()));
    assertThat(logMessageEmptySearchResult).isEqualTo(SearchResult.empty());
    assertThat(readOnlyChunk.info()).isNull();

    assertThat(meterRegistry.get(CHUNK_EVICTION_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(1);
    assertThat(meterRegistry.get(CHUNK_EVICTION_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(1);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(0);

    curatorFramework.unwrap().close();
  }

  @Test
  public void shouldHandleMissingS3Assets() throws Exception {
    AstraConfigs.AstraConfig AstraConfig = makeCacheConfig();
    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldHandleMissingS3Assets")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    AsyncCuratorFramework curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    ReplicaMetadataStore replicaMetadataStore = new ReplicaMetadataStore(curatorFramework);
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(curatorFramework);
    SearchMetadataStore searchMetadataStore = new SearchMetadataStore(curatorFramework, true);
    CacheSlotMetadataStore cacheSlotMetadataStore = new CacheSlotMetadataStore(curatorFramework);

    String replicaId = "foo";
    String snapshotId = "bar";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(curatorFramework, replicaId, snapshotId);
    initializeZkSnapshot(curatorFramework, snapshotId, 0);

    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new RocksDbReadOnlyChunkImpl<>(
            curatorFramework,
            meterRegistry,
            blobStore,
            SearchContext.fromConfig(AstraConfig.getCacheConfig().getServerConfig()),
            AstraConfig.getS3Config().getS3Bucket(),
            AstraConfig.getCacheConfig().getDataDirectory(),
            AstraConfig.getCacheConfig().getReplicaSet(),
            cacheSlotMetadataStore,
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore);

    // wait for chunk to register
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    assignReplicaToChunk(cacheSlotMetadataStore, replicaId, readOnlyChunk);

    // assert that the chunk was released back to free
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    // ensure we did not register a search node
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(0);

    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(1);

    curatorFramework.unwrap().close();
  }

  @Test
  public void shouldHandleMissingZkData() throws Exception {
    AstraConfigs.AstraConfig AstraConfig = makeCacheConfig();
    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldHandleMissingZkData")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    AsyncCuratorFramework curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    ReplicaMetadataStore replicaMetadataStore = new ReplicaMetadataStore(curatorFramework);
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(curatorFramework);
    SearchMetadataStore searchMetadataStore = new SearchMetadataStore(curatorFramework, true);
    CacheSlotMetadataStore cacheSlotMetadataStore = new CacheSlotMetadataStore(curatorFramework);

    String replicaId = "foo";
    String snapshotId = "bar";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(curatorFramework, replicaId, snapshotId);
    // we intentionally do not initialize a Snapshot, so the lookup is expected to fail

    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new RocksDbReadOnlyChunkImpl<>(
            curatorFramework,
            meterRegistry,
            blobStore,
            SearchContext.fromConfig(AstraConfig.getCacheConfig().getServerConfig()),
            AstraConfig.getS3Config().getS3Bucket(),
            AstraConfig.getCacheConfig().getDataDirectory(),
            AstraConfig.getCacheConfig().getReplicaSet(),
            cacheSlotMetadataStore,
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore);

    // wait for chunk to register
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    assignReplicaToChunk(cacheSlotMetadataStore, replicaId, readOnlyChunk);

    // assert that the chunk was released back to free
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    // ensure we did not register a search node
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(0);

    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(1);

    curatorFramework.unwrap().close();
  }

  @Test
  public void closeShouldCleanupLiveChunkCorrectly() throws Exception {
    AstraConfigs.AstraConfig AstraConfig = makeCacheConfig();
    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldHandleChunkLivecycle")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    AsyncCuratorFramework curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    ReplicaMetadataStore replicaMetadataStore = new ReplicaMetadataStore(curatorFramework);
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(curatorFramework);
    SearchMetadataStore searchMetadataStore = new SearchMetadataStore(curatorFramework, true);
    CacheSlotMetadataStore cacheSlotMetadataStore = new CacheSlotMetadataStore(curatorFramework);

    String replicaId = "foo";
    String snapshotId = "bar";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(curatorFramework, replicaId, snapshotId);
    initializeZkSnapshot(curatorFramework, snapshotId, 0);
    initializeBlobStorageWithRocksdbIndex(snapshotId);

    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new RocksDbReadOnlyChunkImpl<>(
            curatorFramework,
            meterRegistry,
            blobStore,
            SearchContext.fromConfig(AstraConfig.getCacheConfig().getServerConfig()),
            AstraConfig.getS3Config().getS3Bucket(),
            AstraConfig.getCacheConfig().getDataDirectory(),
            AstraConfig.getCacheConfig().getReplicaSet(),
            cacheSlotMetadataStore,
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore);

    // wait for chunk to register
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    assignReplicaToChunk(cacheSlotMetadataStore, replicaId, readOnlyChunk);

    // ensure that the chunk was marked LIVE
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.LIVE);

    SearchQuery query =
        new SearchQuery(
            "Message1",
            Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().toEpochMilli(),
            500,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder(
                "Message1",
                Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                Instant.now().toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    SearchResult<LogMessage> logMessageSearchResult = readOnlyChunk.query(query);
    assertThat(logMessageSearchResult.hits.size()).isEqualTo(1);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(1);

    // ensure we registered a search node for this cache slot
    await().until(() -> searchMetadataStore.listSync().size() == 1);
    assertThat(searchMetadataStore.listSync().get(0).snapshotName).isEqualTo(snapshotId);

    assertThat(searchMetadataStore.listSync().get(0).url).isEqualTo("gproto+http://localhost:8080");
    assertThat(searchMetadataStore.listSync().get(0).name)
        .isEqualTo(SearchMetadata.generateSearchContextSnapshotId(snapshotId, "localhost"));

    // verify we have files on disk
    try (var files = java.nio.file.Files.list(readOnlyChunk.getDataDirectory())) {
      assertThat(files.findFirst().isPresent()).isTrue();
    }

    // attempt to close the readOnlyChunk
    readOnlyChunk.close();

    // verify no results are returned for the exact same query we did above
    SearchResult<LogMessage> logMessageSearchResultEmpty = readOnlyChunk.query(query);
    assertThat(logMessageSearchResultEmpty).isEqualTo(SearchResult.empty());
    assertThat(readOnlyChunk.info()).isNull();

    // verify that the directory has been cleaned up
    try (var files = java.nio.file.Files.list(readOnlyChunk.getDataDirectory())) {
      assertThat(files.findFirst().isPresent()).isFalse();
    }

    curatorFramework.unwrap().close();
  }

  @Test
  public void shouldHandleDynamicChunkSizeLifecycle() throws Exception {
    AstraConfigs.AstraConfig AstraConfig = makeCacheConfig();
    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldHandleChunkLivecycle")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    AsyncCuratorFramework curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    ReplicaMetadataStore replicaMetadataStore = new ReplicaMetadataStore(curatorFramework);
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(curatorFramework);
    SearchMetadataStore searchMetadataStore = new SearchMetadataStore(curatorFramework, true);
    CacheSlotMetadataStore cacheSlotMetadataStore = new CacheSlotMetadataStore(curatorFramework);
    CacheNodeAssignmentStore cacheNodeAssignmentStore =
        new CacheNodeAssignmentStore(curatorFramework);

    String replicaId = "foo";
    String snapshotId = "boo";
    String assignmentId = "dog";
    String cacheNodeId = "baz";
    String replicaSet = "cat";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(curatorFramework, replicaId, snapshotId);
    initializeZkSnapshot(curatorFramework, snapshotId, 29);
    initializeBlobStorageWithRocksdbIndex(snapshotId);
    initializeCacheNodeAssignment(
        cacheNodeAssignmentStore, assignmentId, snapshotId, cacheNodeId, replicaSet, replicaId);

    SearchContext searchContext =
        SearchContext.fromConfig(AstraConfig.getCacheConfig().getServerConfig());
    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new RocksDbReadOnlyChunkImpl<>(
            curatorFramework,
            meterRegistry,
            blobStore,
            searchContext,
            AstraConfig.getS3Config().getS3Bucket(),
            AstraConfig.getCacheConfig().getDataDirectory(),
            AstraConfig.getCacheConfig().getReplicaSet(),
            cacheSlotMetadataStore,
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore,
            cacheNodeAssignmentStore,
            cacheNodeAssignmentStore.getSync(cacheNodeId, assignmentId),
            snapshotMetadataStore.findSync(snapshotId));

    // wait for chunk to register
    // ignoreExceptions is workaround for https://github.com/aws/aws-sdk-java-v2/issues/3658
    await()
        .ignoreExceptions()
        .until(
            () -> {
              Path dataDirectory =
                  Path.of(
                      String.format(
                          "%s/astra-chunk-%s",
                          AstraConfig.getCacheConfig().getDataDirectory(), assignmentId));

              if (java.nio.file.Files.isDirectory(dataDirectory)) {
                FileUtils.cleanDirectory(dataDirectory.toFile());
              }
              readOnlyChunk.downloadChunkData();

              return cacheNodeAssignmentStore.getSync(
                          readOnlyChunk.getCacheNodeAssignment().cacheNodeId,
                          readOnlyChunk.getCacheNodeAssignment().assignmentId)
                      .state
                  == Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE;
            });

    SearchResult<LogMessage> logMessageSearchResult =
        readOnlyChunk.query(
            new SearchQuery(
                "Message1",
                Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                Instant.now().toEpochMilli(),
                500,
                Collections.emptyList(),
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1",
                    Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                    Instant.now().toEpochMilli()),
                null,
                createGenericDateHistogramAggregatorFactoriesBuilder()));
    assertThat(logMessageSearchResult.hits.size()).isEqualTo(1);

    // ensure we registered a search node for this cache assignment
    await().until(() -> searchMetadataStore.listSync().size() == 1);
    assertThat(searchMetadataStore.listSync().get(0).snapshotName).isEqualTo(snapshotId);

    assertThat(searchMetadataStore.listSync().get(0).url).isEqualTo("gproto+http://localhost:8080");
    assertThat(searchMetadataStore.listSync().get(0).name)
        .isEqualTo(SearchMetadata.generateSearchContextSnapshotId(snapshotId, "localhost"));

    // simulate eviction
    readOnlyChunk.evictChunk(cacheNodeAssignmentStore.findSync(assignmentId));

    // verify that the directory has been cleaned up
    try (var files = java.nio.file.Files.list(readOnlyChunk.getDataDirectory())) {
      assertThat(files.findFirst().isPresent()).isFalse();
    }

    curatorFramework.unwrap().close();
  }

  private void assignReplicaToChunk(
      CacheSlotMetadataStore cacheSlotMetadataStore,
      String replicaId,
      ReadOnlyChunkImpl<LogMessage> readOnlyChunk) {
    // update chunk to assigned
    CacheSlotMetadata updatedCacheSlotMetadata =
        new CacheSlotMetadata(
            readOnlyChunk.slotId,
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaId,
            Instant.now().toEpochMilli(),
            readOnlyChunk.searchContext.hostname,
            "rep1");
    cacheSlotMetadataStore.updateAsync(updatedCacheSlotMetadata);
  }

  private void initializeZkSnapshot(
      AsyncCuratorFramework curatorFramework, String snapshotId, long sizeInBytesOnDisk)
      throws Exception {
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(curatorFramework);
    snapshotMetadataStore.createSync(
        new SnapshotMetadata(
            snapshotId,
            Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().toEpochMilli(),
            1,
            "partitionId",
            sizeInBytesOnDisk));
  }

  private void initializeZkReplica(
      AsyncCuratorFramework curatorFramework, String replicaId, String snapshotId)
      throws Exception {
    ReplicaMetadataStore replicaMetadataStore = new ReplicaMetadataStore(curatorFramework);
    replicaMetadataStore.createSync(
        new ReplicaMetadata(
            replicaId,
            snapshotId,
            "rep1",
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(60).toEpochMilli(),
            false));
  }

  private static final String PRIMARY_KEY_HEX = "4a6f686e446f65"; // Example: "JohnDoe" in hex
  private static final String SECONDARY_KEY_HEX = "4a616e65446f65"; // Example: "JaneDoe" in hex

  private void initializeBlobStorageWithRocksdbIndex(String snapshotId) throws Exception {
    File dataDirectory = Files.newTemporaryFolder();

    byte[] primaryKeyBytes = Base64.getDecoder().decode(PRIMARY_KEY_HEX);
    int primaryKeySize = primaryKeyBytes.length;
    byte[] secondaryKeyBytes = Base64.getDecoder().decode(SECONDARY_KEY_HEX);
    ByteBuffer key1 = ByteBuffer.allocate(8 + primaryKeySize + secondaryKeyBytes.length + 4);
    key1.putLong(primaryKeySize);
    key1.put(primaryKeyBytes);
    key1.put(secondaryKeyBytes);
    key1.putInt(1024);

    ByteBuffer key2 = ByteBuffer.allocate(8 + primaryKeySize + secondaryKeyBytes.length + 4);
    key2.putLong(primaryKeySize);
    key2.put(primaryKeyBytes);
    key2.put(secondaryKeyBytes);
    key2.putInt(2048);

    byte[] value1 = key1.array();
    byte[] value2 = key2.array();

    try (org.rocksdb.Options options = new org.rocksdb.Options();
        EnvOptions envOptions = new EnvOptions();
        SstFileWriter sstFileWriter = new SstFileWriter(envOptions, options)) {

      // Open the SST file
      sstFileWriter.open(dataDirectory.toString() + "/a.sst");

      // Write the key-value pair
      sstFileWriter.put(key1.array(), value1);
      sstFileWriter.put(key2.array(), value2);

      // Finish writing the SST file
      sstFileWriter.finish();

      System.out.println("Generated SST file: " + dataDirectory.toString());
    }

    // Copy files to S3.
    blobStore.upload(snapshotId, dataDirectory.toPath());

    List<String> s3Files = blobStore.listFiles(snapshotId);
    assertThat(s3Files.size()).isGreaterThanOrEqualTo(dataDirectory.listFiles().length);
  }

  private void initializeBlobStorageWithLocalSST(String snapshotId) throws Exception {
    String dataDirectory = "/Users/suman_karumuri/temp/test_rocksdb_sst/";
    // create a new path
    Path path = Paths.get(dataDirectory);
    // Copy files to S3.
    blobStore.upload(snapshotId, path);
  }

  private void initializeCacheNodeAssignment(
      CacheNodeAssignmentStore cacheNodeAssignmentStore,
      String assignmentId,
      String snapshotId,
      String cacheNodeId,
      String replicaSet,
      String replicaId)
      throws Exception {
    cacheNodeAssignmentStore.createSync(
        new CacheNodeAssignment(
            assignmentId,
            cacheNodeId,
            snapshotId,
            replicaId,
            replicaSet,
            0,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LOADING));
  }

  private AstraConfigs.AstraConfig makeCacheConfig() {
    AstraConfigs.CacheConfig cacheConfig =
        AstraConfigs.CacheConfig.newBuilder()
            .setSlotsPerInstance(3)
            .setReplicaSet("rep1")
            .setDataDirectory(
                String.format(
                    "/tmp/%s/%s",
                    this.getClass().getSimpleName(), RandomStringUtils.randomAlphabetic(10)))
            .setServerConfig(
                AstraConfigs.ServerConfig.newBuilder()
                    .setServerAddress("localhost")
                    .setServerPort(8080)
                    .build())
            .build();

    AstraConfigs.S3Config s3Config =
        AstraConfigs.S3Config.newBuilder()
            .setS3Bucket(TEST_S3_BUCKET)
            .setS3Region("us-east-1")
            .build();

    return AstraConfigs.AstraConfig.newBuilder()
        .setCacheConfig(cacheConfig)
        .setS3Config(s3Config)
        .build();
  }
}
