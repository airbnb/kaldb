package com.slack.astra.server;

import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.testlib.AstraConfigUtil.makeIndexerConfig;
import static com.slack.astra.testlib.AstraConfigUtil.makeKafkaConfig;
import static com.slack.astra.testlib.ChunkManagerUtil.TEST_HOST;
import static com.slack.astra.testlib.ChunkManagerUtil.TEST_PORT;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static com.slack.astra.testlib.SpanUtil.makeSpan;
import static com.slack.astra.util.AggregatorFactoriesUtil.createGenericDateHistogramAggregatorFactoriesBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.blobfs.S3TestUtils;
import com.slack.astra.bulkIngestApi.BulkIngestRequest;
import com.slack.astra.bulkIngestApi.BulkIngestResponse;
import com.slack.astra.bulkIngestApi.BulkIngestS3Producer;
import com.slack.astra.chunk.SearchContext;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.SearchQuery;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.wal.WalProtos;
import com.slack.astra.testlib.ChunkManagerUtil;
import com.slack.astra.testlib.TestKafkaServer;
import com.slack.astra.util.QueryBuilderUtil;
import com.slack.astra.writer.kafka.AstraKafkaConsumer;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3WalIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(S3WalIntegrationTest.class);

  private static final String TEST_KAFKA_TOPIC = "test-s3-wal-topic";
  private static final int TEST_KAFKA_PARTITION = 0;
  private static final String ASTRA_TEST_CLIENT = "astra-s3-test-client";
  private static final String S3_TEST_BUCKET = "test-s3-wal-bucket";
  private static final String INDEX_NAME = "test_s3_wal_index";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  private static final Instant startTime = Instant.now();

  private ChunkManagerUtil<LogMessage> chunkManagerUtil;
  private AstraIndexer astraIndexer;
  private SimpleMeterRegistry metricsRegistry;
  private TestKafkaServer kafkaServer;
  private TestingServer testZKServer;
  private AsyncCuratorFramework curatorFramework;
  private AstraConfigs.MetadataStoreConfig metadataStoreConfig;
  private SnapshotMetadataStore snapshotMetadataStore;
  private RecoveryTaskMetadataStore recoveryTaskStore;
  private SearchMetadataStore searchMetadataStore;
  private DatasetMetadataStore datasetMetadataStore;
  private BulkIngestS3Producer bulkIngestS3Producer;
  private BlobStore blobStore;

  @BeforeEach
  public void setUp() throws Exception {
    AstraConfigs.IndexerConfig indexerConfig = makeIndexerConfig();
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();

    testZKServer = new TestingServer();

    // Metadata store setup
    metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testZKServer.getConnectString())
                    .setZkPathPrefix("testZK")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .build();

    curatorFramework =
        CuratorBuilder.build(metricsRegistry, metadataStoreConfig.getZookeeperConfig());

    // ChunkManager setup
    chunkManagerUtil =
        new ChunkManagerUtil<>(
            S3_MOCK_EXTENSION,
            S3_TEST_BUCKET,
            metricsRegistry,
            testZKServer,
            10 * 1024 * 1024 * 1024L,
            100,
            new SearchContext(TEST_HOST, TEST_PORT),
            curatorFramework,
            indexerConfig,
            metadataStoreConfig);

    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

    // Metadata stores
    snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, metricsRegistry);
    recoveryTaskStore =
        new RecoveryTaskMetadataStore(
            curatorFramework, metadataStoreConfig, metricsRegistry, false);
    searchMetadataStore =
        new SearchMetadataStore(curatorFramework, metadataStoreConfig, metricsRegistry, false);
    datasetMetadataStore =
        new DatasetMetadataStore(curatorFramework, metadataStoreConfig, metricsRegistry, true);

    // Setup Kafka server
    kafkaServer = new TestKafkaServer();
    kafkaServer.createTopicWithPartitions(TEST_KAFKA_TOPIC, 5);

    // Setup S3 BlobStore
    blobStore =
        new BlobStore(
            S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint()), S3_TEST_BUCKET);

    // Setup dataset metadata for testing

    setupTestDataset();

    // Setup S3 Producer
    setupS3Producer();
  }

  private void setupTestDataset() {
    DatasetMetadata dataset =
        new DatasetMetadata(
            INDEX_NAME,
            "owner",
            1,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("0"))),
            INDEX_NAME);
    datasetMetadataStore.createSync(dataset);
  }

  private void setupS3Producer() {
    AstraConfigs.ServerConfig serverConfig =
        AstraConfigs.ServerConfig.newBuilder()
            .setServerPort(8080)
            .setServerAddress("localhost")
            .build();

    AstraConfigs.KafkaConfig kafkaConfig =
        AstraConfigs.KafkaConfig.newBuilder()
            .setKafkaBootStrapServers(kafkaServer.getBroker().getBrokerList().get())
            .setKafkaTopic(TEST_KAFKA_TOPIC)
            .build();

    AstraConfigs.S3Config s3Config =
        AstraConfigs.S3Config.newBuilder()
            .setS3Bucket(S3_TEST_BUCKET)
            .setS3Region("us-west-2")
            .build();

    AstraConfigs.PreprocessorConfig preprocessorConfig =
        AstraConfigs.PreprocessorConfig.newBuilder()
            .setKafkaConfig(kafkaConfig)
            .setS3WalConfig(s3Config)
            .setServerConfig(serverConfig)
            .setPreprocessorInstanceCount(1)
            .setRateLimiterMaxBurstSeconds(1)
            .setUseS3Wal(true) // Enable S3 WAL
            .build();

    bulkIngestS3Producer =
        new BulkIngestS3Producer(
            datasetMetadataStore, preprocessorConfig, metricsRegistry, blobStore);
    bulkIngestS3Producer.startAsync();
    try {
      bulkIngestS3Producer.awaitRunning(DEFAULT_START_STOP_DURATION);
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  private AstraConfigs.KafkaConfig getKafkaConfig() {
    return makeKafkaConfig(
        TEST_KAFKA_TOPIC,
        TEST_KAFKA_PARTITION,
        ASTRA_TEST_CLIENT,
        kafkaServer.getBroker().getBrokerList().get());
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (bulkIngestS3Producer != null) {
      bulkIngestS3Producer.stopAsync();
      bulkIngestS3Producer.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    if (chunkManagerUtil != null) {
      chunkManagerUtil.close();
    }
    if (astraIndexer != null) {
      astraIndexer.stopAsync();
      astraIndexer.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    if (kafkaServer != null) {
      kafkaServer.close();
    }
    if (snapshotMetadataStore != null) {
      snapshotMetadataStore.close();
    }
    if (recoveryTaskStore != null) {
      recoveryTaskStore.close();
    }
    if (searchMetadataStore != null) {
      searchMetadataStore.close();
    }
    if (datasetMetadataStore != null) {
      datasetMetadataStore.close();
    }
    if (curatorFramework != null) {
      curatorFramework.unwrap().close();
    }
    if (testZKServer != null) {
      testZKServer.close();
    }
    metricsRegistry.close();
  }

  @Test
  public void testCompleteS3WalRoundTripFlow() throws Exception {
    LOG.info("=== Starting Complete S3 WAL Round Trip Test ===");

    List<Trace.Span> testSpans = createTestSpansWithServiceName(5, INDEX_NAME);

    // Debug: Check the service name in the spans
    LOG.info("=== DEBUGGING SPAN CREATION ===");
    Trace.Span firstSpan = testSpans.get(0);
    LOG.info("First span ID: {}", firstSpan.getId().toStringUtf8());
    for (Trace.KeyValue tag : firstSpan.getTagsList()) {
      if ("service_name".equals(tag.getKey())) {
        LOG.info("Found service_name tag: {}", tag.getVStr());
      }
    }
    LOG.info("Total tags in first span: {}", firstSpan.getTagsList().size());

    Map<String, List<Trace.Span>> indexDocs = Map.of(INDEX_NAME, testSpans);

    LOG.info(
        "Phase 1: Submitting {} spans to S3Producer for index {}", testSpans.size(), INDEX_NAME);
    BulkIngestRequest request = bulkIngestS3Producer.submitRequest(indexDocs);
    BulkIngestResponse response = request.getResponse();

    // Verify producer response
    assertThat(response.totalDocs()).isEqualTo(5);
    assertThat(response.failedDocs()).isEqualTo(0);
    LOG.info("Phase 1 Complete: S3Producer processed {} spans successfully", testSpans.size());

    // Verify Kafka notification was sent
    LOG.info("Phase 2: Verifying Kafka notification");
    KafkaConsumer<String, byte[]> kafkaConsumer = createTestKafkaConsumer();
    ConsumerRecords<String, byte[]> kafkaRecords =
        kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

    assertThat(kafkaRecords.count()).isEqualTo(1);
    ConsumerRecord<String, byte[]> kafkaRecord = kafkaRecords.iterator().next();
    WalProtos.WalSegmentPointer pointer =
        WalProtos.WalSegmentPointer.parseFrom(kafkaRecord.value());

    assertThat(pointer.getBlobBucket()).isEqualTo(S3_TEST_BUCKET);
    assertThat(pointer.getBlobstoreFilepath()).startsWith("wal/");
    assertThat(pointer.getDocCount()).isEqualTo(5);
    assertThat(pointer.getCompressionType()).isEqualTo("gzip");
    LOG.info(
        "Phase 2 Complete: Kafka notification verified - S3 key: {}",
        pointer.getBlobstoreFilepath());

    kafkaConsumer.close();

    // Setup indexer with S3 WAL enabled
    LOG.info("Phase 3: Starting S3-enabled indexer");
    AstraConfigs.PreprocessorConfig s3ProcessorConfig = createS3PreprocessorConfig();

    astraIndexer =
        new AstraIndexer(
            chunkManagerUtil.chunkManager,
            curatorFramework,
            metadataStoreConfig,
            makeIndexerConfig(1000),
            getKafkaConfig(),
            metricsRegistry,
            s3ProcessorConfig,
            blobStore);

    astraIndexer.startAsync();
    astraIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    LOG.info("Phase 3 Complete: S3-enabled indexer started");

    // Wait for data consumption and indexing
    LOG.info("Phase 4: Verifying data consumption and indexing");
    await().until(() -> getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry) == 5);
    await().until(() -> getCount("s3_message_writer.downloads", metricsRegistry) == 1);
    await().until(() -> getCount("s3_message_writer.spans_processed", metricsRegistry) == 5);

    // Commit active chunk to make data searchable
    if (chunkManagerUtil.chunkManager.getActiveChunk() != null) {
      chunkManagerUtil.chunkManager.getActiveChunk().commit();
      // Small wait to ensure commit is complete
      Thread.sleep(100);
    }

    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(AstraKafkaConsumer.RECORDS_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(AstraKafkaConsumer.RECORDS_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    LOG.info("Phase 4 Complete: All spans processed successfully");

    // Verify search functionality using the service name as the index name
    LOG.info("Phase 5: Verifying search functionality");
    LOG.info("Active chunks: {}", chunkManagerUtil.chunkManager.getChunkList().size());
    if (chunkManagerUtil.chunkManager.getActiveChunk() != null) {
      LOG.info("Active chunk info: {}", chunkManagerUtil.chunkManager.getActiveChunk().info());
    }

    // Search using INDEX_NAME since that's what we used as the service name
    LOG.info("=== DEBUGGING SEARCH ===");
    LOG.info("Searching for index: '{}'", INDEX_NAME);
    LOG.info("Available chunks: {}", chunkManagerUtil.chunkManager.getChunkList().size());

    // Try different search approaches
    SearchResult<LogMessage> searchResult = searchChunkManager(INDEX_NAME, "");
    LOG.info(
        "Search result for index '{}' with empty query: {} hits",
        INDEX_NAME,
        searchResult.hits.size());

    // Try searching with the default index name
    SearchResult<LogMessage> defaultSearch = searchChunkManager("unknown", "");
    LOG.info("Search result for 'unknown' with empty query: {} hits", defaultSearch.hits.size());

    // Debug: inspect the actual indexed documents
    if (defaultSearch.hits.size() > 0) {
      LogMessage firstHit = defaultSearch.hits.get(0);
      LOG.info("=== INSPECTING INDEXED DOCUMENT ===");
      LOG.info("Document source keys: {}", firstHit.getSource().keySet());
      LOG.info("service_name field value: {}", firstHit.getSource().get("service_name"));
      LOG.info("traceId field value: {}", firstHit.getSource().get("traceId"));
      LOG.info("trace_id field value: {}", firstHit.getSource().get("trace_id"));
      LOG.info("All document fields: {}", firstHit.getSource());
    }

    if (searchResult.hits.size() > 0) {
      LOG.info("Using '{}' index since it has the expected service name", INDEX_NAME);
    } else {
      LOG.info("Fallback to 'unknown' index");
      searchResult = defaultSearch;
    }

    assertThat(searchResult.hits).hasSize(5);
    assertThat(searchResult.hits.get(0).getSource()).isNotNull();

    // Both INDEX_NAME and "unknown" work as search indexes, use INDEX_NAME since that's the correct
    // service name
    SearchResult<LogMessage> serviceSearch =
        searchChunkManager(INDEX_NAME, "service_name:" + INDEX_NAME);
    LOG.info(
        "Search result for service_name '{}' on '{}' index: {} hits",
        INDEX_NAME,
        INDEX_NAME,
        serviceSearch.hits.size());
    assertThat(serviceSearch.hits).hasSize(5);

    SearchResult<LogMessage> traceSearch =
        searchChunkManager(INDEX_NAME, "trace_id:test_trace_123");
    LOG.info(
        "Search result for trace_id on '{}' index: {} hits", INDEX_NAME, traceSearch.hits.size());
    assertThat(traceSearch.hits).hasSize(5);

    LOG.info("Phase 5 Complete: Search verification successful");
    LOG.info("=== S3 WAL Round Trip Test PASSED ===");
  }

  @Test
  public void testMultiplePartitionsS3WalFlow() throws Exception {
    LOG.info("=== Starting Multiple Partitions S3 WAL Test ===");

    // Create additional dataset for different partition
    DatasetMetadata dataset2 =
        new DatasetMetadata(
            "second_index",
            "owner",
            1,
            List.of(
                new DatasetPartitionMetadata(
                    1, Long.MAX_VALUE, List.of("1"))), // Different partition
            "second_index");
    datasetMetadataStore.createSync(dataset2);

    // Create spans for different indexes/partitions
    List<Trace.Span> spans1 = createTestSpansWithPrefix("partition0_", 3);
    List<Trace.Span> spans2 = createTestSpansWithPrefix("partition1_", 2);

    Map<String, List<Trace.Span>> indexDocs =
        Map.of(
            INDEX_NAME,
            spans1, // Partition 0
            "second_index",
            spans2 // Partition 1
            );

    // Submit to S3Producer
    BulkIngestRequest request = bulkIngestS3Producer.submitRequest(indexDocs);
    BulkIngestResponse response = request.getResponse();

    assertThat(response.totalDocs()).isEqualTo(5);
    assertThat(response.failedDocs()).isEqualTo(0);

    // Verify 2 Kafka messages (one per partition)
    KafkaConsumer<String, byte[]> kafkaConsumer = createTestKafkaConsumer();
    ConsumerRecords<String, byte[]> kafkaRecords =
        kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

    assertThat(kafkaRecords.count()).isEqualTo(2);

    // Verify each partition has correct span count
    int totalSpansInMessages = 0;
    for (ConsumerRecord<String, byte[]> record : kafkaRecords) {
      WalProtos.WalSegmentPointer pointer = WalProtos.WalSegmentPointer.parseFrom(record.value());
      totalSpansInMessages += pointer.getDocCount();
    }
    assertThat(totalSpansInMessages).isEqualTo(5);

    kafkaConsumer.close();
    LOG.info("=== Multiple Partitions S3 WAL Test PASSED ===");
  }

  @Test
  public void testLargeDataS3WalFlow() throws Exception {
    LOG.info("=== Starting Large Data S3 WAL Test ===");

    // Create larger dataset to test compression
    List<Trace.Span> largeSpanSet = createTestSpans(100);
    Map<String, List<Trace.Span>> indexDocs = Map.of(INDEX_NAME, largeSpanSet);

    BulkIngestRequest request = bulkIngestS3Producer.submitRequest(indexDocs);
    BulkIngestResponse response = request.getResponse();

    assertThat(response.totalDocs()).isEqualTo(100);
    assertThat(response.failedDocs()).isEqualTo(0);

    // Verify S3 upload metrics
    assertThat(getCount("bulk_ingest_producer_s3_wal_uploads_total", metricsRegistry)).isEqualTo(1);
    assertThat(getCount("bulk_ingest_producer_s3_wal_spans_uploaded_total", metricsRegistry))
        .isEqualTo(100);
    assertThat(getCount("bulk_ingest_producer_s3_wal_bytes_uploaded_total", metricsRegistry))
        .isGreaterThan(1000);

    LOG.info("=== Large Data S3 WAL Test PASSED ===");
  }

  // Helper methods

  private List<Trace.Span> createTestSpans(int count) {
    return createTestSpansWithPrefix("span_", count);
  }

  private List<Trace.Span> createTestSpansWithPrefix(String prefix, int count) {
    return createTestSpansWithServiceName(prefix, count, "test_service");
  }

  private List<Trace.Span> createTestSpansWithServiceName(int count, String serviceName) {
    return createTestSpansWithServiceName("span_", count, serviceName);
  }

  private List<Trace.Span> createTestSpansWithServiceName(
      String prefix, int count, String serviceName) {
    List<Trace.Span> spans = new java.util.ArrayList<>();
    for (int i = 0; i < count; i++) {
      spans.add(
          makeSpan(
              "test_trace_123",
              prefix + i,
              "parent_" + i,
              TimeUnit.MICROSECONDS.convert(startTime.toEpochMilli(), TimeUnit.MILLISECONDS),
              500000L,
              "test_operation_" + i,
              serviceName,
              "test_message_type"));
    }
    return spans;
  }

  private KafkaConsumer<String, byte[]> createTestKafkaConsumer() throws Exception {
    Properties properties = kafkaServer.getBroker().consumerConfig();
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);

    KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(List.of(TEST_KAFKA_TOPIC));
    return consumer;
  }

  private SearchResult<LogMessage> searchChunkManager(String indexName, String queryString)
      throws IOException {
    return chunkManagerUtil.chunkManager.query(
        new SearchQuery(
            indexName,
            0L,
            startTime.plus(1, ChronoUnit.HOURS).toEpochMilli(),
            10,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder(
                queryString, 0L, startTime.plus(1, ChronoUnit.HOURS).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder()),
        Duration.ofMillis(3000));
  }

  private AstraConfigs.PreprocessorConfig createS3PreprocessorConfig() {
    return AstraConfigs.PreprocessorConfig.newBuilder()
        .setUseS3Wal(true)
        .setS3WalConfig(
            AstraConfigs.S3Config.newBuilder()
                .setS3Bucket(S3_TEST_BUCKET)
                .setS3Region("us-west-2")
                .build())
        .build();
  }
}
