package com.slack.astra.bulkIngestApi;

import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import brave.Tracing;
import com.google.protobuf.ByteString;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.wal.WalProtos;
import com.slack.astra.testlib.MetricsUtil;
import com.slack.astra.testlib.TestKafkaServer;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BulkIngestS3ProducerTest {

  private static final Logger LOG = LoggerFactory.getLogger(BulkIngestS3ProducerTest.class);
  private static MeterRegistry meterRegistry;
  private static AsyncCuratorFramework curatorFramework;
  private static AstraConfigs.PreprocessorConfig preprocessorConfig;
  private static DatasetMetadataStore datasetMetadataStore;
  private static TestingServer zkServer;
  private static TestKafkaServer kafkaServer;
  private static BlobStore mockBlobStore;
  private BulkIngestS3Producer bulkIngestS3Producer;

  static String INDEX_NAME = "testtransactionindex";
  private static String TEST_S3_BUCKET = "test-wal-bucket";
  private static String DOWNSTREAM_TOPIC = "test-transaction-topic-out";

  @BeforeEach
  public void bootstrapCluster() throws Exception {

    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();

    mockBlobStore = mock(BlobStore.class);
    zkServer = new TestingServer();
    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(zkServer.getConnectString())
            .setZkPathPrefix("testZK")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .setZkCacheInitTimeoutMs(1000)
            .build();

    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(zkConfig)
            .build();

    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);

    kafkaServer = new TestKafkaServer();
    kafkaServer.createTopicWithPartitions(DOWNSTREAM_TOPIC, 5);

    AstraConfigs.ServerConfig serverConfig =
        AstraConfigs.ServerConfig.newBuilder()
            .setServerPort(8080)
            .setServerAddress("localhost")
            .build();
    AstraConfigs.KafkaConfig kafkaConfig =
        AstraConfigs.KafkaConfig.newBuilder()
            .setKafkaBootStrapServers(kafkaServer.getBroker().getBrokerList().get())
            .setKafkaTopic(DOWNSTREAM_TOPIC)
            .build();
    AstraConfigs.S3Config s3Config =
        AstraConfigs.S3Config.newBuilder()
            .setS3Bucket(TEST_S3_BUCKET)
            .setS3Region("us-west-2")
            .build();

    AstraConfigs.S3WalBufferConfig s3WalBufferConfig =
        AstraConfigs.S3WalBufferConfig.newBuilder()
            .setMaxBufferTimeMs(100) // Short timeout for tests
            .setMaxRequestsPerBatch(10) // Small batch size for tests
            .setMaxBufferSizeMb(1) // Small buffer for tests
            .setMinBufferSizeMb(1) // Small min buffer for tests
            .setMaxRequestsPerS3Object(100)
            .setMaxQueueSize(1000)
            .build();

    preprocessorConfig =
        AstraConfigs.PreprocessorConfig.newBuilder()
            .setKafkaConfig(kafkaConfig)
            .setS3WalConfig(s3Config)
            .setS3WalBufferConfig(s3WalBufferConfig)
            .setServerConfig(serverConfig)
            .setPreprocessorInstanceCount(1)
            .setRateLimiterMaxBurstSeconds(1)
            .build();

    datasetMetadataStore =
        new DatasetMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true);
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            INDEX_NAME,
            "owner",
            1,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("1"))),
            INDEX_NAME);
    // Create an entry while init. Update the entry on every test run
    datasetMetadataStore.createSync(datasetMetadata);

    bulkIngestS3Producer =
        new BulkIngestS3Producer(
            datasetMetadataStore, preprocessorConfig, meterRegistry, mockBlobStore);
    bulkIngestS3Producer.startAsync();
    bulkIngestS3Producer.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  @AfterEach
  public void tearDown() throws Exception {
    System.clearProperty("astra.bulkIngest.useKafkaTransactions");
    if (bulkIngestS3Producer != null) {
      bulkIngestS3Producer.stopAsync();
      bulkIngestS3Producer.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    if (kafkaServer != null) {
      kafkaServer.close();
    }
    if (meterRegistry != null) {
      meterRegistry.close();
    }
    if (datasetMetadataStore != null) {
      datasetMetadataStore.close();
    }
    if (curatorFramework != null) {
      curatorFramework.unwrap().close();
    }
    if (zkServer != null) {
      zkServer.close();
    }
  }

  @Test
  public void testS3UploadandKafkaNotification() throws Exception {

    Trace.Span doc1 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("test1")).build();
    Map<String, List<Trace.Span>> indexDocs = Map.of(INDEX_NAME, List.of(doc1));

    BulkIngestRequest request = bulkIngestS3Producer.submitRequest(indexDocs);
    AtomicReference<BulkIngestResponse> response = new AtomicReference<>();

    Thread.ofVirtual()
        .start(
            () -> {
              try {
                response.set(request.getResponse());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    await().until(() -> response.get() != null);

    // verify that the response is successful
    assertThat(response.get().totalDocs()).isEqualTo(1);
    assertThat(response.get().failedDocs()).isEqualTo(0);

    // Verify that the S3 upload was called
    verify(mockBlobStore).upload(any(String.class), any(byte[].class));

    assertThat(MetricsUtil.getCount("bulk_ingest_producer_s3_wal_uploads_total", meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount("bulk_ingest_producer_s3_wal_spans_uploaded_total", meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount("bulk_ingest_producer_s3_wal_bytes_uploaded_total", meterRegistry))
        .isGreaterThan(1);

    KafkaConsumer<String, byte[]> kafkaConsumer = getTestKafkaConsumer();
    ConsumerRecords<String, byte[]> records =
        kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

    assertThat(records.count()).isEqualTo(1);

    for (ConsumerRecord<String, byte[]> record : records) {
      // Parse the WalSegmentPointer from the message
      WalProtos.WalSegmentPointer pointer = WalProtos.WalSegmentPointer.parseFrom(record.value());
      assertThat(pointer.getBlobBucket()).isEqualTo(TEST_S3_BUCKET);
      assertThat(pointer.getBlobstoreFilepath()).startsWith("wal/");
      assertThat(pointer.getDocCount()).isEqualTo(1);
      assertThat(pointer.getCompressionType()).isEqualTo("gzip");

      // Verify the Kafka message key is null (partition-based routing)
      assertThat(record.key()).isNull();
    }
    kafkaConsumer.close();
  }

  @Test
  public void testMultipleIndexesSamePartition() throws Exception {
    // Create additional dataset that maps to the same partition
    DatasetMetadata dataset2 =
        new DatasetMetadata(
            "secondindex_same_partition",
            "owner",
            1,
            List.of(
                new DatasetPartitionMetadata(
                    1, Long.MAX_VALUE, List.of("1"))), // Same partition as INDEX_NAME
            "secondindex_same_partition");
    datasetMetadataStore.createSync(dataset2);

    // Create spans for multiple indexes that will map to the same partition
    Trace.Span span1 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("span1")).build();
    Trace.Span span2 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("span2")).build();
    Trace.Span span3 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("span3")).build();

    // Both indexes map to partition 1
    Map<String, List<Trace.Span>> indexDocs =
        Map.of(INDEX_NAME, List.of(span1, span2), "secondindex_same_partition", List.of(span3));

    BulkIngestRequest request = bulkIngestS3Producer.submitRequest(indexDocs);
    AtomicReference<BulkIngestResponse> response = new AtomicReference<>();

    Thread.ofVirtual()
        .start(
            () -> {
              try {
                response.set(request.getResponse());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    await().until(() -> response.get() != null);

    // Verify response includes all spans
    assertThat(response.get().totalDocs()).isEqualTo(3);
    assertThat(response.get().failedDocs()).isEqualTo(0);

    // Verify only 1 S3 upload (all indexes grouped into one partition)
    verify(mockBlobStore).upload(any(String.class), any(byte[].class));

    // Verify metrics show single upload with all spans
    assertThat(MetricsUtil.getCount("bulk_ingest_producer_s3_wal_uploads_total", meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount("bulk_ingest_producer_s3_wal_spans_uploaded_total", meterRegistry))
        .isEqualTo(3);

    // Verify single Kafka message with total span count
    KafkaConsumer<String, byte[]> kafkaConsumer = getTestKafkaConsumer();
    ConsumerRecords<String, byte[]> records =
        kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

    assertThat(records.count()).isEqualTo(1);

    for (ConsumerRecord<String, byte[]> record : records) {
      WalProtos.WalSegmentPointer pointer = WalProtos.WalSegmentPointer.parseFrom(record.value());
      assertThat(pointer.getBlobBucket()).isEqualTo(TEST_S3_BUCKET);
      assertThat(pointer.getBlobstoreFilepath()).startsWith("wal/");
      assertThat(pointer.getDocCount()).isEqualTo(3); // Total spans across all indexes
      assertThat(pointer.getCompressionType()).isEqualTo("gzip");
    }
    kafkaConsumer.close();
  }

  @Test
  public void testMultipleIndexesDifferentPartitions() throws Exception {
    // Create additional dataset for different partition
    DatasetMetadata dataset2 =
        new DatasetMetadata(
            "secondindex",
            "owner",
            1,
            List.of(
                new DatasetPartitionMetadata(
                    1, Long.MAX_VALUE, List.of("2"))), // Different partition
            "secondindex");
    datasetMetadataStore.createSync(dataset2);

    Trace.Span span1 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("span1")).build();
    Trace.Span span2 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("span2")).build();

    BulkIngestRequest request1 =
        bulkIngestS3Producer.submitRequest(Map.of(INDEX_NAME, List.of(span1)));
    BulkIngestRequest request2 =
        bulkIngestS3Producer.submitRequest(Map.of("secondindex", List.of(span2)));

    Map<BulkIngestRequest, BulkIngestResponse> responseMap = new ConcurrentHashMap<>();
    CountDownLatch latch = new CountDownLatch(2);

    for (BulkIngestRequest req : List.of(request1, request2)) {
      Thread.ofVirtual()
          .start(
              () -> {
                try {
                  responseMap.put(req, req.getResponse());
                  latch.countDown();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
    }
    latch.await();

    // Verify responses
    assertThat(responseMap).hasSize(2);
    List<BulkIngestResponse> responses = new ArrayList<>(responseMap.values());
    int totalSpans = responses.stream().mapToInt(BulkIngestResponse::totalDocs).sum();
    assertThat(totalSpans).isEqualTo(2);
    assertThat(responses.stream().allMatch(r -> r.failedDocs() == 0)).isTrue();

    // Verify 2 S3 uploads (one per partition)
    verify(mockBlobStore, times(2)).upload(any(String.class), any(byte[].class));

    // Verify metrics show 2 uploads with 1 span each
    assertThat(MetricsUtil.getCount("bulk_ingest_producer_s3_wal_uploads_total", meterRegistry))
        .isEqualTo(2);
    assertThat(
            MetricsUtil.getCount("bulk_ingest_producer_s3_wal_spans_uploaded_total", meterRegistry))
        .isEqualTo(2);

    // Verify 2 Kafka messages (one per partition)
    KafkaConsumer<String, byte[]> kafkaConsumer = getTestKafkaConsumer();
    ConsumerRecords<String, byte[]> records =
        kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

    assertThat(records.count()).isEqualTo(2);

    // Verify each message has 1 span
    for (ConsumerRecord<String, byte[]> record : records) {
      WalProtos.WalSegmentPointer pointer = WalProtos.WalSegmentPointer.parseFrom(record.value());
      assertThat(pointer.getBlobBucket()).isEqualTo(TEST_S3_BUCKET);
      assertThat(pointer.getBlobstoreFilepath()).startsWith("wal/");
      assertThat(pointer.getDocCount()).isEqualTo(1); // 1 span per partition
      assertThat(pointer.getCompressionType()).isEqualTo("gzip");
    }
    kafkaConsumer.close();
  }

  @Test
  public void testMultipleRequestsSamePartitionBatching() throws Exception {
    // Create 3 separate requests that all go to the same partition
    Trace.Span span1 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("req1_span1")).build();
    Trace.Span span2 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("req1_span2")).build();
    Trace.Span span3 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("req2_span1")).build();
    Trace.Span span4 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("req3_span1")).build();

    // Create 3 separate requests all using the same index (same partition)
    BulkIngestRequest request1 =
        bulkIngestS3Producer.submitRequest(Map.of(INDEX_NAME, List.of(span1, span2)));
    BulkIngestRequest request2 =
        bulkIngestS3Producer.submitRequest(Map.of(INDEX_NAME, List.of(span3)));
    BulkIngestRequest request3 =
        bulkIngestS3Producer.submitRequest(Map.of(INDEX_NAME, List.of(span4)));

    // Collect all responses
    Map<BulkIngestRequest, BulkIngestResponse> responseMap = new ConcurrentHashMap<>();
    CountDownLatch latch = new CountDownLatch(3);

    // Submit all requests concurrently
    for (BulkIngestRequest req : List.of(request1, request2, request3)) {
      Thread.ofVirtual()
          .start(
              () -> {
                try {
                  responseMap.put(req, req.getResponse());
                  latch.countDown();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
    }

    latch.await();

    // Verify individual responses are correct
    assertThat(responseMap).hasSize(3);

    // Verify specific request responses (order-independent)
    assertThat(responseMap.get(request1).totalDocs()).isEqualTo(2); // request1: 2 spans
    assertThat(responseMap.get(request2).totalDocs()).isEqualTo(1); // request2: 1 span
    assertThat(responseMap.get(request3).totalDocs()).isEqualTo(1); // request3: 1 span
    assertThat(responseMap.values().stream().allMatch(r -> r.failedDocs() == 0)).isTrue();

    // KEY TEST: Verify only 1 S3 upload happened (batching!)
    verify(mockBlobStore, times(1)).upload(any(String.class), any(byte[].class));

    // Verify only 1 Kafka message with total span count
    KafkaConsumer<String, byte[]> kafkaConsumer = getTestKafkaConsumer();
    ConsumerRecords<String, byte[]> records =
        kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

    assertThat(records.count()).isEqualTo(1); // Single Kafka message

    for (ConsumerRecord<String, byte[]> record : records) {
      WalProtos.WalSegmentPointer pointer = WalProtos.WalSegmentPointer.parseFrom(record.value());
      assertThat(pointer.getDocCount()).isEqualTo(4); // Total spans from all 3 requests
    }
    kafkaConsumer.close();
  }

  private KafkaConsumer<String, byte[]> getTestKafkaConsumer() throws Exception {

    Properties properties = kafkaServer.getBroker().consumerConfig();
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
    properties.put("isolation.level", "read_committed");
    KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
    kafkaConsumer.subscribe(List.of(DOWNSTREAM_TOPIC));

    return kafkaConsumer;
  }
}
