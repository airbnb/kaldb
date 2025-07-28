package com.slack.astra.writer;

import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.testlib.ChunkManagerUtil.makeChunkManagerUtil;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static com.slack.astra.testlib.SpanUtil.makeSpan;
import static com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension.MAX_TIME;
import static com.slack.astra.util.AggregatorFactoriesUtil.createGenericDateHistogramAggregatorFactoriesBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.bulkIngestApi.WALBatchSerializer;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.SearchQuery;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.proto.wal.WalProtos;
import com.slack.astra.testlib.AstraConfigUtil;
import com.slack.astra.testlib.ChunkManagerUtil;
import com.slack.astra.util.QueryBuilderUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class S3MessageWriterImplTest {
  private static final String S3_TEST_BUCKET = "test-astra-logs";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  private ChunkManagerUtil<LogMessage> chunkManagerUtil;
  private SimpleMeterRegistry metricsRegistry;

  @Mock private BlobStore mockBlobStore;

  private AutoCloseable mockitoCloseable;

  @BeforeEach
  public void setUp() throws Exception {
    mockitoCloseable = MockitoAnnotations.openMocks(this);
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        makeChunkManagerUtil(
            S3_MOCK_EXTENSION,
            S3_TEST_BUCKET,
            metricsRegistry,
            10 * 1024 * 1024 * 1024L,
            100,
            AstraConfigUtil.makeIndexerConfig());
    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (chunkManagerUtil != null) {
      chunkManagerUtil.close();
    }
    metricsRegistry.close();
    if (mockitoCloseable != null) {
      mockitoCloseable.close();
    }
  }

  private SearchResult<LogMessage> searchChunkManager(String indexName, String queryString)
      throws IOException {
    return chunkManagerUtil.chunkManager.query(
        new SearchQuery(
            indexName,
            0L,
            MAX_TIME,
            10,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder(queryString, 0L, MAX_TIME),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder()),
        Duration.ofMillis(3000));
  }

  @Test
  public void testS3MessageWriterBasicFlow() throws IOException {
    final String traceId = "t1";
    final String id = "i1";
    final String parentId = "p2";
    final Instant timestamp = Instant.now();
    final long durationMicros = 500000L;
    final String serviceName = "test_service";
    final String name = "testSpanName";
    final String msgType = "test_message_type";
    final Trace.Span span =
        makeSpan(
            traceId,
            id,
            parentId,
            TimeUnit.MICROSECONDS.convert(timestamp.toEpochMilli(), TimeUnit.MILLISECONDS),
            durationMicros,
            name,
            serviceName,
            msgType);

    // Create test data with one index and one span
    Map<String, List<Trace.Span>> indexDocs = new HashMap<>();
    indexDocs.put(serviceName, Collections.singletonList(span));

    // Create S3 pointer record
    ConsumerRecord<String, byte[]> s3Record = consumerRecordWithS3Pointer(indexDocs);

    // Create S3MessageWriter and process record
    S3MessageWriterImpl messageWriter =
        new S3MessageWriterImpl(chunkManagerUtil.chunkManager, mockBlobStore, metricsRegistry);

    assertThat(messageWriter.insertRecord(s3Record)).isTrue();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManagerUtil.chunkManager.getActiveChunk().commit();

    // Verify the span was indexed correctly
    assertThat(searchChunkManager(serviceName, "").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "http_method:POST").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "type:test_message_type").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "service_name:test_service").hits.size())
        .isEqualTo(1);
  }

  @Test
  public void insertNullRecord() throws IOException {
    S3MessageWriterImpl messageWriter =
        new S3MessageWriterImpl(chunkManagerUtil.chunkManager, mockBlobStore, metricsRegistry);
    assertThat(messageWriter.insertRecord(null)).isFalse();
  }

  // Helper method to create S3 WAL ConsumerRecord
  public ConsumerRecord<String, byte[]> consumerRecordWithS3Pointer(
      Map<String, List<Trace.Span>> indexDocs) throws IOException {

    byte[] compressedData = WALBatchSerializer.serialize(indexDocs);

    int partition = 1;

    String objectKey = generateS3ObjectKeyLikeProducer(partition);

    // Create pointer exactly like S3Producer does
    WalProtos.WalSegmentPointer pointer =
        WalProtos.WalSegmentPointer.newBuilder()
            .setBlobBucket(S3_TEST_BUCKET)
            .setBlobstoreFilepath(objectKey)
            .setDocCount(indexDocs.values().stream().mapToInt(List::size).sum())
            .setTimestampMs(Instant.now().toEpochMilli())
            .setCompressionType("gzip")
            .build();

    // Mock S3 download with realistic key
    when(mockBlobStore.downloadWalBatch(objectKey)).thenReturn(compressedData);

    return new ConsumerRecord<>(
        "testTopic",
        partition,
        10,
        0L,
        TimestampType.CREATE_TIME,
        0L,
        0,
        0,
        "testKey",
        pointer.toByteArray());
  }

  private String generateS3ObjectKeyLikeProducer(int partition) {
    long timestampMillis = System.currentTimeMillis();
    Instant now = Instant.ofEpochMilli(timestampMillis);
    return String.format(
        "wal/%d/%02d/%02d/%02d/partition-%d-%d-test.gz",
        now.atZone(java.time.ZoneOffset.UTC).getYear(),
        now.atZone(java.time.ZoneOffset.UTC).getMonthValue(),
        now.atZone(java.time.ZoneOffset.UTC).getDayOfMonth(),
        now.atZone(java.time.ZoneOffset.UTC).getHour(),
        partition,
        timestampMillis);
  }
}
