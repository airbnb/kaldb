package com.slack.astra.bulkIngestApi;

import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.wal.WalProtos;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkIngestS3Producer extends BulkIngestProducer {

  private static final Logger LOG = LoggerFactory.getLogger(BulkIngestS3Producer.class);
  protected final String kafkaTopic;

  // Metric name constants
  public static final String S3_UPLOAD_COUNTER = "bulk_ingest_producer_s3_wal_uploads_total";
  public static final String S3_SPANS_UPLOADED_COUNTER =
      "bulk_ingest_producer_s3_wal_spans_uploaded_total";
  public static final String S3_UPLOAD_TIMER = "bulk_ingest_producer_s3_wal_upload_duration";
  public static final String S3_BYTES_UPLOADED_COUNTER =
      "bulk_ingest_producer_s3_wal_bytes_uploaded_total";

  private final BlobStore blobStore;
  protected final String walBucket;
  private final Counter s3UploadCounter;
  private final Counter s3SpansUploadedCounter;
  private final Timer s3UploadTimer;
  private final Counter s3BytesUploadedCounter;

  public BulkIngestS3Producer(
      final DatasetMetadataStore datasetMetadataStore,
      final AstraConfigs.PreprocessorConfig preprocessorConfig,
      final MeterRegistry meterRegistry,
      BlobStore blobStore) {

    super(datasetMetadataStore, preprocessorConfig, meterRegistry);

    // Initialize S3Producer specific fields
    this.blobStore = blobStore;
    this.walBucket = preprocessorConfig.getS3WalConfig().getS3Bucket();
    this.kafkaTopic = preprocessorConfig.getKafkaConfig().getKafkaTopic();
    this.s3UploadCounter = meterRegistry.counter(S3_UPLOAD_COUNTER);
    this.s3SpansUploadedCounter = meterRegistry.counter(S3_SPANS_UPLOADED_COUNTER);
    this.s3UploadTimer = meterRegistry.timer(S3_UPLOAD_TIMER);
    this.s3BytesUploadedCounter = meterRegistry.counter(S3_BYTES_UPLOADED_COUNTER);
  }

  @Override
  protected Map<BulkIngestRequest, BulkIngestResponse> produceDocuments(
      List<BulkIngestRequest> requests) {

    Map<BulkIngestRequest, BulkIngestResponse> responseMap = new HashMap<>();
    try {
      for (BulkIngestRequest request : requests) {
        responseMap.put(request, processRequest(request));
      }
      for (Map.Entry<BulkIngestRequest, BulkIngestResponse> entry : responseMap.entrySet()) {
        BulkIngestRequest key = entry.getKey();
        BulkIngestResponse value = entry.getValue();
        if (!key.setResponse(value)) {
          LOG.warn("Failed to add result to the bulk ingest request, consumer thread went away?");
          failedSetResponseCounter.increment();
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to write batch to S3/kafka", e);
      for (BulkIngestRequest request : requests) {
        responseMap.put(
            request,
            new BulkIngestResponse(
                0,
                request.getInputDocs().values().stream().mapToInt(List::size).sum(),
                e.getMessage()));
      }
    }
    return responseMap;
  }

  protected BulkIngestResponse processRequest(BulkIngestRequest request) throws Exception {

    Map<String, List<Trace.Span>> indexDocs = request.getInputDocs();
    int totalDocs = indexDocs.values().stream().mapToInt(List::size).sum();

    if (totalDocs == 0) {
      // No documents to process
      return new BulkIngestResponse(0, 0, "");
    }

    Map<Integer, Map<String, List<Trace.Span>>> partitionGroups = new HashMap<>();

    for (Map.Entry<String, List<Trace.Span>> indexDoc : indexDocs.entrySet()) {
      String index = indexDoc.getKey();
      List<Trace.Span> spans = indexDoc.getValue();
      int partition = getPartition(index);

      if (partition < 0) {
        LOG.warn("index=" + index + " does not have a provisioned dataset associated with it");
        continue; // Skip this index if no partition is found
      }

      partitionGroups.computeIfAbsent(partition, k -> new HashMap<>()).put(index, spans);
    }

    // Create one S3 object per partition
    for (Map.Entry<Integer, Map<String, List<Trace.Span>>> partitionGroup :
        partitionGroups.entrySet()) {

      int partition = partitionGroup.getKey();
      Map<String, List<Trace.Span>> indexesForPartition = partitionGroup.getValue();
      int docsInPartition = indexesForPartition.values().stream().mapToInt(List::size).sum();

      byte[] compressedData = WALBatchSerializer.serializeAndCompress(indexesForPartition);

      String objectKey = generateS3ObjectKey(partition);

      // put req then upload object to S3
      Timer.Sample uploadTimer = Timer.start(meterRegistry);
      try {

        // upload to S3
        blobStore.uploadWalBatch(objectKey, compressedData);

        LOG.debug(
            "Uploaded {} spans ({} bytes compressed) to S3 at key {} for partition {}",
            docsInPartition,
            compressedData.length,
            objectKey,
            partition);

      } catch (Exception e) {
        LOG.error("Failed to upload to S3", e);
        return new BulkIngestResponse(0, totalDocs, "S3 upload failed: " + e.getMessage());
      } finally {
        uploadTimer.stop(s3UploadTimer);
      }

      // prepare pointer message
      WalProtos.WalSegmentPointer pointer =
          WalProtos.WalSegmentPointer.newBuilder()
              .setBlobBucket(walBucket)
              .setBlobstoreFilepath(objectKey)
              .setDocCount(docsInPartition)
              .setTimestampMs(Instant.now().toEpochMilli())
              .setCompressionType("gzip")
              .build();

      byte[] pointerBytes = pointer.toByteArray();

      ProducerRecord<String, byte[]> producerRecord =
          new ProducerRecord<>(kafkaTopic, partition, null, pointerBytes);

      try {
        RecordMetadata recordMetadata = this.kafkaProducer.send(producerRecord).get();
        LOG.debug(
            "Sent WAL pointer for partition {} to Kafka topic {} partition {} offset {}",
            partition,
            kafkaTopic,
            recordMetadata.partition(),
            recordMetadata.offset());

      } catch (Exception e) {
        LOG.error(
            "Failed to send WAL pointer for partition {} to Kafka - deleting S3 object {}",
            partition,
            objectKey,
            e);
        return new BulkIngestResponse(
            0, totalDocs, "Failed to send WAL pointer to Kafka: " + e.getMessage());
      }
      // Increment metrics
      s3UploadCounter.increment();
      s3SpansUploadedCounter.increment(docsInPartition);
      s3BytesUploadedCounter.increment(compressedData.length);
    }
    return new BulkIngestResponse(totalDocs, 0, "Success");
  }

  // generate a key based on the current timestamp and a UUID.
  private String generateS3ObjectKey(int partition) {
    long timestampMillis = System.currentTimeMillis();
    Instant now = Instant.ofEpochMilli(timestampMillis);
    // Create hour based directory structure
    return String.format(
        "wal/%d/%02d/%02d/%02d/partition-%d-%d-%s.gz",
        now.atZone(ZoneOffset.UTC).getYear(),
        now.atZone(ZoneOffset.UTC).getMonthValue(),
        now.atZone(ZoneOffset.UTC).getDayOfMonth(),
        now.atZone(ZoneOffset.UTC).getHour(),
        partition,
        timestampMillis,
        UUID.randomUUID().toString().substring(0, 8));
  }
}
