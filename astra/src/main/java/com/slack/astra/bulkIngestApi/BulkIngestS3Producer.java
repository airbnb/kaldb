package com.slack.astra.bulkIngestApi;

import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.wal.WalProtos;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
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
  public static final String S3_UPLOAD_FAILURES_COUNTER =
      "bulk_ingest_producer_s3_wal_upload_failures_total";
  public static final String KAFKA_POINTER_FAILURES_COUNTER =
      "bulk_ingest_producer_s3_wal_kafka_failures_total";
  public static final String STOP_INGESTION_COUNTER =
      "bulk_ingest_producer_s3_wal_stop_ingestion_total";
  public static final String BATCH_WAIT_TIMER = "bulk_ingest_producer_s3_wal_batch_wait_duration";
  private final BlobStore blobStore;
  protected final String walBucket;
  private final Timer s3UploadTimer;
  private final Timer batchWaitTimer;
  private final int maxBufferTimeMs;
  private final int maxRequestsPerBatch;
  private final int minBatchSize;
  private final int bufferWaitMs;

  public BulkIngestS3Producer(
      final DatasetMetadataStore datasetMetadataStore,
      final AstraConfigs.PreprocessorConfig preprocessorConfig,
      final MeterRegistry meterRegistry,
      BlobStore blobStore) {

    super(datasetMetadataStore, preprocessorConfig, meterRegistry);

    // Initialize S3Producer specific fields
    AstraConfigs.S3WalBufferConfig s3WalBufferConfig =
        preprocessorConfig.getS3WalConfig().getBufferConfig();
    this.blobStore = blobStore;
    this.walBucket = preprocessorConfig.getS3WalConfig().getS3Config().getS3Bucket();
    this.kafkaTopic = preprocessorConfig.getKafkaConfig().getKafkaTopic();
    this.s3UploadTimer = meterRegistry.timer(S3_UPLOAD_TIMER);
    this.batchWaitTimer = meterRegistry.timer(BATCH_WAIT_TIMER);
    this.producerSleepMs =
        Integer.parseInt(System.getProperty("astra.bulkIngest.S3producerSleepMs", "100"));

    // Maximum time to wait for more requests before processing a batch
    // This is used for time-based batching, allowing enough time to accumulate requests.
    this.maxBufferTimeMs = s3WalBufferConfig.getMaxBufferTimeMs();
    // Maximum number of requests to process in a single batch so that S3 objects are not too large.
    this.maxRequestsPerBatch = s3WalBufferConfig.getMaxRequestsPerBatch();
    // Minimum number of requests to wait for before processing. This allows large enough S3
    // objects.
    this.minBatchSize = Math.max(1, maxRequestsPerBatch / 4);
    // Sleep interval when waiting for more requests to reach minBatchSize before maxBufferTimeMs
    // expires.
    // Set to 1/10th of maxBufferTimeMs (min 10ms) to check frequently without excessive CPU usage.
    this.bufferWaitMs = Math.max(10, maxBufferTimeMs / 10);
  }

  @Override
  protected void run() throws Exception {

    long lastProcessTime = System.currentTimeMillis();
    Timer.Sample batchWaitSample = Timer.start(meterRegistry);

    while (isRunning()) {

      // Check queue size first, don't drain if not ready
      int availableRequests = pendingRequests.size();
      if (availableRequests == 0) {
        // No requests available, sleep and continue
        try {
          stallCounter.increment();
          Thread.sleep(producerSleepMs);
        } catch (InterruptedException e) {
          return;
        }
        continue;
      }

      // Check if we should wait for more requests
      if (shouldWaitForMoreRequests(availableRequests, lastProcessTime)) {
        // Don't drain anything, just sleep and wait for more
        Thread.sleep(bufferWaitMs);
        continue;
      }
      List<BulkIngestRequest> requests = new ArrayList<>(maxRequestsPerBatch);
      // drain only up to maxBatch requests from the pending queue
      pendingRequests.drainTo(requests, maxRequestsPerBatch);

      batchSizeGauge.set(requests.size());

      // Process the batch
      batchWaitSample.stop(batchWaitTimer);
      produceDocuments(requests);
      lastProcessTime = System.currentTimeMillis();
      batchWaitSample = Timer.start(meterRegistry);
    }
  }

  @Override
  protected Map<BulkIngestRequest, BulkIngestResponse> produceDocuments(
      List<BulkIngestRequest> requests) {

    Map<BulkIngestRequest, BulkIngestResponse> responseMap = new HashMap<>();
    try {
      // Group all requests by partition
      Map<Integer, Set<BulkIngestRequest>> requestsByPartition =
          aggregateRequestsByPartition(requests);

      // Process all the requests for each partition
      for (Map.Entry<Integer, Set<BulkIngestRequest>> entry : requestsByPartition.entrySet()) {

        int partition = entry.getKey();
        Set<BulkIngestRequest> partitionRequests = entry.getValue();

        processBatchedRequests(partition, partitionRequests, responseMap);
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

  private void processBatchedRequests(
      int partition,
      Set<BulkIngestRequest> requests,
      Map<BulkIngestRequest, BulkIngestResponse> responseMap)
      throws Exception {

    // Combine all spans from all requests for this partition
    Map<String, List<Trace.Span>> combinedIndexDocs = new HashMap<>();
    int totalDocs = 0;

    // Track how many documents each request contributes
    Map<BulkIngestRequest, Integer> requestDocCounts = new HashMap<>();

    // Combine all requests' spans into combinedIndexDocs
    for (BulkIngestRequest request : requests) {
      int requestDocs = 0;
      for (Map.Entry<String, List<Trace.Span>> indexDoc : request.getInputDocs().entrySet()) {
        String index = indexDoc.getKey();
        combinedIndexDocs
            .computeIfAbsent(index, k -> new ArrayList<>())
            .addAll(indexDoc.getValue());
        requestDocs += indexDoc.getValue().size();
      }
      totalDocs += requestDocs;
      requestDocCounts.put(request, requestDocs);
    }
    // Serialize and upload combined data for the partition as a single S3 object
    byte[] serializedData = WALBatchSerializer.serialize(combinedIndexDocs);

    // Upload combined data to S3 and send Kafka pointer for the partition
    String errorMessage = uploadToS3AndSendKafkaPointer(partition, serializedData, totalDocs);

    // If there was an error during upload or Kafka send, all requests in this partition fail and
    // return the error message
    if (errorMessage != null) {
      // Handle failures
      for (BulkIngestRequest request : requests) {
        int requestDocs = requestDocCounts.get(request);
        responseMap.put(request, new BulkIngestResponse(0, requestDocs, errorMessage));
      }
    } else {
      // Handle successes
      for (BulkIngestRequest request : requests) {
        int requestDocs = requestDocCounts.get(request);
        responseMap.put(request, new BulkIngestResponse(requestDocs, 0, "Success"));
      }
    }
  }

  private boolean shouldWaitForMoreRequests(int availableRequests, long lastProcessTime) {

    // Time-based: Don't wait if max buffer time exceeded
    long timeSinceLastProcess = System.currentTimeMillis() - lastProcessTime;
    if (timeSinceLastProcess >= maxBufferTimeMs) {
      return false; // Process now, time limit reached
    }

    // Count-based: Don't wait if we have enough requests
    if (availableRequests > minBatchSize) {
      return false; // Process now, minimum batch size reached
    }

    return true; // Wait for more requests, either time or count not met
  }

  private Map<Integer, Set<BulkIngestRequest>> aggregateRequestsByPartition(
      List<BulkIngestRequest> requests) {

    Map<Integer, Set<BulkIngestRequest>> requestsByPartition = new HashMap<>();

    // Iterate through all requests and group them by partition
    for (BulkIngestRequest request : requests) {

      Map<String, List<Trace.Span>> indexDocs = request.getInputDocs();

      // Find the partition for each index in the request (Current constraint allows only 1 index
      // per request)
      for (Map.Entry<String, List<Trace.Span>> indexDoc : indexDocs.entrySet()) {
        String index = indexDoc.getKey();
        int partition = getPartition(index);

        if (partition < 0) {
          LOG.warn("index=" + index + " does not have a provisioned dataset associated with it");
          continue; // Skip this index if no partition is found
        }
        // Add the request to the list of its partition
        requestsByPartition.computeIfAbsent(partition, k -> new HashSet<>()).add(request);
      }
    }
    return requestsByPartition;
  }

  private String uploadToS3AndSendKafkaPointer(
      int partition, byte[] compressedData, int totalDocs) {

    String objectKey = generateS3ObjectKey(partition);

    // Put req then upload object to S3
    Timer.Sample uploadTimer = Timer.start(meterRegistry);
    try {
      // upload to S3 using blobstore
      blobStore.upload(objectKey, compressedData);

      LOG.debug(
          "Uploaded {} spans ({} bytes compressed) to S3 at key {} for partition {}",
          totalDocs,
          compressedData.length,
          objectKey,
          partition);

    } catch (Exception e) {
      LOG.error("Fatal: Failed to upload to S3 - stopping ingestion", e);
      updateFailureMetrics(partition, S3_UPLOAD_FAILURES_COUNTER);
      return "S3 upload failed: " + e.getMessage();
    } finally {
      uploadTimer.stop(s3UploadTimer);
    }

    // prepare pointer message for Kafka
    WalProtos.WalSegmentPointer pointer =
        WalProtos.WalSegmentPointer.newBuilder()
            .setBlobBucket(walBucket)
            .setBlobstoreFilepath(objectKey)
            .setDocCount(totalDocs)
            .setTimestampMs(Instant.now().toEpochMilli())
            .setCompressionType("gzip")
            .build();

    byte[] pointerBytes = pointer.toByteArray();

    ProducerRecord<String, byte[]> producerRecord =
        new ProducerRecord<>(kafkaTopic, partition, null, pointerBytes);

    try {
      // send the record to Kafka
      this.kafkaProducer.send(producerRecord);
      LOG.debug("Sent WAL pointer for partition {} to Kafka topic {}", partition, kafkaTopic);

    } catch (Exception e) {
      LOG.error(
          "Failed to send WAL pointer for partition {} to Kafka - stopping ingestion {}",
          partition,
          objectKey,
          e);
      updateFailureMetrics(partition, KAFKA_POINTER_FAILURES_COUNTER);
      return "Failed to send WAL pointer to Kafka: " + e.getMessage();
    }
    // Increment metrics
    updateMetricsForPartition(partition, totalDocs, compressedData.length);

    return null; // successful upload and Kafka send, return null to indicate no error
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

  private void updateMetricsForPartition(int partition, int docsInPartition, int bytesUploaded) {
    meterRegistry.counter(S3_UPLOAD_COUNTER, "partition", String.valueOf(partition)).increment();
    meterRegistry
        .counter(S3_SPANS_UPLOADED_COUNTER, "partition", String.valueOf(partition))
        .increment(docsInPartition);
    meterRegistry
        .counter(S3_BYTES_UPLOADED_COUNTER, "partition", String.valueOf(partition))
        .increment(bytesUploaded);
  }

  private void updateFailureMetrics(int partition, String failureType) {

    meterRegistry.counter(failureType, "partition", String.valueOf(partition)).increment();
    meterRegistry
        .counter(STOP_INGESTION_COUNTER, "partition", String.valueOf(partition))
        .increment();
  }
}
