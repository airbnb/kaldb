package com.slack.astra.bulkIngestApi;

import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.wal.WalProtos;
import com.slack.astra.bulkIngestApi.WALBatchSerializer;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.*;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class BulkIngestS3Producer extends BulkIngestProducer {

  private static final Logger LOG = LoggerFactory.getLogger(BulkIngestS3Producer.class);
  protected final String walBucket;
  protected final String kafkaTopic;

  private final Counter s3UploadCounter;
  private final Counter s3SpansUploadedCounter;
  private final Timer s3UploadTimer;
  private final Counter s3BytesUploadedCounter;

  public BulkIngestS3Producer(
      final DatasetMetadataStore datasetMetadataStore,
      final AstraConfigs.PreprocessorConfig preprocessorConfig,
      final MeterRegistry meterRegistry,
      S3AsyncClient s3Client) {

    super(datasetMetadataStore, preprocessorConfig, meterRegistry, s3Client);

    // Initialize S3Producer specific fields
    this.walBucket = preprocessorConfig.getS3Config().getS3Bucket();
    this.kafkaTopic = preprocessorConfig.getKafkaConfig().getKafkaTopic();

    this.s3UploadCounter = meterRegistry.counter("s3_wal_uploads_total");
    this.s3SpansUploadedCounter = meterRegistry.counter("s3_wal_spans_uploaded_total");
    this.s3UploadTimer = meterRegistry.timer("s3_wal_upload_duration");
    this.s3BytesUploadedCounter = meterRegistry.counter("s3_wal_bytes_uploaded_total");
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

    // Serialize and compress
    byte[] compressedData = WALBatchSerializer.serializeAndCompress(indexDocs);

    // Create object key
    String objectKey = generateS3ObjectKey();

    // put req then upload object to S3
    Timer.Sample uploadTimer = Timer.start(meterRegistry);
    try {
      PutObjectRequest putObjectRequest =
          PutObjectRequest.builder().bucket(walBucket).key(objectKey).build();

      s3Client.putObject(putObjectRequest, AsyncRequestBody.fromBytes(compressedData)).get();

      s3UploadCounter.increment();
      s3SpansUploadedCounter.increment(totalDocs);
      s3BytesUploadedCounter.increment(compressedData.length);

      LOG.debug("Uploaded {} spans ({} bytes compressed) to S3 at key {}",
          totalDocs, compressedData.length, objectKey);

    } catch (Exception e) {
      LOG.error("Failed to upload to S3", e);
      throw new RuntimeException("S3 upload failed", e);
    } finally {
      uploadTimer.stop(s3UploadTimer);
    }

    for (Map.Entry<String, List<Trace.Span>> indexDoc : indexDocs.entrySet()) {
      String index = indexDoc.getKey();
      List<Trace.Span> spans = indexDoc.getValue();
      int partition = getPartition(index);

      if (partition < 0) {
        LOG.warn("index=" + index + " does not have a provisioned dataset associated with it");
        continue; // Skip this index if no partition is found
      }
      // prepare pointer message
      WalProtos.S3WalPointer pointer =
          WalProtos.S3WalPointer.newBuilder()
              .setS3Bucket(walBucket)
              .setS3Key(objectKey)
              .setDocCount(spans.size())
              .setTimestampMs(Instant.now().toEpochMilli())
              .setCompressionType("gzip")
              .build();

      byte[] pointerBytes = pointer.toByteArray();

      ProducerRecord<String, byte[]> producerRecord =
          new ProducerRecord<>(kafkaTopic, partition, index, pointerBytes);

      try {
        RecordMetadata recordMetadata = this.kafkaProducer.send(producerRecord).get();
        LOG.debug(
            "Sent WAL pointer for index {} to Kafka topic {} partition {} offset {}",
            index,
            kafkaTopic,
            recordMetadata.partition(),
            recordMetadata.offset());

      } catch (Exception e) {
        LOG.error(
            "Failed to send WAL pointer for index {} to Kafka - deleting S3 object {}",
            index,
            objectKey,
            e);
        DeleteObjectRequest deleteRequest =
            DeleteObjectRequest.builder().bucket(walBucket).key(objectKey).build();
        s3Client.deleteObject(deleteRequest).join();
        throw new RuntimeException("Failed to send WAL pointer to Kafka", e);
      }
    }
    return new BulkIngestResponse(totalDocs, 0, "Success");
  }

  protected void shutDown() throws Exception {
    if (s3Client != null) {
      s3Client.close();
    }
    super.shutDown();
  }

  // generate a key based on the current timestamp and a UUID.
  private String generateS3ObjectKey() {
    Instant now = Instant.now();
    // Create hour based directory structure
    return String.format(
        "wal/%d/%02d/%02d/%02d/batch-%d-%s.gz",
        now.atZone(ZoneOffset.UTC).getYear(),
        now.atZone(ZoneOffset.UTC).getMonthValue(),
        now.atZone(ZoneOffset.UTC).getDayOfMonth(),
        now.atZone(ZoneOffset.UTC).getHour(),
        now.toEpochMilli(),
        UUID.randomUUID().toString().substring(0, 8));
  }
}
