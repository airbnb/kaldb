package com.slack.astra.writer;

import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.bulkIngestApi.WALBatchSerializer;
import com.slack.astra.chunkManager.ChunkManager;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.proto.wal.WalProtos;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3MessageWriterImpl implements MessageWriter {
  private static final Logger LOG = LoggerFactory.getLogger(S3MessageWriterImpl.class);

  private final ChunkManager<LogMessage> chunkManager;
  private final BlobStore blobStore;

  private final Counter s3DownloadCounter;
  private final Counter s3DownloadErrorCounter;
  private final Counter spansProcessedCounter;
  private final Counter decompressionErrorCounter;

  public S3MessageWriterImpl(
      ChunkManager<LogMessage> chunkManager, BlobStore blobStore, MeterRegistry meterRegistry) {

    this.chunkManager = chunkManager;
    this.blobStore = blobStore;

    // Initialize metrics
    this.s3DownloadCounter = meterRegistry.counter("s3_message_writer.downloads");
    this.s3DownloadErrorCounter = meterRegistry.counter("s3_message_writer.download_errors");
    this.spansProcessedCounter = meterRegistry.counter("s3_message_writer.spans_processed");
    this.decompressionErrorCounter =
        meterRegistry.counter("s3_message_writer.decompression_errors");
  }

  @Override
  public boolean insertRecord(ConsumerRecord<String, byte[]> record) throws IOException {

    if (record == null) return false;
    try {
      // Deserialize the S3WalPointer from the record value
      WalProtos.WalSegmentPointer pointer = WalProtos.WalSegmentPointer.parseFrom(record.value());
      LOG.debug(
          "Processing WAL segment: bucket={}, key={}, docCount={}",
          pointer.getBlobBucket(),
          pointer.getBlobstoreFilepath(),
          pointer.getDocCount());
      try {
        // Create a GetObjectRequest for the S3 object
        byte[] compressedData = blobStore.downloadWalBatch(pointer.getBlobstoreFilepath());
        s3DownloadCounter.increment();

        // Deserialize and decompress to get all indexes and their spans
        Map<String, List<Trace.Span>> indexDocs =
            WALBatchSerializer.deserializeAndDecompress(compressedData);

        boolean allSuccessful = true;
        int totalSpansProcessed = 0;

        for (Map.Entry<String, List<Trace.Span>> entry : indexDocs.entrySet()) {

          String index = entry.getKey();
          List<Trace.Span> spans = entry.getValue();

          LOG.debug(
              "Processing {} spans for index {} from partition {}",
              spans.size(),
              index,
              record.partition());

          for (Trace.Span span : spans) {
            try {
              chunkManager.addMessage(
                  span,
                  span.getSerializedSize(),
                  String.valueOf(record.partition()),
                  record.offset());
              totalSpansProcessed++;
            } catch (Exception e) {
              LOG.error("Failed to add span to chunk manager", e);
              allSuccessful = false;
            }
          }
        }
        spansProcessedCounter.increment(totalSpansProcessed);

        LOG.debug(
            "Successfully processed {} spans from S3 object for partition {}",
            totalSpansProcessed,
            record.partition());
        return allSuccessful;
      } catch (Exception e) {
        LOG.error(
            "Failed to process S3 batch: bucket={}, key={}",
            pointer.getBlobBucket(),
            pointer.getBlobstoreFilepath(),
            e);
        decompressionErrorCounter.increment();
        throw new IOException("S3 batch processing failed", e);
      }
    } catch (Exception e) {
      LOG.error("Failed to process S3 WAL pointer message", e);
      return false;
    }
  }
}
