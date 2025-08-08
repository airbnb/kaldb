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

public class S3WalMessageWriterImpl implements MessageWriter {
  private static final Logger LOG = LoggerFactory.getLogger(S3WalMessageWriterImpl.class);

  private final ChunkManager<LogMessage> chunkManager;
  private final BlobStore blobStore;

  private final Counter s3DownloadCounter;
  private final Counter spansProcessedCounter;

  public S3WalMessageWriterImpl(
      ChunkManager<LogMessage> chunkManager, BlobStore blobStore, MeterRegistry meterRegistry) {

    this.chunkManager = chunkManager;
    this.blobStore = blobStore;

    // Initialize metrics
    this.s3DownloadCounter = meterRegistry.counter("s3_message_writer.downloads");
    this.spansProcessedCounter = meterRegistry.counter("s3_message_writer.spans_processed");
  }

  @Override
  public boolean insertRecord(ConsumerRecord<String, byte[]> record) throws IOException {
    if (record == null) return false;

    // Parse S3 pointer (let exceptions propagate like LogMessageWriter)

    WalProtos.WalSegmentPointer pointer = WalProtos.WalSegmentPointer.parseFrom(record.value());

    LOG.debug(
        "Processing WAL segment: bucket={}, key={}, docCount={}",
        pointer.getBlobBucket(),
        pointer.getBlobstoreFilepath(),
        pointer.getDocCount());

    // Download batch from S3
    byte[] serializedData = blobStore.downloadWalBatch(pointer.getBlobstoreFilepath());
    s3DownloadCounter.increment();

    // Deserialize batch
    Map<String, List<Trace.Span>> indexDocs = WALBatchSerializer.deserialize(serializedData);

    int totalSpansProcessed = 0;

    // Process each span
    for (Map.Entry<String, List<Trace.Span>> entry : indexDocs.entrySet()) {
      for (Trace.Span span : entry.getValue()) {

        chunkManager.addMessage(
            span, span.getSerializedSize(), String.valueOf(record.partition()), record.offset());
        totalSpansProcessed++;
      }
    }

    spansProcessedCounter.increment(totalSpansProcessed);

    LOG.debug(
        "Successfully processed {} spans from S3 object for partition {}",
        totalSpansProcessed,
        record.partition());

    return true; // Only reached if ALL spans succeeded
  }
}
