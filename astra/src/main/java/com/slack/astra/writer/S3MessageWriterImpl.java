package com.slack.astra.writer;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class S3MessageWriterImpl implements MessageWriter {
    private static final Logger LOG = LoggerFactory.getLogger(S3MessageWriterImpl.class);

    private final ChunkManager<LogMessage> chunkManager;
    private final S3AsyncClient s3Client;

    private final Counter s3DownloadCounter;
    private final Counter s3DownloadErrorCounter;
    private final Counter spansProcessedCounter;
    private final Counter decompressionErrorCounter;

    public S3MessageWriterImpl(
            ChunkManager<LogMessage> chunkManager,
            S3AsyncClient s3Client,
            MeterRegistry meterRegistry) {

        this.chunkManager = chunkManager;
        this.s3Client = s3Client;

        // Initialize metrics
        this.s3DownloadCounter = meterRegistry.counter("s3_message_writer.downloads");
        this.s3DownloadErrorCounter = meterRegistry.counter("s3_message_writer.download_errors");
        this.spansProcessedCounter = meterRegistry.counter("s3_message_writer.spans_processed");
        this.decompressionErrorCounter = meterRegistry.counter("s3_message_writer.decompression_errors");

    }

    @Override
    public boolean insertRecord(ConsumerRecord<String, byte[]> record) throws IOException {

        if (record == null) return false;
        try {
            //Deserialize the S3WalPointer from the record value
            WalProtos.S3WalPointer pointer = WalProtos.S3WalPointer.parseFrom(record.value());
            try {
                // Create a GetObjectRequest for the S3 object
                byte[] compressedData = downloadFromS3(pointer.getS3Bucket(), pointer.getS3Key());

                Map<String, List<Trace.Span>> indexDocs =
                        WALBatchSerializer.deserializeAndDecompress(compressedData);

                String recordIndex = record.key();

                // get spans for this index
                List<Trace.Span> spans = indexDocs.get(recordIndex);

                if (spans == null || spans.isEmpty()) {
                    LOG.warn("No spans found for index {} in S3 batch: bucket={}, key={}",
                            recordIndex, pointer.getS3Bucket(), pointer.getS3Key());
                    return true; //empty data for this partition
                }
                boolean allSuccessful = true;
                for (Trace.Span span : spans) {
                    try {
                        chunkManager.addMessage(
                                span,
                                span.getSerializedSize(),
                                String.valueOf(record.partition()),
                                record.offset());
                    } catch (Exception e) {
                        LOG.error("Failed to add span to chunk manager", e);
                        allSuccessful = false;
                    }
                }
                return allSuccessful;
            } catch (Exception e) {
                LOG.error("Failed to process S3 batch: bucket={}, key={}",
                        pointer.getS3Bucket(), pointer.getS3Key(), e);
                decompressionErrorCounter.increment();
                throw new IOException("S3 batch processing failed", e);
            }
        } catch (Exception e) {
            LOG.error("Failed to process S3 WAL pointer message", e);
            return false;
        }
    }

    private byte[] downloadFromS3(String bucket, String key) throws IOException {
        int maxRetries = 3;
        long backoffMs = 1000;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                try {
                    GetObjectRequest getObjectRequest =
                            GetObjectRequest.builder().bucket(bucket).key(key).build();

                    ResponseBytes<GetObjectResponse> response =
                            s3Client.getObject(getObjectRequest, AsyncResponseTransformer.toBytes()).get();

                    return response.asByteArray();

                } catch (ExecutionException e) {
                    LOG.error("Failed to download S3 object: bucket={}, key={}", bucket, key, e.getCause());
                    throw new IOException("S3 download failed", e.getCause());
                } catch (InterruptedException e) {
                    LOG.error("S3 download interrupted: bucket={}, key={}", bucket, key, e);
                    Thread.currentThread().interrupt(); // Restore interrupted status
                    throw new IOException("S3 download interrupted", e);
                }
            } catch (IOException e) {
                if (attempt == maxRetries) throw e;

                LOG.warn("S3 download attempt {} failed, retrying in {}ms", attempt, backoffMs);
                //Thread.sleep(backoffMs);
                backoffMs *= 2; // Exponential backoff
            }
        }
        return null;
    }
}
