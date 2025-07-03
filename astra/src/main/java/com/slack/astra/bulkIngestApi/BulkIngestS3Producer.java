package com.slack.astra.bulkIngestApi;

import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import static com.google.common.base.Preconditions.checkArgument;

public class BulkIngestS3Producer extends BulkIngestProducer {
    private static final Logger LOG = LoggerFactory.getLogger(BulkIngestS3Producer.class);

    private final KafkaProducer<String, byte[]> kafkaProducer;

    protected final String walBucket;
    protected final String kafkaTopic;

    public BulkIngestS3Producer(
            final DatasetMetadataStore datasetMetadataStore,
            final AstraConfigs.PreprocessorConfig preprocessorConfig,
            final MeterRegistry meterRegistry,
            S3AsyncClient s3Client,
            KafkaProducer<String, byte[]> kafkaProducer) {
        super(datasetMetadataStore, preprocessorConfig, meterRegistry, s3Client);

        // Initialize S3Producer specific fields
        this.kafkaProducer = kafkaProducer;
        this.walBucket = preprocessorConfig.getS3Config().getS3Bucket();
        this.kafkaTopic = preprocessorConfig.getKafkaConfig().getKafkaTopic();
    }

    //todo - remove this from here and put it in the producer class
    @Override
    protected void run() throws Exception {
        while (isRunning()) {
            List<BulkIngestRequest> batch = new ArrayList<>();
            pendingRequests.drainTo(batch);

            if (batch.isEmpty()) {
                try {
                    stallCounter.increment();
                    Thread.sleep(producerSleepMs);
                } catch (InterruptedException e) {
                    return; // Exit if interrupted
                }
            } else {
                for (BulkIngestRequest req : batch) {
                    BulkIngestResponse resp;
                    try {
                        resp = processRequest(req);
                    } catch (Exception e) {
                        LOG.error("WAL batch processing failed", e);
                        int failedDocs = req.getInputDocs().values().stream().mapToInt(List::size).sum();
                        resp = new BulkIngestResponse(0, failedDocs, "Error: " + e.getMessage());
                    }
                    // Set the response (unblocks BulkIngestApi thread waiting on getResponse()):
                    if (!req.setResponse(resp)) {
                        LOG.warn("Failed to deliver response to BulkIngestRequest (Possibly timed out)");
                    }
                }
            }
        }

    }

    protected BulkIngestResponse processRequest(BulkIngestRequest request) throws Exception {
        // Implement the logic to process the
        Map<String, List<Trace.Span>> indexDocs = request.getInputDocs();
        int totalDocs = indexDocs.values().stream().mapToInt(List::size).sum();

        if (totalDocs == 0) {
            // No documents to process
            return new BulkIngestResponse(0, 0, "");
        }

        for (Map.Entry<String, List<Trace.Span>> indexDoc : indexDocs.entrySet()) {
            String index = indexDoc.getKey();
            List<Trace.Span> spans = indexDoc.getValue();
            int partition = getPartition(index);

            if (partition < 0) {
                LOG.warn("index=" + index + " does not have a provisioned dataset associated with it");
            }

            if (indexDocs.isEmpty()) {
                // All docs were for unknown datasets
                return new BulkIngestResponse(0, 0, "No provisioned dataset for index");
            }

            //Serialize and compress
            byte[] compressedData = serializeAndCompress(indexDocs);

            //Create object key
            String objectKey = String.format("%s/%d-%s.gz", index, Instant.now().toEpochMilli(), UUID.randomUUID());

            //put req then upload object to S3

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(walBucket)
                    .key(objectKey)
                    .build();

            s3Client.putObject(
                            putObjectRequest,
                            AsyncRequestBody.fromBytes(compressedData))
                    .get();
            LOG.debug("Uploaded {} spans ({} bytes compressed) to S3 at key {}",
                    spans.size(), compressedData.length, objectKey);

            //prepare pointer message

            String pointerJson = String.format(
                    "{\"s3Bucket\": \"%s\", \"s3Key\": %s, \"docCount\": \"%d\"}", walBucket, objectKey, spans.size());

            //send notification to kafka topic
            for (String idx : indexDocs.keySet()) {
                int idxpartition = getPartition(idx);
                byte[] pointerBytes = pointerJson.getBytes(java.nio.charset.StandardCharsets.UTF_8);

                checkArgument(idxpartition >= 0, "No provisioned dataset for index: " + idx);

                ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>
                        (kafkaConfig.getKafkaTopic(), idxpartition, index, pointerBytes);

                try
                {
                    RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
                    LOG.debug("Sent WAL pointer for index {} to Kafka topic {} partition {} offset {}",
                            idx, kafkaConfig.getKafkaTopic(), recordMetadata.partition(), recordMetadata.offset());

                } catch (Exception e) {
                    LOG.error("Failed to send WAL pointer for index {} to Kafka", idx, e);
                    throw new RuntimeException("Failed to send WAL pointer to Kafka", e);
                }
            }
        }
        return new BulkIngestResponse(0, 0, "Success");
    }


    protected void shutDown() throws Exception {
        super.shutDown();
    }


    //Serializes and compresses a map of spans for efficient storage.

    private byte[] serializeAndCompress(Map<String, List<Trace.Span>> indexDocs) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {

            // Serialize the batch
            for (Map.Entry<String, List<Trace.Span>> entry : indexDocs.entrySet()) {
                String index = entry.getKey();
                writeString(gzipOut, index);

                // Write number of spans
                writeInt(gzipOut, entry.getValue().size());

                // Write each span
                for (Trace.Span span : entry.getValue()) {
                    byte[] spanBytes = span.toByteArray();
                    writeInt(gzipOut, spanBytes.length);
                    gzipOut.write(spanBytes);
                }
            }
            gzipOut.finish();
            return baos.toByteArray();
        }
    }

    //Writes a string to the output stream with its length prefix.

    private void writeString(OutputStream out, String str) throws IOException {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        writeInt(out, bytes.length);
        out.write(bytes);
    }

    //Writes an integer to the output stream as 4 bytes.

    private void writeInt(OutputStream out, int value) throws IOException {
        out.write((value >>> 24) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write(value & 0xFF);
    }
}