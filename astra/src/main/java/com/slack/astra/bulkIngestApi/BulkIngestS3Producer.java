package com.slack.astra.bulkIngestApi;

import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import static com.google.common.base.Preconditions.checkArgument;

public class BulkIngestS3Producer extends BulkIngestProducer {

    private static final Logger LOG = LoggerFactory.getLogger(BulkIngestS3Producer.class);
    protected final String walBucket;
    protected final String kafkaTopic;

    public BulkIngestS3Producer(
            final DatasetMetadataStore datasetMetadataStore,
            final AstraConfigs.PreprocessorConfig preprocessorConfig,
            final MeterRegistry meterRegistry,
            S3AsyncClient s3Client) {

        super(datasetMetadataStore, preprocessorConfig, meterRegistry, s3Client);

        // Initialize S3Producer specific fields
        this.walBucket = preprocessorConfig.getS3Config().getS3Bucket();
        this.kafkaTopic = preprocessorConfig.getKafkaConfig().getKafkaTopic();
    }
    @Override
    protected Map<BulkIngestRequest, BulkIngestResponse> produceDocuments(List<BulkIngestRequest> requests){

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

        //Serialize and compress
        byte[] compressedData = serializeAndCompress(indexDocs);

        //Create object key
        String objectKey = String.format("wal/batch-%d-%s.gz", Instant.now().toEpochMilli(), UUID.randomUUID());

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
                totalDocs, compressedData.length, objectKey);

        for (Map.Entry<String, List<Trace.Span>> indexDoc : indexDocs.entrySet()) {
            String index = indexDoc.getKey();
            List<Trace.Span> spans = indexDoc.getValue();
            int partition = getPartition(index);

            if (partition < 0) {
                LOG.warn("index=" + index + " does not have a provisioned dataset associated with it");
                continue; // Skip this index if no partition is found
            }
            //prepare pointer message
            String pointerJson = String.format(
                    "{\"s3Bucket\": \"%s\", \"s3Key\": \"%s\", \"docCount\": %d}", walBucket, objectKey, spans.size());

            byte[] pointerBytes = pointerJson.getBytes(java.nio.charset.StandardCharsets.UTF_8);

            ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>
                    (kafkaTopic, partition, index, pointerBytes);

            try {
                RecordMetadata recordMetadata = this.kafkaProducer.send(producerRecord).get();
                LOG.debug("Sent WAL pointer for index {} to Kafka topic {} partition {} offset {}",
                        index, kafkaTopic, recordMetadata.partition(), recordMetadata.offset());

            } catch (Exception e) {
                LOG.error("Failed to send WAL pointer for index {} to Kafka - deleting S3 object {}", index, objectKey, e);
                DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                        .bucket(walBucket)
                        .key(objectKey)
                        .build();
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