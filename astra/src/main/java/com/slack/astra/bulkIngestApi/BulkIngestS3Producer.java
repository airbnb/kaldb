package com.slack.astra.bulkIngestApi;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.slack.astra.metadata.dataset.DatasetMetadata.MATCH_ALL_SERVICE;
import static com.slack.astra.metadata.dataset.DatasetMetadata.MATCH_STAR_SERVICE;
import static com.slack.astra.server.ManagerApiGrpc.MAX_TIME;

public abstract class BulkIngestS3Producer extends AbstractExecutionThreadService {
    private static final Logger LOG = LoggerFactory.getLogger(BulkIngestS3Producer.class);

    protected final DatasetMetadataStore datasetMetadataStore;
    private final AstraMetadataStoreChangeListener<DatasetMetadata> datasetListener =
            (_) -> cacheSortedDataset();
    protected List<DatasetMetadata> throughputSortedDatasets;

    private final BlockingQueue<BulkIngestRequest> pendingRequests;

    private final Integer producerSleepMs;
    public static final String FAILED_SET_RESPONSE_COUNTER =
            "bulk_ingest_producer_failed_set_response";
    private final Counter failedSetResponseCounter;
    public static final String STALL_COUNTER = "bulk_ingest_producer_stall_counter";
    private final Counter stallCounter;

    private final KafkaProducer<String, byte[]> kafkaProducer;
    private final S3AsyncClient s3Client;
    private String walBucket;
    private final String kafkaTopic;

    public static final String KAFKA_RESTART_COUNTER = "bulk_ingest_producer_kafka_restart_timer";

    private final Timer kafkaRestartTimer;

    public static final String BATCH_SIZE_GAUGE = "bulk_ingest_producer_batch_size";
    private final AtomicInteger batchSizeGauge;

    protected final MeterRegistry meterRegistry;

    protected static final Set<String> OVERRIDABLE_CONFIGS =
            Set.of(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

    public BulkIngestS3Producer(
            final DatasetMetadataStore datasetMetadataStore,
            final AstraConfigs.PreprocessorConfig preprocessorConfig,
            final MeterRegistry meterRegistry,
            S3AsyncClient s3Client,
            KafkaProducer<String, byte[]> kafkaProducer,
            String kafkaTopic) {

        this.meterRegistry = meterRegistry;
        this.datasetMetadataStore = datasetMetadataStore;
        this.pendingRequests = new LinkedBlockingQueue<>();

        this.s3Client = s3Client;
        this.kafkaProducer = kafkaProducer;
        /* this.walBucket =  this.walBucket = preprocessorConfig.hasWalBucket()
                ? preprocessorConfig.getWalBucket()
               : preprocessorConfig.getS3Config().getS3Bucket(); */
        this.kafkaTopic = preprocessorConfig.getKafkaConfig().getKafkaTopic();

        this.producerSleepMs =
                Integer.parseInt(System.getProperty("astra.bulkIngest.producerSleepMs", "50"));

        this.failedSetResponseCounter = meterRegistry.counter(FAILED_SET_RESPONSE_COUNTER);
        this.stallCounter = meterRegistry.counter(STALL_COUNTER);
        this.kafkaRestartTimer = meterRegistry.timer(KAFKA_RESTART_COUNTER);
        this.batchSizeGauge = meterRegistry.gauge(BATCH_SIZE_GAUGE, new AtomicInteger(0));

    }

    public BulkIngestRequest submitRequest(Map<String, List<Trace.Span>> inputDocs) {
        BulkIngestRequest request = new BulkIngestRequest(inputDocs);
        pendingRequests.add(request);
        return request;
    }

    @Override
    protected void startUp() throws Exception { }

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
    @Override
    protected void shutDown() throws Exception {
    datasetMetadataStore.removeListener(datasetListener);
    shutdownProducer(); // Call abstract method
   }

    private BulkIngestResponse processRequest(BulkIngestRequest request) throws Exception {
        // Implement the logic to process the
        Map<String, List<Trace.Span>> indexDocs = request.getInputDocs();
        int totalDocs = indexDocs.values().stream().mapToInt(List::size).sum();

        if (totalDocs == 0) {
            // No documents to process
            return new BulkIngestResponse(0, 0, "");
        }

        for (Map.Entry<String, List<Trace.Span>> entry : indexDocs.entrySet()) {
            String index = entry.getKey();

            int partition = getPartition(index);

            if (partition < 0) {
                LOG.warn("index=" + index + " does not have a provisioned dataset associated with it");

            }
        }

        if (indexDocs.isEmpty()) {
            // All docs were for unknown datasets
            return new BulkIngestResponse(0, 0, "No provisioned dataset for index");
        }

        //serializeandcompress
        //createobjectkey
        //putreq then upload object to S3

        //prepare pointer message
        //send to kafka topic





        return new BulkIngestResponse(0, 0, "Success");
    }

    private void cacheSortedDataset() {
    }



    private int getPartition(String index) {
        for (DatasetMetadata datasetMetadata : throughputSortedDatasets) {
            String serviceNamePattern = datasetMetadata.getServiceNamePattern();

            if (serviceNamePattern.equals(MATCH_ALL_SERVICE)
                    || serviceNamePattern.equals(MATCH_STAR_SERVICE)
                    || index.equals(serviceNamePattern)) {
                List<Integer> partitions = getActivePartitionList(datasetMetadata);
                return partitions.get(ThreadLocalRandom.current().nextInt(partitions.size()));
            }
        }
        return -1;
    }

    private static List<Integer> getActivePartitionList(DatasetMetadata datasetMetadata) {
        Optional<DatasetPartitionMetadata> datasetPartitionMetadata =
                datasetMetadata.getPartitionConfigs().stream()
                        .filter(partitionMetadata -> partitionMetadata.getEndTimeEpochMs() == MAX_TIME)
                        .findFirst();

        if (datasetPartitionMetadata.isEmpty()) {
            return Collections.emptyList();
        }
        return datasetPartitionMetadata.get().getPartitions().stream()
                .map(Integer::parseInt)
                .collect(Collectors.toUnmodifiableList());
    }

//  protected Map<BulkIngestRequest, BulkIngestResponse> produceDocuments(List<BulkIngestRequest> requests) {
//    // Modify to call abstract method produceDocumentsBatch()
//  }
//  private static List<Integer> getActivePartitionList(DatasetMetadata datasetMetadata) { }
//
//  }
}