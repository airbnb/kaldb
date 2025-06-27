package com.slack.astra.bulkIngestApi;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
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
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

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
//        this.walBucket =  this.walBucket = preprocessorConfig.hasWalBucket()
//                ? preprocessorConfig.getWalBucket()
//                : preprocessorConfig.getS3Config().getS3Bucket();
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

    protected void run() throws Exception {}

    private BulkIngestResponse processRequest(BulkIngestRequest request) {
        // Implement the logic to process the request
        return new BulkIngestResponse(0, 0, "Success");
    }

    private void cacheSortedDataset() {
    }
    protected int getPartitionForIndex(String index) {return 0;}
//
//  @Override
//  protected void startUp() throws Exception { }
//
//  @Override
//  protected void run() throws Exception { }
//
//  @Override
//  protected void shutDown() throws Exception {
//    datasetMetadataStore.removeListener(datasetListener);
//    shutdownProducer(); // Call abstract method
//  }
//
//  public BulkIngestRequest submitRequest(Map<String, List<Trace.Span>> inputDocs) {  }
//
//  protected Map<BulkIngestRequest, BulkIngestResponse> produceDocuments(List<BulkIngestRequest> requests) {
//    // Modify to call abstract method produceDocumentsBatch()
//  }
//
//  private static List<Integer> getActivePartitionList(DatasetMetadata datasetMetadata) { }
//
//  }
}