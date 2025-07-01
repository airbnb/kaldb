package com.slack.astra.bulkIngestApi;

import static com.google.common.base.Preconditions.checkArgument;
import static com.slack.astra.metadata.dataset.DatasetMetadata.MATCH_ALL_SERVICE;
import static com.slack.astra.metadata.dataset.DatasetMetadata.MATCH_STAR_SERVICE;
import static com.slack.astra.server.ManagerApiGrpc.MAX_TIME;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.writer.KafkaUtils;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public abstract class BulkIngestProducer extends AbstractExecutionThreadService {

    private static final Logger LOG = LoggerFactory.getLogger(BulkIngestKafkaProducer.class);
    private final boolean useKafkaTransactions;

    private KafkaProducer<String, byte[]> kafkaProducer;

    private KafkaClientMetrics kafkaMetrics;

    private final AstraConfigs.KafkaConfig kafkaConfig;

    private final DatasetMetadataStore datasetMetadataStore;
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

    protected final S3AsyncClient s3Client;
    private String walBucket;

    public static final String KAFKA_RESTART_COUNTER = "bulk_ingest_producer_kafka_restart_timer";
    private final Timer kafkaRestartTimer;

    public static final String BATCH_SIZE_GAUGE = "bulk_ingest_producer_batch_size";
    private final AtomicInteger batchSizeGauge;

    private final MeterRegistry meterRegistry;

    private static final Set<String> OVERRIDABLE_CONFIGS =
            Set.of(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);


    public BulkIngestProducer(
            final DatasetMetadataStore datasetMetadataStore,
            final AstraConfigs.PreprocessorConfig preprocessorConfig,
            final MeterRegistry meterRegistry,
            S3AsyncClient s3Client) {

        this.kafkaConfig = preprocessorConfig.getKafkaConfig();

        checkArgument(
                !kafkaConfig.getKafkaBootStrapServers().isEmpty(),
                "Kafka bootstrapServers must be provided");
        checkArgument(!kafkaConfig.getKafkaTopic().isEmpty(), "Kafka topic must be provided");

        this.meterRegistry = meterRegistry;
        this.datasetMetadataStore = datasetMetadataStore;
        this.pendingRequests = new LinkedBlockingQueue<>();

        this.producerSleepMs =
                Integer.parseInt(System.getProperty("astra.bulkIngest.producerSleepMs", "50"));

        this.useKafkaTransactions =
                Boolean.parseBoolean(System.getProperty("astra.bulkIngest.useKafkaTransactions", "false"));

        this.s3Client = s3Client;
        this.failedSetResponseCounter = meterRegistry.counter(FAILED_SET_RESPONSE_COUNTER);
        this.stallCounter = meterRegistry.counter(STALL_COUNTER);
        this.kafkaRestartTimer = meterRegistry.timer(KAFKA_RESTART_COUNTER);
        this.batchSizeGauge = meterRegistry.gauge(BATCH_SIZE_GAUGE, new AtomicInteger(0));

        startKafkaProducer();
    }

    private void startKafkaProducer() {

        this.kafkaProducer = createKafkaTransactionProducer(UUID.randomUUID().toString());
        this.kafkaMetrics = new KafkaClientMetrics(kafkaProducer);
        this.kafkaMetrics.bindTo(meterRegistry);
        if (useKafkaTransactions) {
            this.kafkaProducer.initTransactions();
        }
    }

    private void stopKafkaProducer() {
        try {
            if (this.kafkaProducer != null) {
                this.kafkaProducer.close(Duration.ZERO);
            }

            if (this.kafkaMetrics != null) {
                this.kafkaMetrics.close();
            }
        } catch (Exception e) {
            LOG.error("Error attempting to stop the Kafka producer", e);
        }
    }

    private void restartKafkaProducer() {
        Timer.Sample restartTimer = Timer.start(meterRegistry);
        stopKafkaProducer();
        startKafkaProducer();
        LOG.info("Restarted the kafka producer");
        restartTimer.stop(kafkaRestartTimer);
    }

    private void cacheSortedDataset() {

        this.throughputSortedDatasets =
                datasetMetadataStore.listSync().stream()
                        .sorted(Comparator.comparingLong(DatasetMetadata::getThroughputBytes).reversed())
                        .toList();
    }

    @Override
    protected void startUp() throws Exception {
        cacheSortedDataset();
        datasetMetadataStore.addListener(datasetListener);
    }

    @Override
    protected void run() throws Exception {
        while (isRunning()) {
            List<BulkIngestRequest> requests = new ArrayList<>();
            pendingRequests.drainTo(requests);
            batchSizeGauge.set(requests.size());
            if (requests.isEmpty()) {
                try {
                    stallCounter.increment();
                    Thread.sleep(producerSleepMs);
                } catch (InterruptedException e) {
                    return;
                }
            } else {
                //processRequest(requests);
            }
        }
    }

    @Override
    protected void shutDown() throws Exception {
        datasetMetadataStore.removeListener(datasetListener);

        kafkaProducer.close();
        if (kafkaMetrics != null) {
            kafkaMetrics.close();
        }
    }

    public BulkIngestRequest submitRequest(Map<String, List<Trace.Span>> inputDocs) {
        BulkIngestRequest request = new BulkIngestRequest(inputDocs);
        pendingRequests.add(request);
        return request;
    }

    protected abstract BulkIngestResponse processRequest(BulkIngestRequest request) throws Exception;

    protected int getPartition(String index) {
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

}
