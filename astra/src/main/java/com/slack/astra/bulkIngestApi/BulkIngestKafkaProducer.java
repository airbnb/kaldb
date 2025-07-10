package com.slack.astra.bulkIngestApi;


import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkIngestKafkaProducer extends BulkIngestProducer {
  private static final Logger LOG = LoggerFactory.getLogger(BulkIngestKafkaProducer.class);
  private final boolean useKafkaTransactions;

  private KafkaClientMetrics kafkaMetrics;

  private static final Set<String> OVERRIDABLE_CONFIGS =
      Set.of(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

  public BulkIngestKafkaProducer(
      final DatasetMetadataStore datasetMetadataStore,
      final AstraConfigs.PreprocessorConfig preprocessorConfig,
      final MeterRegistry meterRegistry) {
    super(datasetMetadataStore, preprocessorConfig, meterRegistry, null);
    this.useKafkaTransactions =
        Boolean.parseBoolean(System.getProperty("astra.bulkIngest.useKafkaTransactions", "false"));
  }

  @Override
  protected void startKafkaProducer() {
    // since we use a new transaction ID every time we start a preprocessor there can be some zombie
    // transactions?
    // I think they will remain in kafka till they expire. They should never be readable if the
    // consumer sets isolation.level as "read_committed"
    // see "zombie fencing" https://www.confluent.io/blog/transactions-apache-kafka/
    super.startKafkaProducer(); // This calls parent's kafka setup
    this.kafkaMetrics = new KafkaClientMetrics(kafkaProducer);
    this.kafkaMetrics.bindTo(meterRegistry);
    if (useKafkaTransactions) {
      this.kafkaProducer.initTransactions();
    }
  }

  @Override
  protected Map<BulkIngestRequest, BulkIngestResponse> produceDocuments(
      List<BulkIngestRequest> requests) {
    if (useKafkaTransactions) {
      return produceDocumentsAndCommit(requests);
    } else {
      Map<BulkIngestRequest, BulkIngestResponse> responseMap = new HashMap<>();
      try {
        for (BulkIngestRequest request : requests) {
          responseMap.put(request, produceDocuments(request.getInputDocs(), kafkaProducer));
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
        LOG.error("Failed to write batch to kafka", e);
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
  }

  private Map<BulkIngestRequest, BulkIngestResponse> produceDocumentsAndCommit(
      List<BulkIngestRequest> requests) {
    Map<BulkIngestRequest, BulkIngestResponse> responseMap = new HashMap<>();
    try {
      kafkaProducer.beginTransaction();
      for (BulkIngestRequest request : requests) {
        responseMap.put(request, produceDocuments(request.getInputDocs(), kafkaProducer));
      }
      kafkaProducer.commitTransaction();
    } catch (TimeoutException te) {
      // todo - consider collapsing these exceptions into a common implementation

      // In the event of a timeout, we cannot abort but must either retry or restart the producer
      // See org.apache.kafka.clients.producer.KafkaProducer.abortTransaction docblock
      LOG.error("Commit transaction timeout, must restart producer", te);
      restartKafkaProducer();

      for (BulkIngestRequest request : requests) {
        responseMap.put(
            request,
            new BulkIngestResponse(
                0,
                request.getInputDocs().values().stream().mapToInt(List::size).sum(),
                te.getMessage()));
      }
    } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
      // We can't recover from these exceptions, so our only option is to close the producer and
      // exit.
      LOG.error("Unrecoverable kafka error, must restart producer", e);
      restartKafkaProducer();

      for (BulkIngestRequest request : requests) {
        responseMap.put(
            request,
            new BulkIngestResponse(
                0,
                request.getInputDocs().values().stream().mapToInt(List::size).sum(),
                e.getMessage()));
      }
    } catch (Exception e) {
      LOG.warn("failed transaction with error", e);
      if (kafkaProducer != null) {
        try {
          kafkaProducer.abortTransaction();
        } catch (ProducerFencedException err) {
          LOG.error("Could not abort transaction, must restart producer", err);
          restartKafkaProducer();
        }
      }

      for (BulkIngestRequest request : requests) {
        responseMap.put(
            request,
            new BulkIngestResponse(
                0,
                request.getInputDocs().values().stream().mapToInt(List::size).sum(),
                e.getMessage()));
      }
    }

    for (Map.Entry<BulkIngestRequest, BulkIngestResponse> entry : responseMap.entrySet()) {
      BulkIngestRequest key = entry.getKey();
      BulkIngestResponse value = entry.getValue();
      if (!key.setResponse(value)) {
        LOG.warn("Failed to add result to the bulk ingest request, consumer thread went away?");
        failedSetResponseCounter.increment();
      }
    }
    return responseMap;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private BulkIngestResponse produceDocuments(
      Map<String, List<Trace.Span>> indexDocs, KafkaProducer<String, byte[]> kafkaProducer) {
    int totalDocs = indexDocs.values().stream().mapToInt(List::size).sum();

    // we cannot create a generic pool of producers because the kafka API expects the transaction ID
    // to be a property while creating the producer object.
    for (Map.Entry<String, List<Trace.Span>> indexDoc : indexDocs.entrySet()) {
      String index = indexDoc.getKey();

      // call once per batch and use the same partition for better batching
      // todo - this probably shouldn't be tied to the transaction batching logic?
      int partition = getPartition(index);

      // since there isn't a dataset provisioned for this service/index we will not index this set
      // of docs
      if (partition < 0) {
        LOG.warn("index=" + index + " does not have a provisioned dataset associated with it");
        continue;
      }

      // KafkaProducer does not allow creating multiple transactions from a single object -
      // rightfully so.
      // Till we fix the producer design to allow for multiple /_bulk requests to be able to
      // write to the same txn
      // we will limit producing documents 1 thread at a time
      for (Trace.Span doc : indexDoc.getValue()) {
        ProducerRecord<String, byte[]> producerRecord =
            new ProducerRecord<>(kafkaConfig.getKafkaTopic(), partition, index, doc.toByteArray());

        // we intentionally suppress FutureReturnValueIgnored here in errorprone - this is because
        // we wrap this in a transaction, which is responsible for flushing all of the pending
        // messages
        kafkaProducer.send(producerRecord);
      }
    }

    return new BulkIngestResponse(totalDocs, 0, "");
  }
}
