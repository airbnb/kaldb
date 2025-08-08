package com.slack.astra.benchmarks;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 100, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class SingleMessageLatencyBenchmark {

  private HttpClient httpClient;

  // Docker compose endpoints
  private final String kafkaPreprocessorUrl = "http://localhost:8086"; // Original Kafka WAL
  private final String s3PreprocessorUrl = "http://localhost:8087"; // S3 WAL
  private final String queryUrl = "http://localhost:8081"; // Shared query service
  private final String indexName = System.getProperty("benchmark.index.name", "test");

  @Setup
  public void setup() {
    httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  }

  @Benchmark
  public long measureKafkaWalIngestOnly() throws Exception {
    return measureIngestLatency(kafkaPreprocessorUrl, "kafka-wal");
  }

  @Benchmark
  public long measureS3WalIngestOnly() throws Exception {
    return measureIngestLatency(s3PreprocessorUrl, "s3-wal");
  }

  private long measureIngestLatency(String preprocessorUrl, String walType) throws Exception {
    var uniqueId = UUID.randomUUID().toString().substring(0, 8);
    var timestamp = Instant.now().toString();

    var startTime = System.currentTimeMillis();

    // measure ingest time
    sendSingleLog(preprocessorUrl, uniqueId, timestamp, walType);

    var endTime = System.currentTimeMillis();
    var ingestTime = endTime - startTime;

    // Print for debugging
    System.out.printf("[%s] Pure ingest time: %dms%n", walType, ingestTime);

    return ingestTime;
  }

  private void sendSingleLog(
      String preprocessorUrl, String uniqueId, String timestamp, String walType)
      throws IOException, InterruptedException {
    String requestBody =
        "{ \"index\" : { \"_index\" : \""
            + indexName
            + "\", \"_id\" : \""
            + uniqueId
            + "\" } }\n"
            + "{ \"@timestamp\": \""
            + timestamp
            + "\", \"level\": \"INFO\", \"message\": \"Benchmark "
            + walType
            + " message "
            + uniqueId
            + "\",\"service-name\": \""
            + indexName
            + "\", \"benchmark-id\": \""
            + uniqueId
            + "\", \"wal-type\": \""
            + walType
            + "\" }\n";

    var request =
        HttpRequest.newBuilder()
            .uri(URI.create(preprocessorUrl + "/_bulk"))
            .header("Content-Type", "application/x-ndjson")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();

    var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new RuntimeException(
          "Bulk ingest failed on "
              + preprocessorUrl
              + ": "
              + response.statusCode()
              + " "
              + response.body());
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder().include(SingleMessageLatencyBenchmark.class.getSimpleName()).build();

    new Runner(opt).run();
  }
}
