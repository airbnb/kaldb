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
@Warmup(iterations = 1)
@Measurement(iterations = 10, batchSize = 1)
@Fork(1)
@Threads(1)
public class QueryLatencyBenchmark {

  private HttpClient httpClient;

  // Docker compose endpoints
  private final String kafkaPreprocessorUrl = "http://localhost:8086"; // Kafka WAL
  private final String s3PreprocessorUrl = "http://localhost:8087"; // S3 WAL
  private final String queryUrl = "http://localhost:8081"; // Shared query service
  private final String indexName = System.getProperty("benchmark.index.name", "test");

  @Setup
  public void setup() {
    httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(60)).build();
  }

  @Benchmark
  public LatencyResult measureKafkaWalLatency() throws Exception {
    return measureLatency(kafkaPreprocessorUrl, "kafka-wal");
  }

  @Benchmark
  public LatencyResult measureS3WalLatency() throws Exception {
    return measureLatency(s3PreprocessorUrl, "s3-wal");
  }

  private LatencyResult measureLatency(String preprocessorUrl, String walType) throws Exception {
    var uniqueId = UUID.randomUUID().toString().substring(0, 8);
    var timestamp = Instant.now().toString();

    var totalStartTime = System.currentTimeMillis();

    // Step 1: Measure WAL processing time (ingest only)
    var walStartTime = System.currentTimeMillis();
    sendSingleLog(preprocessorUrl, uniqueId, timestamp, walType);
    var walEndTime = System.currentTimeMillis();
    var walLatency = walEndTime - walStartTime;

    // Step 2: Measure query visibility time
    var queryStartTime = System.currentTimeMillis();
    waitForLogVisibility(uniqueId);
    var queryEndTime = System.currentTimeMillis();
    var queryLatency = queryEndTime - queryStartTime;

    var totalEndTime = System.currentTimeMillis();
    var totalLatency = totalEndTime - totalStartTime;

    // Print detailed breakdown
    System.out.printf(
        "[%s] WAL: %d ms, Query: %d ms, Total: %d ms%n",
        walType, walLatency, queryLatency, totalLatency);

    return new LatencyResult(walLatency, queryLatency, totalLatency);
  }

  private void sendSingleLog(
      String preprocessorUrl, String uniqueId, String timestamp, String walType)
      throws IOException, InterruptedException {

    StringBuilder sb = new StringBuilder();

    // Index line
    sb.append(
        String.format(
            "{ \"index\" : { \"_index\" : \"%s\", \"_id\" : \"%s\" } }%n", indexName, uniqueId));

    // Document line
    sb.append(
        String.format(
            "{ \"@timestamp\": \"%s\", \"level\": \"INFO\", "
                + "\"message\": \"Benchmark %s message %s\", \"service-name\": \"%s\", "
                + "\"benchmark-id\": \"%s\", \"wal-type\": \"%s\"}%n",
            timestamp, walType, uniqueId, indexName, uniqueId, walType, uniqueId));

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(preprocessorUrl + "/_bulk"))
            .header("Content-Type", "application/x-ndjson")
            .timeout(Duration.ofSeconds(60)) // Add this line
            .POST(HttpRequest.BodyPublishers.ofString(sb.toString()))
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

  private void waitForLogVisibility(String uniqueId) throws IOException, InterruptedException {
    var currentTime = System.currentTimeMillis();
    var startRange = currentTime - 300_000; // 5 minutes ago
    var endRange = currentTime + 300_000; // 5 minutes from now

    var searchBody =
        String.format(
            """
                { "index": "%s"}
                {"query" : {"bool": {"must": [{"match": {"benchmark-id": "%s"}}]}}, "gte":%d,"lte":%d, "size": 1}
                """,
            indexName, uniqueId, startRange, endRange);

    var request =
        HttpRequest.newBuilder()
            .uri(URI.create(queryUrl + "/_msearch"))
            .header("Content-Type", "application/x-ndjson")
            .timeout(Duration.ofSeconds(60))
            .POST(HttpRequest.BodyPublishers.ofString(searchBody))
            .build();

    // Poll until found (max 60 seconds)
    for (int i = 0; i < 1200; i++) {
      try {
        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 200) {
          var responseBody = response.body();
          if (responseBody.contains(uniqueId) && responseBody.contains("\"total\":{\"value\":1")) {
            return; // Found!
          }
        }
      } catch (Exception e) {
        // Continue polling on any error
      }

      Thread.sleep(50); // Wait 50ms between polls
    }

    throw new RuntimeException("Log message not found within 60 second timeout");
  }

  // Result class to hold latency breakdown
  public static class LatencyResult {
    public final long walLatency;
    public final long queryLatency;
    public final long totalLatency;

    public LatencyResult(long walLatency, long queryLatency, long totalLatency) {
      this.walLatency = walLatency;
      this.queryLatency = queryLatency;
      this.totalLatency = totalLatency;
    }

    @Override
    public String toString() {
      return String.format(
          "WAL:%dms Query:%dms Total:%dms", walLatency, queryLatency, totalLatency);
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(QueryLatencyBenchmark.class.getSimpleName()).build();

    new Runner(opt).run();
  }
}
