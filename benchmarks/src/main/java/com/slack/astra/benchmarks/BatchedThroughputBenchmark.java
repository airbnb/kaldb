package com.slack.astra.benchmarks;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BatchedThroughputBenchmark {

  private HttpClient httpClient;
  private final String kafkaPreprocessorUrl = "http://localhost:8086";
  private final String s3PreprocessorUrl = "http://localhost:8087";
  private final String indexName = "test";

  // Test parameters
  private final int requestsPerSecond;
  private final int spansPerRequest;
  private final int testDurationSeconds;
  private final String walType;
  private final String preprocessorUrl;

  // Metrics
  private final AtomicInteger requestsSent = new AtomicInteger(0);
  private final AtomicInteger requestsCompleted = new AtomicInteger(0);
  private final AtomicInteger requestsFailed = new AtomicInteger(0);
  private final AtomicLong totalLatencyMs = new AtomicLong(0);
  private final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());

  public BatchedThroughputBenchmark(
      int requestsPerSecond, int spansPerRequest, int testDurationSeconds, String walType) {
    this.requestsPerSecond = requestsPerSecond;
    this.spansPerRequest = spansPerRequest;
    this.testDurationSeconds = testDurationSeconds;
    this.walType = walType;
    this.preprocessorUrl = walType.equals("kafka") ? kafkaPreprocessorUrl : s3PreprocessorUrl;
    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
  }

  public void runBenchmark() throws InterruptedException {
    System.out.printf(
        "Starting %s WAL benchmark: %d RPS, %d spans/request, %d seconds%n",
        walType, requestsPerSecond, spansPerRequest, testDurationSeconds);

    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

    // Schedule requests at fixed rate
    long intervalMs = 1000 / requestsPerSecond;
    scheduler.scheduleAtFixedRate(this::sendRequest, 0, intervalMs, TimeUnit.MILLISECONDS);

    // Run for specified duration
    Thread.sleep(testDurationSeconds * 1000);
    scheduler.shutdown();

    // Wait for remaining requests to complete
    Thread.sleep(5000);

    printResults();
  }

  private void sendRequest() {
    long startTime = System.currentTimeMillis();
    requestsSent.incrementAndGet();

    try {
      String requestBody = generateBulkRequest();

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(preprocessorUrl + "/_bulk"))
              .header("Content-Type", "application/x-ndjson")
              .POST(HttpRequest.BodyPublishers.ofString(requestBody))
              .build();

      CompletableFuture<HttpResponse<String>> responseFuture =
          httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());

      responseFuture.whenComplete(
          (response, throwable) -> {
            long latency = System.currentTimeMillis() - startTime;

            if (throwable != null || response.statusCode() != 200) {
              requestsFailed.incrementAndGet();
              System.err.printf(
                  "Request failed: %s%n",
                  throwable != null ? throwable.getMessage() : response.statusCode());
            } else {
              // Validate response content matches expected WAL type
              String responseBody = response.body();
              boolean isValidResponse = validateWalResponse(responseBody, walType);

              if (isValidResponse) {
                requestsCompleted.incrementAndGet();
                totalLatencyMs.addAndGet(latency);
                latencies.add(latency);
              } else {
                requestsFailed.incrementAndGet();
                System.err.printf("Invalid WAL response for %s: %s%n", walType, responseBody);
              }
            }
          });

    } catch (Exception e) {
      requestsFailed.incrementAndGet();
      System.err.printf("Error sending request: %s%n", e.getMessage());
    }
  }

  private String generateBulkRequest() {
    StringBuilder sb = new StringBuilder();
    String uniqueId = UUID.randomUUID().toString().substring(0, 8);
    String timestamp = Instant.now().toString();

    for (int i = 0; i < spansPerRequest; i++) {
      String spanId = uniqueId + "-" + i;

      // Index line
      sb.append(
          String.format(
              "{ \"index\" : { \"_index\" : \"%s\", \"_id\" : \"%s\" } }%n", indexName, spanId));

      // Document line
      sb.append(
          String.format(
              "{ \"@timestamp\": \"%s\", \"level\": \"INFO\", "
                  + "\"message\": \"Batch %s span %d\", \"service-name\": \"%s\", "
                  + "\"benchmark-id\": \"%s\", \"wal-type\": \"%s\", \"span-index\": %d }%n",
              timestamp, walType, i, indexName, uniqueId, walType, i));
    }

    return sb.toString();
  }

  private boolean validateWalResponse(String responseBody, String walType) {
    if (walType.equals("kafka")) {
      // Kafka WAL should have empty errorMsg: ""
      return responseBody.contains("\"errorMsg\":\"\"");
    } else if (walType.equals("s3")) {
      // S3 WAL should have errorMsg: "Success"
      return responseBody.contains("\"errorMsg\":\"Success\"");
    }
    return false;
  }

  private void printResults() {
    int sent = requestsSent.get();
    int completed = requestsCompleted.get();
    int failed = requestsFailed.get();

    if (completed == 0) {
      System.out.println("No requests completed successfully");
      return;
    }

    List<Long> sortedLatencies = new ArrayList<>(latencies);
    Collections.sort(sortedLatencies);

    long p50 = sortedLatencies.get(sortedLatencies.size() / 2);
    long p95 = sortedLatencies.get((int) (sortedLatencies.size() * 0.95));
    long p99 = sortedLatencies.get((int) (sortedLatencies.size() * 0.99));
    double avgLatency = (double) totalLatencyMs.get() / completed;

    double actualRps = (double) completed / testDurationSeconds;
    double actualSpansPerSec = actualRps * spansPerRequest;

    System.out.printf("%n=== %s WAL Results ===%n", walType.toUpperCase());
    System.out.printf("Requests: %d sent, %d completed, %d failed%n", sent, completed, failed);
    System.out.printf("Throughput: %.1f RPS, %.1f spans/sec%n", actualRps, actualSpansPerSec);
    System.out.printf(
        "Latency: avg=%.1fms, p50=%dms, p95=%dms, p99=%dms%n", avgLatency, p50, p95, p99);
    System.out.printf("Success rate: %.1f%%%n", (double) completed / sent * 100);
  }

  public static void main(String[] args) throws InterruptedException {
    if (args.length < 4) {
      System.out.println(
          "Usage: BatchedThroughputBenchmark <rps> <spans-per-request> <duration-sec> <wal-type>");
      System.out.println("Example: BatchedThroughputBenchmark 10 5 30 kafka");
      return;
    }

    int rps = Integer.parseInt(args[0]);
    int spansPerRequest = Integer.parseInt(args[1]);
    int duration = Integer.parseInt(args[2]);
    String walType = args[3];

    new BatchedThroughputBenchmark(rps, spansPerRequest, duration, walType).runBenchmark();
  }
}
