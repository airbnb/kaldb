package com.slack.astra.benchmarks;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
public class ObjectSizeBenchmark {

  private HttpClient httpClient;
  private final String kafkaPreprocessorUrl = "http://localhost:8086";
  private final String s3PreprocessorUrl = "http://localhost:8087";
  private final String indexName = "test";

  // Test parameters
  private final int requestCount;
  private final int spansPerRequest;
  private final String payloadSize; // "small", "medium", "large"
  private final String walType;
  private final String preprocessorUrl;

  // Size metrics
  private final AtomicInteger requestsSent = new AtomicInteger(0);
  private final AtomicInteger requestsCompleted = new AtomicInteger(0);
  private final AtomicLong totalRequestBytes =
      new AtomicLong(0); // Total bytes sent to preprocessor

  public ObjectSizeBenchmark(
      int requestCount, int spansPerRequest, String payloadSize, String walType) {
    this.requestCount = requestCount;
    this.spansPerRequest = spansPerRequest;
    this.payloadSize = payloadSize;
    this.walType = walType;
    this.preprocessorUrl = walType.equals("kafka") ? kafkaPreprocessorUrl : s3PreprocessorUrl;
    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
  }

  public void runBenchmark() throws InterruptedException, IOException {

    System.out.printf(
        "Starting %s WAL size benchmark: %d requests, %d spans/request, %s payload%n",
        walType.toUpperCase(), requestCount, spansPerRequest, payloadSize);

    long startTime = System.currentTimeMillis();

    // Send all requests
    for (int i = 0; i < requestCount; i++) {
      sendRequest();
      if (i > 0 && i % 10 == 0) {
        System.out.printf("Sent %d/%d requests%n", i, requestCount);
      }
    }

    // Wait for completion
    System.out.println("Waiting for requests to complete...");
    Thread.sleep(5000);

    long endTime = System.currentTimeMillis();
    long testDurationMs = endTime - startTime;

    // Measure storage sizes
    long actualStorageBytes = 0;

    if (walType.equals("kafka")) {
      actualStorageBytes = measureKafkaStorage();
    } else {
      actualStorageBytes = measureS3StorageAndGetPointers();
    }
    printResults(testDurationMs, actualStorageBytes);
  }

  private void sendRequest() throws IOException, InterruptedException {
    String requestBody = generateBulkRequest();
    long requestBytes = requestBody.getBytes().length;

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(preprocessorUrl + "/_bulk"))
            .header("Content-Type", "application/x-ndjson")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();

    requestsSent.incrementAndGet();
    totalRequestBytes.addAndGet(requestBytes);

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() == 200 && validateWalResponse(response.body(), walType)) {
      requestsCompleted.incrementAndGet();
    } else {
      System.err.printf("Request failed: %d %s%n", response.statusCode(), response.body());
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

      // Document line with configurable payload
      sb.append(generateSpan(timestamp, uniqueId, i));
    }

    return sb.toString();
  }

  private String generateSpan(String timestamp, String uniqueId, int spanIndex) {
    String baseSpan =
        String.format(
            "{ \"@timestamp\": \"%s\", \"level\": \"INFO\", "
                + "\"message\": \"Size test %s span %d\", \"service-name\": \"%s\", "
                + "\"benchmark-id\": \"%s\", \"wal-type\": \"%s\", \"span-index\": %d",
            timestamp, walType, spanIndex, indexName, uniqueId, walType, spanIndex);

    // Add payload fields based on size configuration
    String payload = generatePayload();
    int extraFields = getExtraFieldCount();

    StringBuilder spanBuilder = new StringBuilder(baseSpan);

    // Add payload field
    spanBuilder.append(String.format(", \"payload\": \"%s\"", payload));

    // Add extra fields for size control
    for (int i = 0; i < extraFields; i++) {
      spanBuilder.append(String.format(", \"field_%d\": \"%s\"", i, payload));
    }

    spanBuilder.append(" }\n");
    return spanBuilder.toString();
  }

  private String generatePayload() {
    switch (payloadSize.toLowerCase()) {
      case "small":
        return "x".repeat(50); // ~50 bytes
      case "medium":
        return "x".repeat(500); // ~500 bytes
      case "large":
        return "x".repeat(2000); // ~2KB
      default:
        return "small-payload";
    }
  }

  private int getExtraFieldCount() {
    switch (payloadSize.toLowerCase()) {
      case "small":
        return 2; // 2 extra fields
      case "medium":
        return 5; // 5 extra fields
      case "large":
        return 10; // 10 extra fields
      default:
        return 1;
    }
  }

  private boolean validateWalResponse(String responseBody, String walType) {
    if (walType.equals("kafka")) {
      return responseBody.contains("\"errorMsg\":\"\"");
    } else if (walType.equals("s3")) {
      return responseBody.contains("\"errorMsg\":\"Success\"");
    }
    return false;
  }

  private long measureKafkaStorage() throws IOException, InterruptedException {
    System.out.println("Measuring actual Kafka message sizes...");

    // Add Kafka consumer dependency and measure real message bytes
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "size-measurement-" + System.currentTimeMillis());
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("auto.offset.reset", "earliest"); // Read from beginning to get all messages

    KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList("test-topic"));

    long totalKafkaBytes = 0;
    int messagesFound = 0;
    long startTime = System.currentTimeMillis();

    // Get current time minus a buffer to catch recent messages
    long testStartTime = System.currentTimeMillis() - 60000; // 1 minute ago

    // Poll for messages from our test
    while (messagesFound < requestsCompleted.get() * spansPerRequest
        && (System.currentTimeMillis() - startTime) < 30000) {
      ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

      if (records.isEmpty()) {
        System.out.printf(
            "No messages found yet, continuing... (found %d so far)%n", messagesFound);
        continue;
      }

      for (ConsumerRecord<String, byte[]> record : records) {
        // Only count recent messages from our test
        if (record.timestamp() > testStartTime) {
          totalKafkaBytes += record.serializedValueSize();
          messagesFound++;
          System.out.printf(
              "Kafka message %d: %d bytes (timestamp: %d)%n",
              messagesFound, record.serializedValueSize(), record.timestamp());
        }
      }
    }

    consumer.close();

    System.out.printf("Kafka messages found: %d%n", messagesFound);
    System.out.printf("Actual Kafka storage: %d bytes%n", totalKafkaBytes);

    return totalKafkaBytes;
  }

  private long measureS3StorageAndGetPointers() throws IOException, InterruptedException {
    System.out.println("Measuring actual S3 object sizes...");

    // Wait for S3 objects to be created
    Thread.sleep(3000);

    // Get S3 bucket listing with actual object sizes
    HttpRequest s3Request =
        HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:9090/test-s3-bucket?list-type=2"))
            .GET()
            .build();

    HttpResponse<String> s3Response =
        httpClient.send(s3Request, HttpResponse.BodyHandlers.ofString());

    long totalS3Bytes = 0;
    if (s3Response.statusCode() == 200) {
      String responseBody = s3Response.body();

      // Parse actual S3 object sizes from XML response
      totalS3Bytes = parseS3ObjectSizes(responseBody);
      int objectCount = countS3Objects(responseBody);

      System.out.printf("S3 objects found: %d%n", objectCount);
      System.out.printf("Actual S3 storage: %d bytes%n", totalS3Bytes);

    }
    return totalS3Bytes;
  }

  private long parseS3ObjectSizes(String s3Response) {
    long totalBytes = 0;

    // Only count objects created in the last few minutes (during this test)
    long testStartTime = System.currentTimeMillis() - (10 * 60 * 1000); // 10 minutes ago

    String[] keyParts = s3Response.split("<Key>");

    for (String part : keyParts) {
      if (part.contains("wal/")) {
        int endKey = part.indexOf("</Key>");
        if (endKey > 0) {
          String walKey = part.substring(0, endKey);

          // Extract timestamp from WAL key (format: partition-0-{timestamp}-{uuid}.gz)
          if (isRecentWalObject(walKey, testStartTime)) {
            int sizeStart = part.indexOf("<Size>") + 6;
            int sizeEnd = part.indexOf("</Size>");
            if (sizeStart > 5 && sizeEnd > sizeStart) {
              String sizeStr = part.substring(sizeStart, sizeEnd);
              try {
                long objectSize = Long.parseLong(sizeStr);
                totalBytes += objectSize;
                System.out.printf("Recent WAL object %s size: %d bytes%n", walKey, objectSize);
              } catch (NumberFormatException e) {
                System.err.printf("Could not parse size %s for WAL object %s%n", sizeStr, walKey);
              }
            }
          }
        }
      }
    }

    System.out.printf("Total recent WAL storage: %d bytes%n", totalBytes);
    return totalBytes;
  }

  private boolean isRecentWalObject(String walKey, long testStartTime) {
    // Extract timestamp from key
    try {
      String[] parts = walKey.split("-");
      if (parts.length >= 3) {
        long objectTimestamp = Long.parseLong(parts[2]); // Extract timestamp
        return objectTimestamp >= testStartTime;
      }
    } catch (NumberFormatException e) {
      // If we can't parse timestamp, include it to be safe
      return true;
    }
    return false;
  }

  private int countS3Objects(String s3Response) {
    int count = 0;

    // S3Mock returns XML in a single line
    String[] keyParts = s3Response.split("<Key>");

    for (String part : keyParts) {
      if (part.contains("wal/")) {
        int endKey = part.indexOf("</Key>");
        if (endKey > 0) {
          String walKey = part.substring(0, endKey);
          count++;
          System.out.printf("WAL object %d: %s%n", count, walKey);
        }
      }
    }

    System.out.printf("Total WAL objects found: %d%n", count);
    return count;
  }

  private void printResults(long testDurationMs, long actualStorageBytes) {
    int sent = requestsSent.get();
    int completed = requestsCompleted.get();
    long inputBytes = totalRequestBytes.get();

    System.out.printf("%n=== %s WAL SIZE RESULTS ===%n", walType.toUpperCase());
    System.out.printf("Requests: %d sent, %d completed%n", sent, completed);
    System.out.printf("Total spans: %d%n", completed * spansPerRequest);
    System.out.printf("Input data size: %d bytes (%.2f KB)%n", inputBytes, inputBytes / 1024.0);
    System.out.printf(
        "Average bytes per span: %.1f%n", (double) inputBytes / (completed * spansPerRequest));
    System.out.printf("Test duration: %.1f seconds%n", testDurationMs / 1000.0);

    System.out.printf(
        "Actual storage size: %d bytes (%.2f KB)%n",
        actualStorageBytes, actualStorageBytes / 1024.0);

    if (walType.equals("s3")) {
      System.out.printf("Total S3 WAL storage: %d bytes%n", actualStorageBytes);
    }

    double compressionRatio = (double) inputBytes / actualStorageBytes;
    System.out.printf("Compression ratio: %.2f:1%n", compressionRatio);
    System.out.printf(
        "Storage efficiency: %.1f%% of input size%n",
        (double) actualStorageBytes / inputBytes * 100);
  }

  public static void main(String[] args) throws InterruptedException, IOException {
    if (args.length < 4) {
      System.out.println(
          "Usage: ObjectSizeBenchmark <request-count> <spans-per-request> <payload-size> <wal-type>");
      System.out.println("Example: ObjectSizeBenchmark 50 5 medium kafka");
      System.out.println("Payload sizes: small, medium, large");
      return;
    }

    int requestCount = Integer.parseInt(args[0]);
    int spansPerRequest = Integer.parseInt(args[1]);
    String payloadSize = args[2];
    String walType = args[3];

    new ObjectSizeBenchmark(requestCount, spansPerRequest, payloadSize, walType).runBenchmark();
  }
}
