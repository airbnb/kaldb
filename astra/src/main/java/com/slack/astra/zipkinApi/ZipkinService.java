package com.slack.astra.zipkinApi;

import static com.slack.astra.metadata.dataset.DatasetPartitionMetadata.MATCH_ALL_DATASET;

import brave.Tracing;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Default;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Header;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Path;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogWireMessage;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.server.AstraQueryServiceBase;
import com.slack.astra.util.JsonUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
/**
 * Zipkin compatible API service
 *
 * @see <a
 *     href="https://github.com/grafana/grafana/blob/main/public/app/plugins/datasource/zipkin/datasource.ts">Grafana
 *     Zipkin API</a> <a
 *     href="https://github.com/openzipkin/zipkin-api/blob/master/zipkin.proto">Trace proto
 *     compatible with Zipkin</a> <a href="https://zipkin.io/zipkin-api/#/">Trace API Swagger
 *     Hub</a>
 */
public class ZipkinService {

  protected static String convertLogWireMessageToZipkinSpan(List<LogWireMessage> messages)
      throws JsonProcessingException {
    List<ZipkinSpanResponse> traces = new ArrayList<>(messages.size());
    for (LogWireMessage message : messages) {
      if (message.getId() == null) {
        LOG.warn("Document={} cannot have missing id ", message);
        continue;
      }

      String id = message.getId();
      String messageTraceId = null;
      String parentId = null;
      String name = null;
      String serviceName = null;
      String timestamp = String.valueOf(message.getTimestamp().toEpochMilli());
      long duration = 0L;
      Map<String, String> messageTags = new HashMap<>();

      for (String k : message.getSource().keySet()) {
        Object value = message.getSource().get(k);
        if (LogMessage.ReservedField.TRACE_ID.fieldName.equals(k)) {
          messageTraceId = (String) value;
        } else if (LogMessage.ReservedField.PARENT_ID.fieldName.equals(k)) {
          parentId = (String) value;
        } else if (LogMessage.ReservedField.NAME.fieldName.equals(k)) {
          name = (String) value;
        } else if (LogMessage.ReservedField.SERVICE_NAME.fieldName.equals(k)) {
          serviceName = (String) value;
        } else if (LogMessage.ReservedField.DURATION.fieldName.equals(k)) {
          duration = ((Number) value).longValue();
        } else if (LogMessage.ReservedField.ID.fieldName.equals(k)) {
          id = (String) value;
        } else {
          messageTags.put(k, String.valueOf(value));
        }
      }

      // TODO: today at Slack the duration is sent as "duration_ms"
      // We we have this special handling which should be addressed upstream
      // and then removed from here
      if (duration == 0) {
        Object value =
            message.getSource().getOrDefault(LogMessage.ReservedField.DURATION_MS.fieldName, 0);
        duration = TimeUnit.MICROSECONDS.convert(Duration.ofMillis(((Number) value).intValue()));
      }

      // these are some mandatory fields without which the grafana zipkin plugin fails to display
      // the span
      if (messageTraceId == null) {
        messageTraceId = message.getId();
      }
      if (timestamp == null) {
        LOG.warn(
            "Document id={} missing {}",
            message,
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
        continue;
      }

      final ZipkinSpanResponse span = new ZipkinSpanResponse(id, messageTraceId);
      span.setParentId(parentId);
      span.setName(name);
      if (serviceName != null) {
        ZipkinEndpointResponse remoteEndpoint = new ZipkinEndpointResponse();
        remoteEndpoint.setServiceName(serviceName);
        span.setRemoteEndpoint(remoteEndpoint);
      }
      span.setTimestamp(convertToMicroSeconds(message.getTimestamp()));
      span.setDuration(duration);
      span.setTags(messageTags);
      traces.add(span);
    }
    return objectMapper.writeValueAsString(traces);
  }

  // returning LogWireMessage instead of LogMessage
  // If we return LogMessage the caller then needs to call getSource which is a deep copy of the
  // object. To return LogWireMessage we do a JSON parse
  static List<LogWireMessage> searchResultToLogWireMessage(AstraSearch.SearchResult searchResult)
      throws IOException {
    List<ByteString> hitsByteList = searchResult.getHitsList().asByteStringList();
    List<LogWireMessage> messages = new ArrayList<>(hitsByteList.size());
    for (ByteString byteString : hitsByteList) {
      LogWireMessage hit = JsonUtil.read(byteString.toStringUtf8(), LogWireMessage.class);
      // LogMessage message = LogMessage.fromWireMessage(hit);
      messages.add(hit);
    }
    return messages;
  }

  @VisibleForTesting
  protected static long convertToMicroSeconds(Instant instant) {
    return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
  }

  private static final Logger LOG = LoggerFactory.getLogger(ZipkinService.class);
  private final int defaultMaxSpans;
  private final int defaultLookbackMins;

  private final long defaultDataFreshnessInMinutes;

  private final AstraQueryServiceBase searcher;

  private final BlobStore blobStore;

  public static final String TRACE_CACHE_PREFIX = "traceCacheData";

  private static final ObjectMapper objectMapper =
      JsonMapper.builder()
          // sort alphabetically for easier test asserts
          .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
          // don't serialize null values or empty maps
          .serializationInclusion(JsonInclude.Include.NON_EMPTY)
          .build();

  public ZipkinService(
      AstraQueryServiceBase searcher,
      BlobStore blobStore,
      int defaultMaxSpans,
      int defaultLookbackMins,
      long defaultDataFreshnessInMinutes) {
    this.searcher = searcher;
    this.blobStore = blobStore;
    this.defaultMaxSpans = defaultMaxSpans;
    this.defaultLookbackMins = defaultLookbackMins;
    this.defaultDataFreshnessInMinutes = defaultDataFreshnessInMinutes;
  }

  @Get
  @Path("/api/v2/services")
  public HttpResponse getServices() throws IOException {
    String output = "[]";
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  @Get("/api/v2/spans")
  public HttpResponse getSpans(@Param("serviceName") Optional<String> serviceName)
      throws IOException {
    String output = "[]";
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  @Get("/api/v2/traces")
  public HttpResponse getTraces(
      @Param("serviceName") Optional<String> serviceName,
      @Param("spanName") Optional<String> spanName,
      @Param("annotationQuery") Optional<String> annotationQuery,
      @Param("minDuration") Optional<Integer> minDuration,
      @Param("maxDuration") Optional<Integer> maxDuration,
      @Param("endTs") Long endTs,
      @Param("lookback") Long lookback,
      @Param("limit") @Default("10") Integer limit)
      throws IOException {
    String output = "[]";
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  @Blocking
  @Get("/api/v2/trace/{traceId}")
  public HttpResponse getTraceByTraceId(
      @Param("traceId") String traceId,
      @Param("startTimeEpochMs") Optional<Long> startTimeEpochMs,
      @Param("endTimeEpochMs") Optional<Long> endTimeEpochMs,
      @Param("maxSpans") Optional<Integer> maxSpans,
      @Header("X-User-Request") Optional<Boolean> userRequest,
      @Header("X-Data-Freshness-In-Minutes") Optional<Long> dataFreshnessInMinutes)
      throws IOException {

    // Log the custom header userRequest value if present
    if (userRequest.isPresent()) {
      LOG.info("Received custom header X-User-Request: {}", userRequest.get());
      // try to retrieve trace data from S3; check timestamp before using S3 for data freshness
      long dataFreshnessInMinutesValue =
          dataFreshnessInMinutes.orElse(
              this.defaultDataFreshnessInMinutes); // default to 15 minutes if not provided
      String traceData = retrieveDataFromS3(traceId, dataFreshnessInMinutesValue);
      // if found, return the data
      if (traceData != null) {
        LOG.info("Trace data retrieved from S3 for traceId={}", traceId);
        return HttpResponse.of(HttpStatus.OK, MediaType.ANY_APPLICATION_TYPE, traceData);
      }
    }
    JSONObject traceObject = new JSONObject();
    traceObject.put("trace_id", traceId);
    JSONObject queryJson = new JSONObject();
    queryJson.put("term", traceObject);
    String queryString = queryJson.toString();
    long startTime =
        startTimeEpochMs.orElseGet(
            () -> Instant.now().minus(this.defaultLookbackMins, ChronoUnit.MINUTES).toEpochMilli());
    // we are adding a buffer to end time also because some machines clock may be ahead of current
    // system clock and those spans would be stored but can't be queried

    long endTime =
        endTimeEpochMs.orElseGet(
            () -> Instant.now().plus(this.defaultLookbackMins, ChronoUnit.MINUTES).toEpochMilli());
    int howMany = maxSpans.orElse(this.defaultMaxSpans);

    brave.Span span = Tracing.currentTracer().currentSpan();
    span.tag("startTimeEpochMs", String.valueOf(startTime));
    span.tag("endTimeEpochMs", String.valueOf(endTime));
    span.tag("howMany", String.valueOf(howMany));
    // Add custom header to span tags if present
    userRequest.ifPresent(headerValue -> span.tag("userRequest", headerValue.toString()));

    // TODO: when MAX_SPANS is hit the results will look weird because the index is sorted in
    // reverse timestamp and the spans returned will be the tail. We should support sort in the
    // search request
    AstraSearch.SearchRequest.Builder searchRequestBuilder = AstraSearch.SearchRequest.newBuilder();
    AstraSearch.SearchResult searchResult =
        searcher.doSearch(
            searchRequestBuilder
                .setDataset(MATCH_ALL_DATASET)
                .setQuery(queryString)
                .setStartTimeEpochMs(startTime)
                .setEndTimeEpochMs(endTime)
                .setHowMany(howMany)
                .build());
    // we don't account for any failed nodes in the searchResult today
    List<LogWireMessage> messages = searchResultToLogWireMessage(searchResult);
    String output = convertLogWireMessageToZipkinSpan(messages);

    if (userRequest.isPresent() && userRequest.get() && !output.isEmpty()) {
      // Save the trace data to S3 if the custom header is present
      saveDataToS3(traceId, output);
    }
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  @VisibleForTesting
  protected static byte[] compressJsonData(String jsonData) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
      gzipOutputStream.write(jsonData.getBytes(StandardCharsets.UTF_8));
    }
    return byteArrayOutputStream.toByteArray();
  }

  @VisibleForTesting
  protected static String decompressJsonData(byte[] compressedData) {
    assert compressedData != null && compressedData.length > 0;

    try (GZIPInputStream gzipInputStream =
        new GZIPInputStream(new ByteArrayInputStream(compressedData))) {
      return new String(gzipInputStream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOG.error("Error decompressing JSON data", e);
      throw new RuntimeException("Failed to decompress JSON data", e);
    }
  }

  protected String retrieveDataFromS3(String traceId, long dataFreshnessInMinutes) {
    assert traceId != null && !traceId.isEmpty();

    try {
      // If data is fresh skip S3 retrieval
      long currentTime = Instant.now().toEpochMilli();

      HeadObjectResponse response =
          blobStore.getFileMetadata(
              String.format("%s/%s/traceData.json.gz", TRACE_CACHE_PREFIX, traceId));
      long lastModified = response.lastModified().toEpochMilli();
      if (currentTime - lastModified < dataFreshnessInMinutes * 60 * 1000) {
        return null; // Data is still getting updated, check on cache and live nodes
      }

      // Retrieve the compressed trace data from S3
      java.nio.file.Path tempDir = Files.createTempDirectory("");
      blobStore.download(String.format("%s/%s", TRACE_CACHE_PREFIX, traceId), tempDir);

      // Decompress the JSON data
      byte[] compressedData = Files.readAllBytes(tempDir.resolve("traceData.json.gz"));
      String jsonData = decompressJsonData(compressedData);

      LOG.info("Retrieved and decompressed trace data from S3 for traceId={}", traceId);
      // Process the jsonData as needed
      return jsonData;

    } catch (Exception e) {
      LOG.error("Error retrieving trace data from S3 for traceId={}", traceId, e);
    }
    return null;
  }

  protected void saveDataToS3(String traceId, String output) {
    assert traceId != null && !traceId.isEmpty();
    assert output != null && !output.isEmpty();

    try {
      // Compress the JSON data using GZIP
      byte[] compressedData = compressJsonData(output);

      // Create a temporary directory to store the trace data
      java.nio.file.Path tempDir = Files.createTempDirectory(traceId);
      java.nio.file.Path traceFile = tempDir.resolve("traceData.json.gz");
      Files.write(traceFile, compressedData);

      // Upload the compressed trace data to S3
      blobStore.upload(String.format("%s/%s", TRACE_CACHE_PREFIX, traceId), tempDir);

      // Clean up the temporary directory
      Files.deleteIfExists(traceFile);
      Files.deleteIfExists(tempDir);

      LOG.info("Compressed trace data saved to S3 for traceId={}", traceId);

    } catch (Exception e) {
      LOG.error("Error saving trace data to S3 for traceId={}", traceId, e);
      throw new RuntimeException("Failed to save trace data to S3", e);
    }
  }
}
