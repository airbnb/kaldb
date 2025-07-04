package com.slack.astra.zipkinApi;

import static com.slack.astra.zipkinApi.ZipkinService.TRACE_CACHE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.blobfs.S3TestUtils;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.server.AstraQueryServiceBase;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class ZipkinServiceTest {
  private static final String TEST_S3_BUCKET = "zipkin-service-test";
  @Mock private AstraQueryServiceBase searcher;
  private ZipkinService zipkinService;
  private AstraSearch.SearchResult mockSearchResult;
  private BlobStore mockBlobStore;

  private static final int defaultMaxSpans = 2000;
  private static final int defaultLookbackMins = 60 * 24 * 7;

  private static final long defaultDataFreshnessInMinutes = 5;

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(TEST_S3_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  @BeforeEach
  public void setup() throws IOException {
    MockitoAnnotations.openMocks(this);
    S3AsyncClient s3AsyncClient =
        S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
    BlobStore blobStore = new BlobStore(s3AsyncClient, TEST_S3_BUCKET);
    mockBlobStore = spy(blobStore);
    zipkinService =
        spy(
            new ZipkinService(
                searcher,
                mockBlobStore,
                defaultMaxSpans,
                defaultLookbackMins,
                defaultDataFreshnessInMinutes));
    // Build mockSearchResult
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonNode =
        objectMapper.readTree(Resources.getResource("zipkinApi/search_result.json"));
    String jsonString = objectMapper.writeValueAsString(jsonNode);
    AstraSearch.SearchResult.Builder builder = AstraSearch.SearchResult.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    mockSearchResult = builder.build();
  }

  @Test
  public void testGetTraceByTraceId_onlyTraceIdProvided() throws Exception {

    try (MockedStatic<Tracing> mockedTracing = mockStatic(Tracing.class)) {
      Tracer mockTracer = mock(Tracer.class);
      Span mockSpan = mock(Span.class);

      mockedTracing.when(Tracing::currentTracer).thenReturn(mockTracer);
      when(mockTracer.currentSpan()).thenReturn(mockSpan);
      String traceId = "test_trace_1";

      when(searcher.doSearch(any())).thenReturn(mockSearchResult);

      // Act
      HttpResponse response =
          zipkinService.getTraceByTraceId(
              traceId,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());
      AggregatedHttpResponse aggregatedResponse = response.aggregate().join();

      // Assert
      assertEquals(HttpStatus.OK, aggregatedResponse.status());
    }
  }

  @Test
  public void testGetTraceByTraceId_respectsDefaultMaxSpans() throws Exception {
    try (MockedStatic<Tracing> mockedTracing = mockStatic(Tracing.class)) {
      // Mocking Tracing and Span
      Tracer mockTracer = mock(Tracer.class);
      Span mockSpan = mock(Span.class);

      mockedTracing.when(Tracing::currentTracer).thenReturn(mockTracer);
      when(mockTracer.currentSpan()).thenReturn(mockSpan);

      String traceId = "test_trace_2";
      when(searcher.doSearch(any())).thenReturn(mockSearchResult);

      zipkinService.getTraceByTraceId(
          traceId,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty());

      verify(searcher)
          .doSearch(
              Mockito.argThat(
                  request ->
                      request.getHowMany() == defaultMaxSpans
                          && request.getQuery().contains("\"trace_id\":\"" + traceId + "\"")));
    }
  }

  @Test
  public void testGetTraceByTraceId_respectsMaxSpans() throws Exception {
    try (MockedStatic<Tracing> mockedTracing = mockStatic(Tracing.class)) {
      // Mocking Tracing and Span
      Tracer mockTracer = mock(Tracer.class);
      Span mockSpan = mock(Span.class);

      mockedTracing.when(Tracing::currentTracer).thenReturn(mockTracer);
      when(mockTracer.currentSpan()).thenReturn(mockSpan);

      String traceId = "test_trace_2";
      when(searcher.doSearch(any())).thenReturn(mockSearchResult);
      int maxSpansParam = 10000;

      zipkinService.getTraceByTraceId(
          traceId,
          Optional.empty(),
          Optional.empty(),
          Optional.of(maxSpansParam),
          Optional.empty(),
          Optional.empty());

      verify(searcher)
          .doSearch(
              Mockito.argThat(
                  request ->
                      request.getHowMany() == maxSpansParam
                          && request.getQuery().contains("\"trace_id\":\"" + traceId + "\"")));
    }
  }

  @Test
  public void testGetTraceByTraceId_respectUserRequest_upload_after_search() throws Exception {
    try (MockedStatic<Tracing> mockedTracing = mockStatic(Tracing.class)) {
      // Mocking Tracing and Span
      Tracer mockTracer = mock(Tracer.class);
      Span mockSpan = mock(Span.class);

      mockedTracing.when(Tracing::currentTracer).thenReturn(mockTracer);
      when(mockTracer.currentSpan()).thenReturn(mockSpan);

      String traceId = "test_trace_3";
      when(searcher.doSearch(any())).thenReturn(mockSearchResult);

      int maxSpansParam = 10000;
      boolean userRequest = true;

      zipkinService.getTraceByTraceId(
          traceId,
          Optional.empty(),
          Optional.empty(),
          Optional.of(maxSpansParam),
          Optional.of(userRequest),
          Optional.empty());

      verify(searcher)
          .doSearch(
              Mockito.argThat(
                  request ->
                      request.getHowMany() == maxSpansParam
                          && request.getQuery().contains("\"trace_id\":\"" + traceId + "\"")));

      verify(mockBlobStore).upload(Mockito.anyString(), Mockito.any(Path.class));

      Path directoryDownloaded = Files.createTempDirectory(traceId);
      mockBlobStore.download(
          String.format("%s/%s", TRACE_CACHE_PREFIX, traceId), directoryDownloaded);

      File[] filesDownloaded = directoryDownloaded.toFile().listFiles();
      assertThat(Objects.requireNonNull(filesDownloaded).length).isEqualTo(1);
      Path uploadedFile = filesDownloaded[0].toPath();
      assertThat(uploadedFile.toString()).endsWith("traceData.json.gz");
      String data = ZipkinService.decompressJsonData(Files.readAllBytes(uploadedFile));
      assertNotNull(data, "Decompressed data should not be null");
    }
  }

  @Test
  public void testGetTraceByTraceId_respectUserRequest_respectDataFreshness_skip_search()
      throws Exception {
    try (MockedStatic<Tracing> mockedTracing = mockStatic(Tracing.class)) {
      // Mocking Tracing and Span
      Tracer mockTracer = mock(Tracer.class);
      Span mockSpan = mock(Span.class);

      mockedTracing.when(Tracing::currentTracer).thenReturn(mockTracer);
      when(mockTracer.currentSpan()).thenReturn(mockSpan);

      String traceId = "test_trace_4";

      Path filePath = Paths.get(Resources.getResource("zipkinApi/traceData.json").toURI());

      byte[] data = ZipkinService.compressJsonData(Files.readString(filePath));
      Path tempDir = Files.createTempDirectory(traceId);
      Path traceFile = tempDir.resolve("traceData.json.gz");
      Files.write(traceFile, data);

      String path = String.format("%s/%s", TRACE_CACHE_PREFIX, traceId);
      mockBlobStore.upload(path, tempDir);

      int maxSpansParam = 10000;
      boolean userRequest = true;
      long dataFreshnessInMinutes = 0;

      zipkinService.getTraceByTraceId(
          traceId,
          Optional.empty(),
          Optional.empty(),
          Optional.of(maxSpansParam),
          Optional.of(userRequest),
          Optional.of(dataFreshnessInMinutes));

      verify(mockBlobStore).download(Mockito.anyString(), Mockito.any(Path.class));

      Path directoryDownloaded = Files.createTempDirectory(traceId);
      mockBlobStore.download(
          String.format("%s/%s", TRACE_CACHE_PREFIX, traceId), directoryDownloaded);

      File[] filesDownloaded = directoryDownloaded.toFile().listFiles();
      assertThat(Objects.requireNonNull(filesDownloaded).length).isEqualTo(1);
      Path uploadedFile = filesDownloaded[0].toPath();
      assertThat(uploadedFile.toString()).endsWith("traceData.json.gz");
      String returnData = ZipkinService.decompressJsonData(Files.readAllBytes(uploadedFile));
      assertNotNull(returnData, "Decompressed data should not be null");
    }
  }

  @Test
  public void testGetTraceByTraceId_respectUserRequest_respectDataFreshness_perform_search()
      throws Exception {
    try (MockedStatic<Tracing> mockedTracing = mockStatic(Tracing.class)) {
      // Mocking Tracing and Span
      Tracer mockTracer = mock(Tracer.class);
      Span mockSpan = mock(Span.class);

      mockedTracing.when(Tracing::currentTracer).thenReturn(mockTracer);
      when(mockTracer.currentSpan()).thenReturn(mockSpan);

      String traceId = "test_trace_5";

      Path filePath = Paths.get(Resources.getResource("zipkinApi/traceData.json").toURI());

      byte[] data = ZipkinService.compressJsonData(Files.readString(filePath));
      Path tempDir = Files.createTempDirectory(traceId);
      Path traceFile = tempDir.resolve("traceData.json.gz");
      Files.write(traceFile, data);

      String path = String.format("%s/%s", TRACE_CACHE_PREFIX, traceId);
      mockBlobStore.upload(path, tempDir);

      int maxSpansParam = 10000;
      boolean userRequest = true;
      long dataFreshnessInMinutes = 5;

      when(searcher.doSearch(any())).thenReturn(mockSearchResult);

      zipkinService.getTraceByTraceId(
          traceId,
          Optional.empty(),
          Optional.empty(),
          Optional.of(maxSpansParam),
          Optional.of(userRequest),
          Optional.of(dataFreshnessInMinutes));

      verify(mockBlobStore).getFileMetadata(Mockito.anyString());
      verify(mockBlobStore, never()).download(Mockito.anyString(), Mockito.any(Path.class));
      verify(mockBlobStore, times(2)).upload(Mockito.anyString(), Mockito.any(Path.class));

      Path directoryDownloaded = Files.createTempDirectory(traceId);
      mockBlobStore.download(
          String.format("%s/%s", TRACE_CACHE_PREFIX, traceId), directoryDownloaded);

      File[] filesDownloaded = directoryDownloaded.toFile().listFiles();
      assertThat(Objects.requireNonNull(filesDownloaded).length).isEqualTo(1);
      Path uploadedFile = filesDownloaded[0].toPath();
      assertThat(uploadedFile.toString()).endsWith("traceData.json.gz");
      String returnData = ZipkinService.decompressJsonData(Files.readAllBytes(uploadedFile));
      assertNotNull(returnData, "Decompressed data should not be null");
    }
  }

  @Test
  public void testCompressJsonData() throws Exception {
    // Arrange
    String jsonData =
        "[{\"id\":\"101\",\"traceId\":\"test_trace_789\",\"name\":\"test-span\",\"tags\":{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}}]";

    // Act
    byte[] compressedData = ZipkinService.compressJsonData(jsonData);

    // Assert
    assertNotNull(compressedData, "Compressed data should not be null");
    assertTrue(compressedData.length > 0, "Compressed data should not be empty");

    // Verify compression actually reduced size
    long originalSize = jsonData.getBytes(StandardCharsets.UTF_8).length;
    assertTrue(
        compressedData.length < originalSize, "Compressed data should be smaller than original");

    // Verify we can decompress and get the original data
    String decompressedData = ZipkinService.decompressJsonData(compressedData);
    assertEquals(jsonData, decompressedData, "Decompressed data should match original");
  }

  @Test
  public void testCompressJsonData_emptyString() throws Exception {
    // Arrange
    String jsonData = "";

    // Act
    byte[] compressedData = ZipkinService.compressJsonData(jsonData);

    // Assert
    assertNotNull(compressedData, "Compressed data should not be null");
    assertTrue(compressedData.length > 0, "Compressed data should not be empty for empty string");

    // Verify we can decompress and get the original data
    String decompressedData = ZipkinService.decompressJsonData(compressedData);
    assertEquals(
        jsonData, decompressedData, "Decompressed data should match original empty string");
  }

  @Test
  public void testDecompressJsonData() throws Exception {
    // Arrange
    String jsonData =
        "[{\"id\":\"101\",\"traceId\":\"test_trace_789\",\"name\":\"test-span\",\"tags\":{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}}]";
    byte[] compressedData = ZipkinService.compressJsonData(jsonData);

    // Act
    String decompressedData = ZipkinService.decompressJsonData(compressedData);

    // Assert
    assertNotNull(decompressedData, "Decompressed data should not be null");
    assertEquals(jsonData, decompressedData, "Decompressed data should match original");
  }

  @Test
  public void testDecompressJsonData_emptyString() throws Exception {
    // Arrange
    String jsonData = "";
    byte[] compressedData = ZipkinService.compressJsonData(jsonData);

    // Act
    String decompressedData = ZipkinService.decompressJsonData(compressedData);

    // Assert
    assertNotNull(decompressedData, "Decompressed data should not be null");
    assertEquals(
        jsonData, decompressedData, "Decompressed data should match original empty string");
  }

  @Test
  public void testRetrieveDataFromS3() throws Exception {
    // Arrange
    String traceId = "test_trace_123";
    String jsonData =
        "[{\"id\":\"101\",\"traceId\":\"test_trace_123\",\"name\":\"test-span\",\"tags\":{\"key1\":\"value1\",\"key2\":\"value2\"}}]";
    zipkinService.saveDataToS3(traceId, jsonData);

    // Act
    String retrievedData = zipkinService.retrieveDataFromS3(traceId, 0);

    // Assert
    assertNotNull(retrievedData, "Retrieved data should not be null");
    assertEquals(jsonData, retrievedData, "Retrieved data should match original");
  }

  @Test
  public void testRetrieveDataFromS3_nullTraceId_throwsException() {
    // Act & Assert
    try {
      zipkinService.retrieveDataFromS3(null, 5);
      assertTrue(false, "Should have thrown an exception");
    } catch (AssertionError e) {
      // Expected - assertion should fail for null traceId
    }
  }

  @Test
  public void testRetrieveDataFromS3_emptyTraceId_throwsException() {
    // Act & Assert
    try {
      zipkinService.retrieveDataFromS3("", 5);
      assertTrue(false, "Should have thrown an exception");
    } catch (AssertionError e) {
      // Expected - assertion should fail for empty traceId
    }
  }

  @Test
  public void testSaveDataToS3() throws Exception {
    // Arrange
    String traceId = "test_trace_456";
    String jsonData =
        "[{\"id\":\"101\",\"traceId\":\"test_trace_456\",\"name\":\"test-span\",\"tags\":{\"key1\":\"value1\",\"key2\":\"value2\"}}]";

    // Act
    zipkinService.saveDataToS3(traceId, jsonData);

    // Assert
    verify(mockBlobStore).upload(anyString(), any(Path.class));

    // Assert
    Path directoryDownloaded = Files.createTempDirectory(traceId);
    mockBlobStore.download(
        String.format("%s/%s", TRACE_CACHE_PREFIX, traceId), directoryDownloaded);
    File[] filesDownloaded = directoryDownloaded.toFile().listFiles();
    assertThat(Objects.requireNonNull(filesDownloaded).length).isEqualTo(1);

    Path uploadedFile = filesDownloaded[0].toPath();
    assertThat(uploadedFile.toString()).endsWith("traceData.json.gz");

    // Verify the file is actually compressed (smaller than original)
    long compressedSize = Files.size(uploadedFile);
    long originalSize = jsonData.getBytes(StandardCharsets.UTF_8).length;
    assertTrue(compressedSize < originalSize, "Compressed file should be smaller than original");

    // Verify we can decompress and get the original
    String decompressedData = ZipkinService.decompressJsonData(Files.readAllBytes(uploadedFile));
    assertEquals(jsonData, decompressedData, "Decompressed data should match original");
  }

  @Test
  public void testSaveDataToS3_nullTraceId_throwsException() {
    String jsonData = "[{\"id\":\"101\"}]";

    // Act & Assert
    try {
      zipkinService.saveDataToS3(null, jsonData);
      assertTrue(false, "Should have thrown an exception");
    } catch (AssertionError e) {
      // Expected - assertion should fail for null traceId
    }
  }

  @Test
  public void testSaveDataToS3_emptyTraceId_throwsException() {
    // Arrange
    String jsonData = "[{\"id\":\"101\"}]";

    // Act & Assert
    try {
      zipkinService.saveDataToS3("", jsonData);
      assertTrue(false, "Should have thrown an exception");
    } catch (AssertionError e) {
      // Expected - assertion should fail for empty traceId
    }
  }

  @Test
  public void testSaveDataToS3_nullData_throwsException() {

    // Act & Assert
    try {
      zipkinService.saveDataToS3("test_trace", null);
      assertTrue(false, "Should have thrown an exception");
    } catch (AssertionError e) {
      // Expected - assertion should fail for null data
    }
  }
}
