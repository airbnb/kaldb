package com.slack.astra.bulkIngestApi;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.slack.service.murron.trace.Trace;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WALBatchSerializerTest {

  static String INDEX_NAME = "testtransactionindex";
  private static final Logger LOG = LoggerFactory.getLogger(WALBatchSerializerTest.class);

  @Test
  public void testCompression() throws Exception {

    // Test 1: Empty spans
    Map<String, List<Trace.Span>> emptyspans = Map.of(INDEX_NAME, List.of());
    byte[] emptyCompressedData = WALBatchSerializer.serialize(emptyspans);
    assertThat(emptyCompressedData.length).isGreaterThan(0);

    // Test 2: Single span
    Trace.Span singleSpan = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("test1")).build();
    Map<String, List<Trace.Span>> singleSpanDocs = Map.of(INDEX_NAME, List.of(singleSpan));
    byte[] singleSpanCompressedData = WALBatchSerializer.serialize(singleSpanDocs);
    assertThat(singleSpanCompressedData.length).isGreaterThan(0);

    // Test 3: Multiple spans with repeated data
    String repeatdata = "testdata".repeat(1000); // Create a large string to test compression

    Trace.Span span1 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("test1")).build();

    Trace.Span span2 =
        Trace.Span.newBuilder()
            .setId(ByteString.copyFromUtf8("test2"))
            .setTraceId(ByteString.copyFromUtf8(repeatdata))
            .build();

    Trace.Span span3 =
        Trace.Span.newBuilder()
            .setId(ByteString.copyFromUtf8("test3"))
            .setTraceId(ByteString.copyFromUtf8(repeatdata))
            .build();

    // create a map with multiple spans
    Map<String, List<Trace.Span>> indexDocs = Map.of(INDEX_NAME, List.of(span1, span2, span3));
    byte[] compressedData = WALBatchSerializer.serialize(indexDocs);

    int uncompressedSize = 0;

    // Calculate the uncompressed size
    for (List<Trace.Span> spans : indexDocs.values()) {
      for (Trace.Span span : spans) {
        uncompressedSize += span.getSerializedSize();
      }
    }

    // Verify that the compressed data is smaller than the uncompressed size
    assertThat(compressedData.length).isLessThan(uncompressedSize);
    LOG.debug(
        "Compression ratio: {} -> {} bytes ({}% reduction)",
        uncompressedSize,
        compressedData.length,
        ((uncompressedSize - compressedData.length) * 100) / uncompressedSize);
  }

  @Test
  public void testWALBatchSerialization_RoundTrip() throws Exception {

    // Test 1: Empty spans round-trip
    Map<String, List<Trace.Span>> emptyDocs = Map.of(INDEX_NAME, List.of());
    byte[] emptyCompressed = WALBatchSerializer.serialize(emptyDocs);
    Map<String, List<Trace.Span>> emptyDecompressed =
        WALBatchSerializer.deserialize(emptyCompressed);

    assertThat(emptyDecompressed).hasSize(1);
    assertThat(emptyDecompressed.get(INDEX_NAME)).isEmpty();

    // Test 2: Single span round-trip
    Trace.Span singleSpan = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("test1")).build();
    Map<String, List<Trace.Span>> singleDocs = Map.of(INDEX_NAME, List.of(singleSpan));
    byte[] singleCompressed = WALBatchSerializer.serialize(singleDocs);
    Map<String, List<Trace.Span>> singleDecompressed =
        WALBatchSerializer.deserialize(singleCompressed);

    assertThat(singleDecompressed).hasSize(1);
    assertThat(singleDecompressed.get(INDEX_NAME)).hasSize(1);
    assertThat(singleDecompressed.get(INDEX_NAME).get(0).getId()).isEqualTo(singleSpan.getId());

    // Test 3: Multiple spans round-trip
    Trace.Span span1 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("test1")).build();
    Trace.Span span2 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("test2")).build();
    Trace.Span span3 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("test3")).build();

    Map<String, List<Trace.Span>> multipleDocs = Map.of(INDEX_NAME, List.of(span1, span2, span3));
    byte[] multipleCompressed = WALBatchSerializer.serialize(multipleDocs);
    Map<String, List<Trace.Span>> multipleDecompressed =
        WALBatchSerializer.deserialize(multipleCompressed);

    assertThat(multipleDecompressed).hasSize(1);
    assertThat(multipleDecompressed.get(INDEX_NAME)).hasSize(3);

    List<Trace.Span> originalSpans = multipleDocs.get(INDEX_NAME);
    List<Trace.Span> decompressedSpans = multipleDecompressed.get(INDEX_NAME);

    for (int i = 0; i < originalSpans.size(); i++) {
      assertThat(decompressedSpans.get(i).getId()).isEqualTo(originalSpans.get(i).getId());
    }

    // Test 4: Multiple indexes round-trip
    Trace.Span indexASpan =
        Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("indexA-span")).build();
    Trace.Span indexBSpan =
        Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("indexB-span")).build();

    Map<String, List<Trace.Span>> multiIndexDocs =
        Map.of(
            "indexA", List.of(indexASpan),
            "indexB", List.of(indexBSpan));

    byte[] multiIndexCompressed = WALBatchSerializer.serialize(multiIndexDocs);
    Map<String, List<Trace.Span>> multiIndexDecompressed =
        WALBatchSerializer.deserialize(multiIndexCompressed);

    assertThat(multiIndexDecompressed).hasSize(2);
    assertThat(multiIndexDecompressed.get("indexA")).hasSize(1);
    assertThat(multiIndexDecompressed.get("indexB")).hasSize(1);
    assertThat(multiIndexDecompressed.get("indexA").get(0).getId()).isEqualTo(indexASpan.getId());
    assertThat(multiIndexDecompressed.get("indexB").get(0).getId()).isEqualTo(indexBSpan.getId());

    LOG.debug("All round-trip serialization tests passed!");
  }
}
