package com.slack.astra.schema;

import static com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParser.convertRequestToDocument;
import static com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParser.fromIngestDocument;
import static com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParserTest.getIndexRequestBytes;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.schema.SchemaAwareLogDocumentBuilderImpl.build;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.Fail.fail;

import com.google.common.io.Files;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParser;
import com.slack.astra.logstore.LogStore;
import com.slack.astra.logstore.LuceneIndexStoreConfig;
import com.slack.astra.logstore.LuceneIndexStoreImpl;
import com.slack.astra.logstore.schema.ReservedFields;
import com.slack.astra.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.astra.metadata.schema.FieldType;
import com.slack.astra.metadata.schema.SchemaUtil;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.writer.SpanFormatter;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.ingest.IngestDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpanFormatterWithSchemaTest {

  private static final Logger LOG = LoggerFactory.getLogger(SpanFormatterWithSchemaTest.class);

  static Schema.IngestSchema schema;

  private SimpleMeterRegistry meterRegistry;
  public File tempFolder;
  public LogStore logStore;

  @BeforeEach
  public void setup() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    this.tempFolder = Files.createTempDir();

    LuceneIndexStoreConfig indexStoreCfg =
        new LuceneIndexStoreConfig(
            Duration.of(5, ChronoUnit.MINUTES),
            Duration.of(5, ChronoUnit.MINUTES),
            tempFolder.getCanonicalPath(),
            false);

    SchemaAwareLogDocumentBuilderImpl dropFieldBuilder =
        build(
            SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.DROP_FIELD, true, meterRegistry);
    this.logStore = new LuceneIndexStoreImpl(indexStoreCfg, dropFieldBuilder, meterRegistry);
  }

  @AfterEach
  public void tearDown() {
    try {
      if (logStore != null) {
        logStore.close();
      }
      FileUtils.deleteDirectory(tempFolder);
    } catch (Exception e) {
      LOG.error("error closing resources", e);
    }
  }

  @BeforeAll
  public static void initializeSchema() {
    Map<String, Schema.SchemaField> fields = new HashMap<>();
    fields.put(
        "host", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.KEYWORD).build());
    fields.put(
        "message", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.TEXT).build());
    fields.put("ip", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.IP).build());
    fields.put(
        "myTimestamp",
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.DATE).build());
    fields.put(
        "success", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.BOOLEAN).build());
    fields.put(
        "cost", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.DOUBLE).build());
    fields.put(
        "amount", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.FLOAT).build());
    fields.put(
        "amount_half_float",
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.HALF_FLOAT).build());
    fields.put(
        "value", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.INTEGER).build());
    fields.put(
        "count", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.LONG).build());
    fields.put(
        "count_scaled_long",
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.SCALED_LONG).build());
    fields.put(
        "count_short",
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.SHORT).build());
    fields.put(
        "bucket", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.BINARY).build());

    schema = Schema.IngestSchema.newBuilder().putAllFields(fields).build();
  }

  @Test
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public void canParseSimpleMapValues() throws Exception {
    List<Trace.KeyValue> list =
        SpanFormatter.convertKVtoProtoDefault(
            "item1",
            Map.of("subitem1", "value1", "subitem2", Map.of("subsubitem1", "subsubvalue1")),
            schema);

    assertThat(list.size()).isEqualTo(2);
    assertThat(
            list.stream()
                .filter(item -> item.getKey().equals("item1.subitem1"))
                .findFirst()
                .get()
                .getVStr())
        .isEqualTo("value1");
    assertThat(
            list.stream()
                .filter(item -> item.getKey().equals("item1.subitem2.subsubitem1"))
                .findFirst()
                .get()
                .getVStr())
        .isEqualTo("subsubvalue1");
  }

  @Test
  public void parseIndexRequestWithNullValues() throws Exception {
    final File schemaFile =
        new File(getClass().getClassLoader().getResource("schema/test_schema.yaml").getFile());
    Schema.IngestSchema schema = SchemaUtil.parseSchema(schemaFile.toPath());

    byte[] rawRequest = getIndexRequestBytes("index_with_null_values");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    AssertionsForClassTypes.assertThat(indexRequests.size()).isEqualTo(1);

    Trace.Span doc =
        BulkApiRequestParser.fromIngestDocument(
            convertRequestToDocument(indexRequests.get(0)), schema);
    assertThat(doc.getId().toStringUtf8()).isEqualTo("1");
    assertThat(doc.getParentId().toStringUtf8()).isEqualTo("1");

    List<String> docTags =
        doc.getTagsList().stream().map(Trace.KeyValue::getKey).collect(Collectors.toList());
    assertThat(docTags.size()).isEqualTo(2); // username,service_name
    assertThat(docTags.contains("bucket")).isFalse();
  }

  @Test
  public void parseIndexRequestToTraceProtoTest() throws Exception {
    final File schemaFile =
        new File(getClass().getClassLoader().getResource("schema/test_schema.yaml").getFile());
    Schema.IngestSchema schema = SchemaUtil.parseSchema(schemaFile.toPath());

    byte[] rawRequest = getIndexRequestBytes("index_all_schema_fields");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    AssertionsForClassTypes.assertThat(indexRequests.size()).isEqualTo(2);

    Trace.Span doc1 =
        BulkApiRequestParser.fromIngestDocument(
            convertRequestToDocument(indexRequests.get(0)), schema);
    Trace.Span doc2 =
        BulkApiRequestParser.fromIngestDocument(
            convertRequestToDocument(indexRequests.get(1)), schema);
    assertThat(doc1.getId().toStringUtf8()).isEqualTo("1");
    assertThat(doc2.getId().toStringUtf8()).isEqualTo("2");

    String[] fields =
        new String[] {
          "host",
          "message",
          "message.keyword",
          "ip",
          "my_date",
          "success",
          "cost",
          "amount",
          "amount_half_float",
          "value",
          "count",
          "count_scaled_long",
          "count_short",
          "bucket"
        };
    List<String> doc1DefinedFields = new ArrayList<>(Arrays.stream(fields).toList());
    List<String> doc1Tags =
        doc1.getTagsList().stream().map(Trace.KeyValue::getKey).collect(Collectors.toList());

    assertThat(doc1DefinedFields.removeAll(doc1Tags)).isEqualTo(true);
    assertThat(doc1DefinedFields.size()).isEqualTo(0);

    // reset
    doc1DefinedFields = new ArrayList<>(Arrays.stream(fields).toList());
    doc1Tags = doc1.getTagsList().stream().map(Trace.KeyValue::getKey).collect(Collectors.toList());

    assertThat(doc1Tags.removeAll(doc1DefinedFields)).isEqualTo(true);
    assertThat(doc1Tags.size()).isEqualTo(1);
    assertThat(doc1Tags.get(0)).isEqualTo("service_name");

    List<String> doc2Tags =
        doc2.getTagsList().stream().map(Trace.KeyValue::getKey).collect(Collectors.toList());
    assertThat(doc2Tags.size()).isEqualTo(3); // service_name and ip
    assertThat(doc2Tags.contains("service_name")).isEqualTo(true);
    assertThat(doc2Tags.contains("ip")).isEqualTo(true);
    assertThat(doc2Tags.contains("username")).isEqualTo(true);
    assertThat(doc2Tags.contains("bucket")).isFalse();
    assertThat(doc2.getParentId().toStringUtf8()).isEqualTo("1");
    assertThat(doc2.getTraceId().toStringUtf8()).isEqualTo("2");
    assertThat(doc2.getName()).isEqualTo("check");
    assertThat(doc2.getDuration()).isEqualTo(20000L);
  }

  @Test
  public void testSimpleSchema() {
    Trace.KeyValue kv = SpanFormatter.convertKVtoProto("host", "host1", schema).get(0);
    assertThat(kv.getVStr()).isEqualTo("host1");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("message", "my message", schema).get(0);
    assertThat(kv.getVStr()).isEqualTo("my message");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("ip", "8.8.8.8", schema).get(0);
    assertThat(kv.getVStr()).isEqualTo("8.8.8.8");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.IP);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    String myTimestamp = "2021-01-01T00:00:00Z";
    Instant myDateInstant = Instant.parse(myTimestamp);
    Timestamp myDateTimestamp =
        Timestamp.newBuilder()
            .setSeconds(myDateInstant.getEpochSecond())
            .setNanos(myDateInstant.getNano())
            .build();
    kv = SpanFormatter.convertKVtoProto("myTimestamp", myTimestamp, schema).get(0);
    assertThat(kv.getVDate()).isEqualTo(myDateTimestamp);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    myTimestamp = "2021-01-01T00:00:00Z";
    myDateInstant = Instant.parse(myTimestamp);
    myDateTimestamp =
        Timestamp.newBuilder()
            .setSeconds(myDateInstant.getEpochSecond())
            .setNanos(myDateInstant.getNano())
            .build();
    kv = SpanFormatter.convertKVtoProto("myTimestamp", myTimestamp, schema).get(0);
    assertThat(kv.getVDate()).isEqualTo(myDateTimestamp);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.DATE);

    kv = SpanFormatter.convertKVtoProto("success", "true", schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.BOOLEAN);
    assertThat(kv.getVBool()).isEqualTo(true);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("success", true, schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.BOOLEAN);
    assertThat(kv.getVBool()).isEqualTo(true);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("cost", "10.0", schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.DOUBLE);
    assertThat(kv.getVFloat64()).isEqualTo(10.0);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("cost", 10.0, schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.DOUBLE);
    assertThat(kv.getVFloat64()).isEqualTo(10.0);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("amount", "10.0", schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.FLOAT);
    assertThat(kv.getVFloat32()).isEqualTo(10.0f);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("amount", 10.0, schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.FLOAT);
    assertThat(kv.getVFloat32()).isEqualTo(10.0f);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("amount_half_float", "10.0", schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.HALF_FLOAT);
    assertThat(kv.getVFloat32()).isEqualTo(10.0f);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("amount_half_float", 10.0, schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.HALF_FLOAT);
    assertThat(kv.getVFloat32()).isEqualTo(10.0f);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("value", "10", schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.INTEGER);
    assertThat(kv.getVInt32()).isEqualTo(10);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("value", 10, schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.INTEGER);
    assertThat(kv.getVInt32()).isEqualTo(10);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("count", "10", schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.LONG);
    assertThat(kv.getVInt64()).isEqualTo(10L);

    kv = SpanFormatter.convertKVtoProto("count", 10, schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.LONG);
    assertThat(kv.getVInt64()).isEqualTo(10L);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("count_scaled_long", "10", schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.SCALED_LONG);
    assertThat(kv.getVInt64()).isEqualTo(10);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("count_scaled_long", 10, schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.SCALED_LONG);
    assertThat(kv.getVInt64()).isEqualTo(10L);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("count_short", "10", schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.SHORT);
    assertThat(kv.getVInt32()).isEqualTo(10L);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("count_short", 10, schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.SHORT);
    assertThat(kv.getVInt32()).isEqualTo(10);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.IN_SCHEMA_INDEX);

    kv = SpanFormatter.convertKVtoProto("bucket", "e30=", schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.BINARY);
    assertThat(kv.getVBinary().toStringUtf8()).isEqualTo("e30=");
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.DO_NOT_INDEX);
  }

  @Test
  public void testKeyValueWithWrongValues() {
    Trace.KeyValue kv = SpanFormatter.convertKVtoProto("success", "notBoolean", schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.BOOLEAN);
    assertThat(kv.getVBool()).isEqualTo(false);

    kv = SpanFormatter.convertKVtoProto("cost", "hello", schema).get(0);
    assertThat(kv.getKey()).isEqualTo("failed_cost");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("hello");

    kv = SpanFormatter.convertKVtoProto("amount", "hello", schema).get(0);
    assertThat(kv.getKey()).isEqualTo("failed_amount");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("hello");

    kv = SpanFormatter.convertKVtoProto("amount_half_float", "half_float_value", schema).get(0);
    assertThat(kv.getKey()).isEqualTo("failed_amount_half_float");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("half_float_value");

    kv = SpanFormatter.convertKVtoProto("value", "int_value", schema).get(0);
    assertThat(kv.getKey()).isEqualTo("failed_value");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("int_value");

    kv = SpanFormatter.convertKVtoProto("count", "long_value", schema).get(0);
    assertThat(kv.getKey()).isEqualTo("failed_count");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("long_value");

    kv = SpanFormatter.convertKVtoProto("count_scaled_long", "scaled_long_val", schema).get(0);
    assertThat(kv.getKey()).isEqualTo("failed_count_scaled_long");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("scaled_long_val");

    kv = SpanFormatter.convertKVtoProto("count_short", "my_short-Val", schema).get(0);
    assertThat(kv.getKey()).isEqualTo("failed_count_short");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("my_short-Val");
  }

  @Test
  public void testSimpleWithoutSchema() {

    Schema.IngestSchema schema = Schema.IngestSchema.getDefaultInstance();
    Trace.KeyValue kv = SpanFormatter.convertKVtoProto("host", "host1", schema).get(0);
    assertThat(kv.getVStr()).isEqualTo("host1");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.DYNAMIC_INDEX);

    kv = SpanFormatter.convertKVtoProto("ip", "8.8.8.8", schema).get(0);
    assertThat(kv.getVStr()).isEqualTo("8.8.8.8");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.DYNAMIC_INDEX);

    kv = SpanFormatter.convertKVtoProto("myTimestamp", "2021-01-01T00:00:00Z", schema).get(0);
    assertThat(kv.getVStr()).isEqualTo("2021-01-01T00:00:00Z");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.DYNAMIC_INDEX);

    kv = SpanFormatter.convertKVtoProto("success", "true", schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("true");
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.DYNAMIC_INDEX);

    kv = SpanFormatter.convertKVtoProto("success", true, schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.BOOLEAN);
    assertThat(kv.getVBool()).isEqualTo(true);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.DYNAMIC_INDEX);

    kv = SpanFormatter.convertKVtoProto("cost", "10.0", schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("10.0");
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.DYNAMIC_INDEX);

    kv = SpanFormatter.convertKVtoProto("amount", 10.0f, schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.FLOAT);
    assertThat(kv.getVFloat32()).isEqualTo(10.0f);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.DYNAMIC_INDEX);

    kv = SpanFormatter.convertKVtoProto("cost", 10.0, schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.DOUBLE);
    assertThat(kv.getVFloat64()).isEqualTo(10.0);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.DYNAMIC_INDEX);

    kv = SpanFormatter.convertKVtoProto("value", 10, schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.INTEGER);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.DYNAMIC_INDEX);

    kv = SpanFormatter.convertKVtoProto("count", 10L, schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.LONG);
    assertThat(kv.getVInt64()).isEqualTo(10L);
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.DYNAMIC_INDEX);

    kv = SpanFormatter.convertKVtoProto("bucket", "e30=", schema).get(0);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("e30=");
    assertThat(kv.getIndexSignal()).isEqualTo(Trace.IndexSignal.DYNAMIC_INDEX);
  }

  @Test
  public void testDuplicateFieldAsTag() {
    Trace.Span span =
        Trace.Span.newBuilder()
            .setName("service1")
            .setId(ByteString.copyFrom("123".getBytes()))
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setKey("name")
                    .setVStr("service2")
                    .setFieldType(Schema.SchemaFieldType.KEYWORD))
            .build();

    logStore.addMessage(span);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, meterRegistry)).isEqualTo(0);

    // duplicate tags
    span =
        Trace.Span.newBuilder()
            .setName("service1")
            .setId(ByteString.copyFrom("123".getBytes()))
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setKey("tag1")
                    .setVStr("value1")
                    .setFieldType(Schema.SchemaFieldType.KEYWORD))
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setKey("tag1")
                    .setVStr("value1")
                    .setFieldType(Schema.SchemaFieldType.KEYWORD))
            .build();

    logStore.addMessage(span);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
  }

  @Test
  public void testTraceProtoToLuceneDocumentTest() throws Exception {
    final File schemaFile =
        new File(getClass().getClassLoader().getResource("schema/test_schema.yaml").getFile());
    Schema.IngestSchema schema = SchemaUtil.parseSchema(schemaFile.toPath());

    byte[] rawRequest = getIndexRequestBytes("index_all_schema_fields");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(2);
    IngestDocument ingestDocument = convertRequestToDocument(indexRequests.get(0));

    Trace.Span span = fromIngestDocument(ingestDocument, schema);
    assertThat(span.getTagsCount()).isEqualTo(15);
    Map<String, Trace.KeyValue> tags =
        span.getTagsList().stream()
            .map(kv -> Map.entry(kv.getKey(), kv))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    assertThat(tags.get("amount").getVFloat32()).isEqualTo(1.1f);
    assertThat(tags.get("cost").getVFloat64()).isEqualTo(4.0);
    assertThat(tags.get("ip").getVStr()).isEqualTo("192.168.1.1");
    assertThat(tags.get("count").getVInt64()).isEqualTo(3);
    assertThat(tags.get("count_short").getVInt32()).isEqualTo(10);
    Instant myDateInstant = Instant.parse("2014-09-01T12:00:00Z");
    Timestamp myDateTimestamp =
        Timestamp.newBuilder()
            .setSeconds(myDateInstant.getEpochSecond())
            .setNanos(myDateInstant.getNano())
            .build();
    assertThat(tags.get("my_date").getVDate()).isEqualTo(myDateTimestamp);
    assertThat(tags.get("bucket").getVInt32()).isEqualTo(20);
    assertThat(tags.get("success").getVBool()).isEqualTo(true);
    assertThat(tags.get("count_scaled_long").getVInt64()).isEqualTo(80);
    assertThat(tags.get("host").getVStr()).isEqualTo("host1");
    assertThat(tags.get("amount_half_float").getVFloat32()).isEqualTo(1.2f);
    assertThat(tags.get("value").getVInt32()).isEqualTo(42);
    assertThat(tags.get("service_name").getVStr()).isEqualTo("test");
    assertThat(tags.get("message").getVStr()).isEqualTo("foo bar");
    assertThat(tags.get("message.keyword").getVStr()).isEqualTo("foo bar");

    SchemaAwareLogDocumentBuilderImpl dropFieldBuilder =
        build(
            SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.DROP_FIELD, true, meterRegistry);
    Document luceneDocument = dropFieldBuilder.fromMessage(span);
    // message is a tag, but is a TEXT field in schema, so it is indexed and not doc values
    // 13 tags X 2(DV and indexed) + (message,message.keyword,_id,_timesinceepoch,_index) x2 +
    // _source + _all

    assertThat(luceneDocument.getFields().size()).isEqualTo(37);

    for (Map.Entry<String, Trace.KeyValue> keyAndTag : tags.entrySet()) {
      String key = keyAndTag.getKey();
      Trace.KeyValue tag = keyAndTag.getValue();
      // Purposely against FieldType to ensure that conversion also works as expected
      FieldType fieldType = FieldType.valueOf(tag.getFieldType().name());
      // list since same field will have two entries - indexed and docvalues
      Arrays.asList(luceneDocument.getFields(key))
          .forEach(
              field -> {
                if (fieldType == FieldType.TEXT) {
                  assertThat(tag.getFieldType()).isEqualTo(Schema.SchemaFieldType.TEXT);
                  assertThat(field.stringValue()).isEqualTo(tag.getVStr());
                } else if (fieldType == FieldType.KEYWORD) {
                  assertThat(tag.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
                  if (field instanceof SortedDocValuesField) {
                    assertThat(field.binaryValue().utf8ToString()).isNotNull();
                  } else {
                    assertThat(field.stringValue()).isEqualTo(tag.getVStr());
                  }
                } else if (fieldType == FieldType.BOOLEAN) {
                  assertThat(tag.getFieldType()).isEqualTo(Schema.SchemaFieldType.BOOLEAN);

                  if (field instanceof SortedNumericDocValuesField) {
                    assertThat(field.numericValue()).isEqualTo(1L);
                  } else {
                    assertThat(field.binaryValue().utf8ToString()).isEqualTo("T");
                  }
                } else if (fieldType == FieldType.DATE) {
                  assertThat(tag.getFieldType()).isEqualTo(Schema.SchemaFieldType.DATE);
                  assertThat(field.numericValue().longValue()).isEqualTo(tag.getVInt64());
                } else if (fieldType == FieldType.DOUBLE) {
                  assertThat(tag.getFieldType()).isEqualTo(Schema.SchemaFieldType.DOUBLE);
                  if (field instanceof DoubleDocValuesField) {
                    // reverse of Double.doubleToRawLongBits(value)
                    assertThat(Double.longBitsToDouble(field.numericValue().longValue()))
                        .isEqualTo(tag.getVFloat64());
                  } else {
                    assertThat(field.numericValue().doubleValue()).isEqualTo(tag.getVFloat64());
                  }
                } else if (fieldType == FieldType.FLOAT) {
                  assertThat(tag.getFieldType()).isEqualTo(Schema.SchemaFieldType.FLOAT);
                  if (field instanceof FloatDocValuesField) {
                    // reverse of Float.floatToRawIntBits(value)
                    assertThat(Float.intBitsToFloat(field.numericValue().intValue()))
                        .isEqualTo(tag.getVFloat32());
                  } else {
                    assertThat(field.numericValue().floatValue()).isEqualTo(tag.getVFloat32());
                  }
                } else if (fieldType == FieldType.INTEGER) {
                  assertThat(tag.getFieldType()).isEqualTo(Schema.SchemaFieldType.INTEGER);
                  assertThat(field.numericValue().intValue()).isEqualTo(tag.getVInt32());
                } else if (fieldType == FieldType.LONG) {
                  assertThat(tag.getFieldType()).isEqualTo(Schema.SchemaFieldType.LONG);
                  assertThat(field.numericValue().longValue()).isEqualTo(tag.getVInt64());
                } else if (fieldType == FieldType.HALF_FLOAT) {
                  assertThat(tag.getFieldType()).isEqualTo(Schema.SchemaFieldType.HALF_FLOAT);
                  if (field instanceof HalfFloatPoint) {
                    assertThat(Math.abs(field.numericValue().floatValue() - tag.getVFloat32()))
                        .isLessThan(0.001F);
                  } else {
                    assertThat(
                            Math.abs(
                                HalfFloatPoint.sortableShortToHalfFloat(
                                        field.numericValue().shortValue())
                                    - tag.getVFloat32()))
                        .isLessThan(0.001F);
                  }
                } else if (fieldType == FieldType.SCALED_LONG) {
                  assertThat(tag.getFieldType()).isEqualTo(Schema.SchemaFieldType.SCALED_LONG);
                  assertThat(field.numericValue().longValue()).isEqualTo(tag.getVInt64());
                } else if (fieldType == FieldType.SHORT) {
                  assertThat(tag.getFieldType()).isEqualTo(Schema.SchemaFieldType.SHORT);
                  assertThat(field.numericValue().intValue()).isEqualTo(tag.getVInt32());
                } else if (fieldType == FieldType.BINARY) {
                  assertThat(tag.getFieldType()).isEqualTo(Schema.SchemaFieldType.BINARY);
                  assertThat(field.binaryValue().utf8ToString()).isEqualTo(tag.getVStr());
                } else if (fieldType == FieldType.IP) {
                  assertThat(tag.getFieldType()).isEqualTo(Schema.SchemaFieldType.IP);
                  if (field instanceof SortedDocValuesField) {
                    assertThat(InetAddressPoint.decode(field.binaryValue().bytes).getHostName())
                        .isEqualTo(tag.getVStr());
                  } else {
                    assertThat(InetAddressPoint.decode(field.binaryValue().bytes).getHostName())
                        .isEqualTo(tag.getVStr());
                  }
                } else if (fieldType == FieldType.BYTE) {
                  assertThat(tag.getFieldType()).isEqualTo(Schema.SchemaFieldType.BYTE);
                  assertThat(field.numericValue().byteValue()).isEqualTo((byte) tag.getVInt32());
                } else {
                  fail("fieldType not defined for field: " + tag);
                }
              });
    }
  }

  @Test
  public void testValidateTimestamp() {
    Assertions.assertThat(SpanFormatter.isValidTimestamp(Instant.ofEpochMilli(0))).isFalse();
    Assertions.assertThat(
            SpanFormatter.isValidTimestamp(Instant.now().plus(61, ChronoUnit.MINUTES)))
        .isFalse();
    Assertions.assertThat(
            SpanFormatter.isValidTimestamp(Instant.now().plus(59, ChronoUnit.MINUTES)))
        .isTrue();
    Assertions.assertThat(
            SpanFormatter.isValidTimestamp(Instant.now().minus(167, ChronoUnit.HOURS)))
        .isTrue();
    Assertions.assertThat(
            SpanFormatter.isValidTimestamp(Instant.now().minus(169, ChronoUnit.HOURS)))
        .isFalse();
  }

  @Test
  public void testDurationParsing() throws IOException {
    String inputDocuments =
        """
        { "index" : { "_index" : "test", "_id" : "1" } }
        { "duration" : 0.20389771461486816}
        { "index" : { "_index" : "test", "_id" : "2" } }
        { "duration" : "0.20389771461486816 ms"}
        { "index" : { "_index" : "test", "_id" : "3" } }
        { "duration" : 1715025939317}
                """;

    byte[] rawRequest = inputDocuments.getBytes(StandardCharsets.UTF_8);

    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    Assertions.assertThat(indexRequests.size()).isEqualTo(3);

    IngestDocument ingestDocument = convertRequestToDocument(indexRequests.get(0));
    Trace.Span span = fromIngestDocument(ingestDocument, ReservedFields.START_SCHEMA);
    assertThat(span.getDuration()).isEqualTo(0L);

    ingestDocument = convertRequestToDocument(indexRequests.get(1));
    span = fromIngestDocument(ingestDocument, ReservedFields.START_SCHEMA);
    assertThat(span.getDuration()).isEqualTo(0L);

    ingestDocument = convertRequestToDocument(indexRequests.get(2));
    span = fromIngestDocument(ingestDocument, ReservedFields.START_SCHEMA);
    assertThat(span.getDuration()).isEqualTo(1715025939317L);
  }

  @Test
  public void testNestedFields() throws Exception {
    SchemaAwareLogDocumentBuilderImpl dropFieldBuilder =
        build(
            SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.DROP_FIELD, true, meterRegistry);
    final File schemaFile =
        new File(getClass().getClassLoader().getResource("schema/test_schema.yaml").getFile());
    Schema.IngestSchema schema = SchemaUtil.parseSchema(schemaFile.toPath());

    byte[] rawRequest = getIndexRequestBytes("index_nested_fields");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(3);

    Trace.Span span1 = fromIngestDocument(convertRequestToDocument(indexRequests.get(0)), schema);
    assertThat(span1.getTagsCount()).isEqualTo(3);
    Map<String, Trace.KeyValue> tags1 =
        span1.getTagsList().stream()
            .map(kv -> Map.entry(kv.getKey(), kv))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    assertThat(tags1.get("list_field").getVStr()).isEqualTo("[host1, host2]");

    Document luceneDocument1 = dropFieldBuilder.fromMessage(span1);
    assertThat(luceneDocument1.get("list_field")).isEqualTo("[host1, host2]");
    assertThat(luceneDocument1.get("map_field.f1")).isEqualTo("v1");

    Trace.Span span2 = fromIngestDocument(convertRequestToDocument(indexRequests.get(1)), schema);
    assertThat(span2.getTagsCount()).isEqualTo(3);
    Map<String, Trace.KeyValue> tags2 =
        span2.getTagsList().stream()
            .map(kv -> Map.entry(kv.getKey(), kv))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    assertThat(tags2.get("list_field").getVStr()).isEqualTo("host3");

    Document luceneDocument2 = dropFieldBuilder.fromMessage(span2);
    assertThat(luceneDocument2.get("list_field")).isEqualTo("host3");
    assertThat(luceneDocument2.get("map_field")).isEqualTo("f1=v1");

    Trace.Span span3 = fromIngestDocument(convertRequestToDocument(indexRequests.get(2)), schema);
    assertThat(span3.getTagsCount()).isEqualTo(3);
    Map<String, Trace.KeyValue> tags3 =
        span3.getTagsList().stream()
            .map(kv -> Map.entry(kv.getKey(), kv))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    assertThat(tags3.get("list_field").getVStr()).isEqualTo("host4");

    Document luceneDocument3 = dropFieldBuilder.fromMessage(span3);
    assertThat(luceneDocument3.get("list_field")).isEqualTo("host4");
    assertThat(luceneDocument3.get("map_field.f1.f2")).isEqualTo("v3");
  }

  @Test
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public void testDefaultStringFields() throws Exception {
    final File schemaFile =
        new File(
            getClass().getClassLoader().getResource("schema/test_schema_defaults.yaml").getFile());
    Schema.IngestSchema schema = SchemaUtil.parseSchema(schemaFile.toPath());

    byte[] rawRequest = getIndexRequestBytes("index_text_defaults");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    AssertionsForClassTypes.assertThat(indexRequests.size()).isEqualTo(2);

    Trace.Span doc1 =
        BulkApiRequestParser.fromIngestDocument(
            convertRequestToDocument(indexRequests.get(0)), schema);
    Trace.Span doc2 =
        BulkApiRequestParser.fromIngestDocument(
            convertRequestToDocument(indexRequests.get(1)), schema);

    assertThat(doc1.getId().toStringUtf8()).isEqualTo("1");
    assertThat(doc1.getTagsList().size()).isEqualTo(7);

    // field defined in schema
    assertThat(
            doc1.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("host"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(
            doc1.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("ip"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.IP);

    // using default behavior
    assertThat(
            doc1.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("message"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(
            doc1.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("message.keyword"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(
            doc1.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("my_date"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(
            doc1.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("my_date.keyword"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);

    assertThat(doc2.getId().toStringUtf8()).isEqualTo("2");
    assertThat(doc2.getTagsList().size()).isEqualTo(11);

    // field defined in schema
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("ip"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.IP);

    // using default behavior
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("value1"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("value1.keyword"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("value2"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("value2.keyword"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("field1"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("field1.keyword"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("username"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("username.keyword"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);

    // default non-string behavior
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("number"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.INTEGER);
    assertThat(doc2.getTagsList().stream().anyMatch((tag) -> tag.getKey().equals("bucket")))
        .isFalse();
  }

  @Test
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public void testDefaultStringFieldsOnly() throws Exception {
    final File schemaFile =
        new File(
            getClass()
                .getClassLoader()
                .getResource("schema/test_schema_defaults_only.yaml")
                .getFile());
    Schema.IngestSchema schema = SchemaUtil.parseSchema(schemaFile.toPath());

    byte[] rawRequest = getIndexRequestBytes("index_text_defaults");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    AssertionsForClassTypes.assertThat(indexRequests.size()).isEqualTo(2);

    Trace.Span doc1 =
        BulkApiRequestParser.fromIngestDocument(
            convertRequestToDocument(indexRequests.get(0)), schema);
    Trace.Span doc2 =
        BulkApiRequestParser.fromIngestDocument(
            convertRequestToDocument(indexRequests.get(1)), schema);

    assertThat(doc1.getId().toStringUtf8()).isEqualTo("1");
    assertThat(doc1.getTagsList().size()).isEqualTo(9);

    // using default behavior
    assertThat(
            doc1.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("host"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(
            doc1.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("host.keyword"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(
            doc1.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("ip"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(
            doc1.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("ip.keyword"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(
            doc1.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("message"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(
            doc1.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("message.keyword"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(
            doc1.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("my_date"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(
            doc1.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("my_date.keyword"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);

    assertThat(doc2.getId().toStringUtf8()).isEqualTo("2");
    assertThat(doc2.getTagsList().size()).isEqualTo(12);

    // using default behavior
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("ip"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("ip.keyword"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("value1"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("value1.keyword"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("value2"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("value2.keyword"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("field1"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("field1.keyword"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("username"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("username.keyword"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);

    // default non-string behavior
    assertThat(
            doc2.getTagsList().stream()
                .filter((tag) -> tag.getKey().equals("number"))
                .findFirst()
                .get()
                .getFieldType())
        .isEqualTo(Schema.SchemaFieldType.INTEGER);
    assertThat(doc2.getTagsList().stream().anyMatch((tag) -> tag.getKey().equals("bucket")))
        .isFalse();
  }
}
