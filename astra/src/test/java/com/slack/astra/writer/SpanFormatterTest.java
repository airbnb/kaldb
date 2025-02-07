package com.slack.astra.writer;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.google.protobuf.ByteString;
import com.slack.astra.proto.schema.Schema;
import com.slack.service.murron.trace.Trace;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class SpanFormatterTest {

  @Test
  public void testMakeTraceKV() {
    // schema type: KEYWORD, key: error, value: test error message (less than 32766)
    Trace.KeyValue.Builder expectedValue = Trace.KeyValue.newBuilder();
    expectedValue.setKey("error");
    expectedValue.setFieldType(Schema.SchemaFieldType.KEYWORD);
    expectedValue.setVStr("test error message");
    Trace.KeyValue actualValue =
        SpanFormatter.makeTraceKV("error", "test error message", Schema.SchemaFieldType.KEYWORD);
    assertThat(actualValue.getKey()).isEqualTo(expectedValue.getKey());
    assertThat(actualValue.getVStr()).isEqualTo(expectedValue.getVStr());

    // schema type: KEYWORD, key: error, value: 1234...9999 (greater than 32766)
    String errorMsg =
        IntStream.range(1, 10000).boxed().map(String::valueOf).collect(Collectors.joining(""));
    expectedValue.setKey("error");
    expectedValue.setFieldType(Schema.SchemaFieldType.KEYWORD);
    expectedValue.setVStr(errorMsg);
    actualValue = SpanFormatter.makeTraceKV("error", errorMsg, Schema.SchemaFieldType.KEYWORD);
    assertThat(actualValue.getKey()).isEqualTo(expectedValue.getKey());
    assertThat(actualValue.getVStr())
        .isEqualTo(expectedValue.getVStr().substring(0, SpanFormatter.MAX_TERM_LENGTH));

    // schema type: TEXT, key: error, value: test error message (less than 32766)
    expectedValue.setKey("error");
    expectedValue.setFieldType(Schema.SchemaFieldType.TEXT);
    expectedValue.setVStr("test error message");
    actualValue =
        SpanFormatter.makeTraceKV("error", "test error message", Schema.SchemaFieldType.TEXT);
    assertThat(actualValue.getKey()).isEqualTo(expectedValue.getKey());
    assertThat(actualValue.getVStr()).isEqualTo(expectedValue.getVStr());

    // schema type: TEXT, key: error, value: 1234...9999 (greater than 32766)
    errorMsg =
        IntStream.range(1, 10000).boxed().map(String::valueOf).collect(Collectors.joining(""));
    expectedValue.setKey("error");
    expectedValue.setFieldType(Schema.SchemaFieldType.TEXT);
    expectedValue.setVStr(errorMsg);
    actualValue = SpanFormatter.makeTraceKV("error", errorMsg, Schema.SchemaFieldType.TEXT);
    assertThat(actualValue.getKey()).isEqualTo(expectedValue.getKey());
    assertThat(actualValue.getVStr())
        .isEqualTo(expectedValue.getVStr().substring(0, SpanFormatter.MAX_TERM_LENGTH));

    // schema type: BINARY, key: error, value: test error message (less than 32766)
    expectedValue.setKey("error");
    expectedValue.setFieldType(Schema.SchemaFieldType.BINARY);
    expectedValue.setVBinary(ByteString.copyFrom("test error message", StandardCharsets.UTF_8));
    actualValue =
        SpanFormatter.makeTraceKV("error", "test error message", Schema.SchemaFieldType.BINARY);
    assertThat(actualValue.getKey()).isEqualTo(expectedValue.getKey());
    assertThat(actualValue.getVBinary()).isEqualTo(expectedValue.getVBinary());

    // schema type: BINARY, key: error, value: 1234...9999 (greater than 32766)
    errorMsg =
        IntStream.range(1, 10000).boxed().map(String::valueOf).collect(Collectors.joining(""));
    expectedValue.setKey("error");
    expectedValue.setFieldType(Schema.SchemaFieldType.BINARY);
    expectedValue.setVBinary(ByteString.copyFrom(errorMsg, StandardCharsets.UTF_8));
    actualValue = SpanFormatter.makeTraceKV("error", errorMsg, Schema.SchemaFieldType.BINARY);
    assertThat(actualValue.getKey()).isEqualTo(expectedValue.getKey());
    assertThat(actualValue.getVBinary())
        .isEqualTo(expectedValue.getVBinary().substring(0, SpanFormatter.MAX_TERM_LENGTH));
  }
}
