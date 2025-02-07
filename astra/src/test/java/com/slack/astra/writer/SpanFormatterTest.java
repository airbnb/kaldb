package com.slack.astra.writer;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.slack.astra.proto.schema.Schema;
import com.slack.service.murron.trace.Trace;
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
  }
}
