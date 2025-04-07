package com.slack.astra.logstore.duckdb;

import com.google.protobuf.ByteString;
import java.util.Map;

public class DuckDBMapConverter {

  public static String convertStringMapToDuckDBMap(Map<String, String> inputMap) {
    return buildMapLiteral(inputMap, val -> "'" + escape(val) + "'");
  }

  public static String convertIntMapToDuckDBMap(Map<String, Integer> inputMap) {
    return buildMapLiteral(inputMap, Object::toString);
  }

  public static String convertLongMapToDuckDBMap(Map<String, Long> inputMap) {
    return buildMapLiteral(inputMap, Object::toString);
  }

  public static String convertDoubleMapToDuckDBMap(Map<String, Double> inputMap) {
    return buildMapLiteral(inputMap, Object::toString);
  }

  public static String convertFloatMapToDuckDBMap(Map<String, Float> inputMap) {
    return buildMapLiteral(inputMap, Object::toString);
  }

  public static String convertBooleanMapToDuckDBMap(Map<String, Boolean> inputMap) {
    return buildMapLiteral(inputMap, Object::toString);
  }

  public static String convertBinaryMapToDuckDBMap(Map<String, ByteString> inputMap) {
    return buildMapLiteral(inputMap, val -> "X'" + bytesToHex(val.toByteArray()) + "'");
  }

  // Generic builder for DuckDB map syntax
  private static <T> String buildMapLiteral(
      Map<String, T> map, java.util.function.Function<T, String> valueFormatter) {
    if (map == null || map.isEmpty()) {
      return "map([], [])";
    }

    StringBuilder keys = new StringBuilder(map.size() * 16);
    StringBuilder values = new StringBuilder(map.size() * 16);
    keys.append("[");
    values.append("[");

    boolean first = true;
    for (Map.Entry<String, T> entry : map.entrySet()) {
      if (!first) {
        keys.append(", ");
        values.append(", ");
      }

      keys.append("'").append(escape(entry.getKey())).append("'");
      T value = entry.getValue();
      values.append(value == null ? "NULL" : valueFormatter.apply(value));

      first = false;
    }

    keys.append("]");
    values.append("]");

    return "map(" + keys + ", " + values + ")";
  }

  private static String escape(String str) {
    return str == null ? "" : str.replace("'", "''");
  }

  private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

  private static String bytesToHex(byte[] bytes) {
    char[] hexChars = new char[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = HEX_ARRAY[v >>> 4];
      hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
    }
    return new String(hexChars);
  }
}
