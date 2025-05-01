package com.slack.kaldb;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@Fork(1)
public class DuckDBMapFlatBenchmark {

  private static final int NUM_COLUMNS = 100;
  private static final int NUM_ROWS = 5000;

  private Connection mapConn;
  private Connection expandedConn;
  private PreparedStatement insertExpandedStmt;
  private List<DataRow> testData;

  private final String MAP_DB_FILE = "map_table.duckdb";
  private final String EXPANDED_DB_FILE = "expanded_table.duckdb";

  @Setup(Level.Trial)
  public void setup() throws Exception {
    Files.deleteIfExists(Paths.get(MAP_DB_FILE));
    Files.deleteIfExists(Paths.get(EXPANDED_DB_FILE));

    Class.forName("org.duckdb.DuckDBDriver");

    mapConn = DriverManager.getConnection("jdbc:duckdb:" + MAP_DB_FILE);
    expandedConn = DriverManager.getConnection("jdbc:duckdb:" + EXPANDED_DB_FILE);

    Statement stmtMap = mapConn.createStatement();
    stmtMap.execute(
        """
            CREATE OR REPLACE TABLE map_items (
                id STRING,
                parent_id STRING,
                trace_id STRING,
                name STRING,
                timestamp TIMESTAMP,
                duration FLOAT,
                stringMap MAP(STRING, STRING),
                numericMap MAP(STRING, FLOAT),
                integerMap MAP(STRING, INTEGER)
            );
        """);

    Statement stmtExpanded = expandedConn.createStatement();
    StringBuilder expandedSchema =
        new StringBuilder(
            """
            CREATE OR REPLACE TABLE expanded_items (
                id STRING,
                parent_id STRING,
                trace_id STRING,
                name STRING,
                timestamp TIMESTAMP,
                duration FLOAT,
        """);
    for (int i = 0; i < NUM_COLUMNS; i++) {
      expandedSchema.append("string_").append(i).append(" STRING, ");
    }
    for (int i = 0; i < NUM_COLUMNS; i++) {
      expandedSchema.append("float_").append(i).append(" FLOAT, ");
    }
    for (int i = 0; i < NUM_COLUMNS; i++) {
      expandedSchema.append("int_").append(i).append(" INTEGER");
      if (i < NUM_COLUMNS - 1) expandedSchema.append(", ");
    }
    expandedSchema.append(");");
    stmtExpanded.execute(expandedSchema.toString());

    StringBuilder placeholders = new StringBuilder("(?, ?, ?, ?, ?, ?");
    for (int i = 0; i < NUM_COLUMNS * 3; i++) {
      placeholders.append(", ?");
    }
    placeholders.append(")");
    insertExpandedStmt =
        expandedConn.prepareStatement("INSERT INTO expanded_items VALUES " + placeholders);

    testData = generateTestData(NUM_ROWS);
  }

  @Benchmark
  public void insertIntoMapTable() throws Exception {
    for (DataRow row : testData) {
      String stringMapLiteral = mapToDuckDBLiteral(row.stringMap);
      String floatMapLiteral = mapToDuckDBLiteral(row.floatMap);
      String intMapLiteral = mapToDuckDBLiteral(row.intMap);
      String sql =
          String.format(
              """
                INSERT INTO map_items VALUES (
                    '%s', '%s', '%s', '%s',
                    TIMESTAMP '%s', %f,
                    %s, %s, %s
                )
            """,
              escape(row.id),
              escape(row.parentId),
              escape(row.traceId),
              escape(row.name),
              row.timestamp.toString(),
              row.duration,
              stringMapLiteral,
              floatMapLiteral,
              intMapLiteral);
      System.out.println("sql:" + sql);

      try (Statement stmt = mapConn.createStatement()) {
        stmt.execute(sql);
      }
    }
  }

  @Benchmark
  public void insertIntoExpandedTable() throws Exception {
    for (DataRow row : testData) {
      int idx = 1;
      insertExpandedStmt.setString(idx++, row.id);
      insertExpandedStmt.setString(idx++, row.parentId);
      insertExpandedStmt.setString(idx++, row.traceId);
      insertExpandedStmt.setString(idx++, row.name);
      insertExpandedStmt.setTimestamp(idx++, Timestamp.from(row.timestamp));
      insertExpandedStmt.setFloat(idx++, row.duration);

      for (int i = 0; i < NUM_COLUMNS; i++) {
        insertExpandedStmt.setString(idx++, row.stringMap.get("key" + i));
      }
      for (int i = 0; i < NUM_COLUMNS; i++) {
        insertExpandedStmt.setFloat(idx++, row.floatMap.get("key" + i));
      }
      for (int i = 0; i < NUM_COLUMNS; i++) {
        insertExpandedStmt.setInt(idx++, row.intMap.get("key" + i));
      }

      insertExpandedStmt.executeUpdate();
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    if (mapConn != null) mapConn.close();
    if (expandedConn != null) expandedConn.close();

    File mapFile = new File(MAP_DB_FILE);
    File expandedFile = new File(EXPANDED_DB_FILE);

    long mapSize = mapFile.length();
    long expandedSize = expandedFile.length();

    System.out.printf("Map DB file size:       %,.2f KB%n", mapSize / 1024.0);
    System.out.printf("Expanded DB file size:  %,.2f KB%n", expandedSize / 1024.0);
  }

  private List<DataRow> generateTestData(int count) {
    List<DataRow> rows = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      rows.add(DataRow.random());
    }
    return rows;
  }

  static class DataRow {
    String id;
    String parentId;
    String traceId;
    String name;
    Instant timestamp;
    float duration;
    Map<String, String> stringMap;
    Map<String, Float> floatMap;
    Map<String, Integer> intMap;

    static DataRow random() {
      DataRow r = new DataRow();
      r.id = UUID.randomUUID().toString();
      r.parentId = UUID.randomUUID().toString();
      r.traceId = UUID.randomUUID().toString();
      r.name = "TestItem";
      r.timestamp = Instant.now();
      r.duration = (float) Math.random();

      r.stringMap =
          IntStream.range(0, NUM_COLUMNS)
              .boxed()
              .collect(Collectors.toMap(i -> "key" + i, i -> "val" + i));

      r.floatMap =
          IntStream.range(0, NUM_COLUMNS)
              .boxed()
              .collect(Collectors.toMap(i -> "key" + i, i -> (float) i));

      r.intMap =
          IntStream.range(0, NUM_COLUMNS).boxed().collect(Collectors.toMap(i -> "key" + i, i -> i));

      return r;
    }
  }

  private <K, V> String mapToDuckDBLiteral(Map<K, V> map) {
    return "{"
        + map.entrySet().stream()
            .map(e -> "\"" + e.getKey() + "\" => " + formatValue(e.getValue()))
            .collect(Collectors.joining(", "))
        + "}";
  }

  private String formatValue(Object val) {
    if (val instanceof String s) {
      return "\"" + s.replace("\"", "\\\"") + "\"";
    }
    return val.toString();
  }

  private String escape(String s) {
    return s.replace("'", "''");
  }
}
