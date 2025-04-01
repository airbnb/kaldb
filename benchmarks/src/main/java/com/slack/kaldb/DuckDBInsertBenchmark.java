package com.slack.kaldb;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Collectors;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@Fork(1)
public class DuckDBInsertBenchmark {

  private Connection conn;
  private Statement stmt;

  private final String MAP_DB_FILE = "map_table.duckdb";

  @Setup(Level.Trial)
  public void setup() throws Exception {
    conn = DriverManager.getConnection("jdbc:duckdb:"+MAP_DB_FILE);
    stmt = conn.createStatement();

    String createTable =
            """
                CREATE OR REPLACE TABLE items (
                    id STRING,
                    parent_id STRING,
                    trace_id STRING,
                    name STRING,
                    timestamp TIMESTAMP,
                    duration FLOAT,
                    stringMap MAP(STRING, STRING),
                    numericMap MAP(STRING, FLOAT),
                    integerMap MAP(STRING, STRING)
                );
                """;

    stmt.execute(createTable);
  }

  @TearDown(Level.Trial)
  public void teardown() throws Exception {
    stmt.close();
    conn.close();
    File mapFile = new File(MAP_DB_FILE);
    long mapSize = mapFile.length();
    System.out.printf("Map DB file size:       %,.2f KB%n", mapSize / 1024.0);
  }

  @Benchmark
  public void insertWith10MapEntries() throws Exception {
    insertItems(1000, 10);
  }

  @Benchmark
  public void insertWith100MapEntries() throws Exception {
    insertItems(1000, 100);
  }

  @Benchmark
  public void insertWith1000MapEntries() throws Exception {
    insertItems(1000, 1000);
  }

  private void insertItems(int count, int mapSize) throws Exception {
    conn.setAutoCommit(false); // begin transaction

    for (int i = 0; i < count; i++) {
      String id = "id-" + i;
      String parentId = (i % 5 == 0) ? "NULL" : "'id-" + (i - 1) + "'";
      String traceId = "trace-" + (i % 10);
      String name = "span-" + i;

      // Timestamp within a single day
      int hour = (i / 60) % 24;
      int minute = i % 60;
      String timestamp = String.format("2025-03-30 %02d:%02d:00", hour, minute);

      float duration = (float) (50 + Math.random() * 100);

      String stringMap = buildStringMap(i, mapSize);
      String numericMap = buildNumericMap(i, mapSize);
      String integerMap = buildIntegerMap(i, mapSize);

      String insert =
              String.format(
                      """
                        INSERT INTO items VALUES (
                            '%s', %s, '%s', '%s', TIMESTAMP '%s', %.2f,
                            %s,
                            %s,
                            %s
                        );
                        """,
                      id, parentId, traceId, name, timestamp, duration, stringMap, numericMap, integerMap);

      stmt.execute(insert);
    }

    conn.commit(); // end transaction
    conn.setAutoCommit(true);
  }

  private String buildStringMap(int base, int count) {
    String keys =
            IntStream.range(base, base + count)
                    .mapToObj(i -> "'key" + i + "'")
                    .collect(Collectors.joining(", "));
    String values =
            IntStream.range(base, base + count)
                    .mapToObj(i -> "'val" + i + "'")
                    .collect(Collectors.joining(", "));
    return String.format("MAP([%s], [%s])", keys, values);
  }

  private String buildNumericMap(int base, int count) {
    String keys =
            IntStream.range(base, base + count)
                    .mapToObj(i -> "'metric" + i + "'")
                    .collect(Collectors.joining(", "));
    String values =
            IntStream.range(0, count)
                    .mapToObj(i -> String.format("%.2f", Math.random() * 100))
                    .collect(Collectors.joining(", "));
    return String.format("MAP([%s], [%s])", keys, values);
  }

  private String buildIntegerMap(int base, int count) {
    String keys =
            IntStream.range(base, base + count)
                    .mapToObj(i -> "'count" + i + "'")
                    .collect(Collectors.joining(", "));
    String values =
            IntStream.range(base, base + count)
                    .mapToObj(i -> "'" + (i % 10) + "'")
                    .collect(Collectors.joining(", "));
    return String.format("MAP([%s], [%s])", keys, values);
  }
}
