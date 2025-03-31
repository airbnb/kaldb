package com.slack.kaldb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class DuckDBInsertBenchmark {

  private Connection conn;
  private Statement stmt;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    conn = DriverManager.getConnection("jdbc:duckdb:");
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
  }

  @Benchmark
  public void insertThousandRows() throws Exception {
    conn.setAutoCommit(false); // begin transaction
    for (int i = 0; i < 1000; i++) {
      String id = "id-" + i;
      String parentId = (i % 5 == 0) ? "NULL" : "'id-" + (i - 1) + "'";
      String traceId = "trace-" + (i % 10);
      String name = "span-" + i;
      String timestamp = "2025-03-30 10:" + String.format("%02d:%02d", i / 60, i % 60);
      float duration = (float) (50 + Math.random() * 100);
      String stringMap =
          String.format("MAP(['key%d', 'key%d'], ['val%d', 'val%d'])", i, i + 1, i, i + 1);
      String numericMap =
          String.format(
              "MAP(['metric%d', 'metric%d'], [%.2f, %.2f])",
              i, i + 1, Math.random() * 100, Math.random() * 100);
      String integerMap =
          String.format("MAP(['count%d', 'count%d'], ['%d', '%d'])", i, i + 1, i % 5, i % 3);

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
}
