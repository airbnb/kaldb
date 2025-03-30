package com.slack.astra.chunk;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DuckdbSample {
  public DuckdbSample() throws SQLException {}

  public static void main() throws SQLException {
    Connection conn = getConnection();
    Statement stmt = conn.createStatement();
    // insertData(stmt);
    insertTraceData(stmt);
    final String parquetFilePath = "/tmp/duckdb_test/items.parquet";
    // Make parquet file.
    exportParquet(stmt, parquetFilePath);
    stmt.close();
    conn.close();
  }

  private static void exportParquet(Statement stmt, String parquetFilePath) throws SQLException {
    stmt.execute(String.format("COPY items TO '%s' (FORMAT parquet)", parquetFilePath));
    System.out.println("Exported to parquet file successfully " + parquetFilePath);

    System.out.println("Start parquet file read");
    final String parquet_scan_query =
        String.format("SELECT * FROM parquet_scan('%s')", parquetFilePath);
    try (ResultSet rs = stmt.executeQuery(parquet_scan_query)) {
      while (rs.next()) {
        System.out.println(rs.getString(1));
        System.out.println(rs.getString(3));
      }
    }
    System.out.println("Finished parquet file read");
  }

  public static void insertData(Statement stmt) throws SQLException {
    stmt.execute(
        "CREATE OR REPLACE TABLE items (item VARCHAR, value DECIMAL(10, 2), count INTEGER)");
    // insert two items into the table
    stmt.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)");

    try (ResultSet rs = stmt.executeQuery("SELECT * FROM items")) {
      while (rs.next()) {
        System.out.println(rs.getString(1));
        System.out.println(rs.getInt(3));
      }
    }
  }

  private static void insertTraceData(Statement stmt) throws SQLException {
    // TODO: Use INTERVAL for duration?
    // TODO: UUID for id, trace_id and parent_id. May be not since bytes can be string.
    // TODO: How is map stored? Need a test. chatgpt says it is encoded. Needs verification. Can't
    // find in docs.
    stmt.execute(
        "CREATE OR REPLACE TABLE items (id STRING, parent_id STRING, trace_id STRING, name STRING, "
            + "timestamp TIMESTAMP, duration FLOAT, stringMap MAP(STRING, STRING), "
            + "numericMap MAP(STRING, FLOAT), integerMap MAP(STRING, STRING))");

    String sql1 =
        """
            INSERT INTO items VALUES (
                '1', NULL, 'trace-123', 'root-span', TIMESTAMP '2025-03-30 10:00:00', 120.5,
                MAP(['http.method', 'http.status_code'], ['GET', '200']),
                MAP(['db.duration', 'cpu.usage'], [12.3, 55.5]),
                MAP(['retry_count', 'attempt'], ['1', '2'])
            );
        """;
    stmt.execute(sql1);

    try (ResultSet rs = stmt.executeQuery("SELECT * FROM items")) {
      while (rs.next()) {
        System.out.println(rs.getString(1));
        System.out.println(rs.getString(3));
      }
    }
  }

  private static Connection getConnection() throws SQLException {
    Properties connectionProperties = new Properties();
    // connectionProperties.setProperty("temp_directory", "/Users/suman_karumuri/temp/duckdb_test");
    Connection conn =
        DriverManager.getConnection(
            "jdbc:duckdb:/tmp/duckdb_test/duckdb_test_db", connectionProperties);
    return conn;
  }
}
