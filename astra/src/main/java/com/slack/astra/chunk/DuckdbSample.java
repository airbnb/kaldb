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
    Properties connectionProperties = new Properties();
    // connectionProperties.setProperty("temp_directory", "/Users/suman_karumuri/temp/duckdb_test");
    Connection conn =
        DriverManager.getConnection(
            "jdbc:duckdb:/tmp/duckdb_test/duckdb_test_db", connectionProperties);

    Statement stmt = conn.createStatement();
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

    final String parquetFilePath = "/tmp/duckdb_test/items.parquet";
    // Make parquet file.
    stmt.execute(String.format("COPY items TO '%s' (FORMAT parquet)", parquetFilePath));
    System.out.println("Exported to parquet file successfully " + parquetFilePath);

    System.out.println("Start parquet file read");
    final String parquet_scan_query =
        String.format("SELECT * FROM parquet_scan('%s')", parquetFilePath);
    try (ResultSet rs = stmt.executeQuery(parquet_scan_query)) {
      while (rs.next()) {
        System.out.println(rs.getString(1));
        System.out.println(rs.getInt(3));
      }
    }
    System.out.println("Finished parquet file read");

    stmt.close();
    conn.close();
  }
}
