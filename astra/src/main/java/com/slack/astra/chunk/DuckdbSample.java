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
    connectionProperties.setProperty("temp_directory", "/Users/suman_karumuri/duckdb_test");
    Connection conn =
        DriverManager.getConnection("jdbc:duckdb:/tmp/my_database", connectionProperties);

    Statement stmt = conn.createStatement();
    stmt.execute("CREATE TABLE items (item VARCHAR, value DECIMAL(10, 2), count INTEGER)");
    // insert two items into the table
    stmt.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)");

    try (ResultSet rs = stmt.executeQuery("SELECT * FROM items")) {
      while (rs.next()) {
        System.out.println(rs.getString(1));
        System.out.println(rs.getInt(3));
      }
    }
    stmt.close();
  }
}
