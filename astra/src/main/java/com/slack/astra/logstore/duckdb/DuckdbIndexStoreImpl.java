package com.slack.astra.logstore.duckdb;

import com.slack.astra.logstore.LogStore;
import com.slack.astra.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.service.murron.trace.Trace;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.FSDirectory;

/*
 * RocksdbIndexStore stores a log message in a duckdb database.
 */
public class DuckdbIndexStoreImpl implements LogStore {

  public static final String INSERT_STATEMENT_TEMPLATE = """
          INSERT INTO spans VALUES (
              '%s', %s, '%s', '%s', TIMESTAMP '%s', %f,
              %s,
              %s,
              %s
          );
          """;

  public static LogStore makeLogStore(
          File dataDirectory,
          Duration commitInterval,
          Duration refreshInterval,
          boolean enableFullTextSearch,
          SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy fieldConflictPolicy,
          MeterRegistry metricsRegistry)
          throws IOException {
    try {
      return new DuckdbIndexStoreImpl(dataDirectory, metricsRegistry);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to create duckDB store.", e);
    }
  }

  // TODO: Use INTERVAL for duration?
  // TODO: UUID for id, trace_id and parent_id. May be not since bytes can be string.
  // TODO: How is map stored? Need a test. chatgpt says it is encoded. Needs verification. Can't
  // TODO: Get this schema from config? Make this schema dynamic?
  // TODO: Add known fields.
  // TODO: Make schema a bit more flat?
  public static final String TABLE_SCHEMA = """
          CREATE TABLE spans (
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
  private final Connection conn;
  private final Statement stmt;
  private final String jdbcURL;

  public DuckdbIndexStoreImpl(File dataDirectory, MeterRegistry metricsRegistry) throws SQLException {
    // TODO: Set connection properties.
    Properties connectionProperties = new Properties();
    // TODO: Use passed in folder. Remove replace once we use the passed in folder.
    jdbcURL = "jdbc:duckdb:/tmp/duckdb_test/duckdb_test_db";
    conn = DriverManager.getConnection(jdbcURL, connectionProperties);
    stmt = conn.createStatement();
    // Create the table.
    stmt.execute(TABLE_SCHEMA);
    // Disable auto-commit.
    conn.setAutoCommit(false);
  }

  /** TODO: Use insert for now. It's slow. So use appender once this works. */
  @Override
  public void addMessage(Trace.Span message) {
    // Sample data
    String id = message.getId().toString();
    String parentId =
        (message.getParentId() == null) ? "NULL" : "'" + message.getParentId().toString() + "'";
    String traceId = message.getTraceId().toString();
    String name = message.getName();
    long timestamp = message.getTimestamp();
    float duration = message.getDuration();
    // TODO: Add remaining fields later
    String stringMap = "";
    String numericMap = "";
    String integerMap = "";

    // Build SQL string
    String sql =
        String.format(
            INSERT_STATEMENT_TEMPLATE,
            id,
            parentId,
            traceId,
            name,
            timestamp,
            duration,
            stringMap,
            numericMap,
            integerMap);

    System.out.println("Executing SQL:\n" + sql);
    try {
      stmt.execute(sql);
    } catch (SQLException e) {
      throw new IllegalArgumentException("SQL failed with exception: " + sql, e);
    }
  }

  @Override
  public SearcherManager getSearcherManager() {
    return null;
  }

  @Override
  public void commit() {
      try {
          conn.commit();
      } catch (SQLException e) {
          throw new RuntimeException("Failed to commit transaction", e);
      }
  }

  @Override
  public void refresh() {}

  @Override
  public void finalMerge() {}

  @Override
  public boolean isOpen() {
    return false;
  }

  @Override
  public void cleanup() throws IOException {}

  @Override
  public FSDirectory getDirectory() {
    return null;
  }

  @Override
  public IndexCommit getIndexCommit() {
    return null;
  }

  @Override
  public IndexWriter getIndexWriter() {
    return null;
  }

  @Override
  public void releaseIndexCommit(IndexCommit indexCommit) {}

  @Override
  public ConcurrentHashMap<String, LuceneFieldDef> getSchema() {
    return null;
  }

  @Override
  public void close() throws IOException {}
}
