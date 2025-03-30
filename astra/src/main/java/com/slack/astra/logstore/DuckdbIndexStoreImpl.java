package com.slack.astra.logstore;

import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.FSDirectory;

/*
 * RocksdbIndexStore stores a log message in a duckdb database.
 */
public class DuckdbIndexStoreImpl implements LogStore {

  private final Connection conn;
  private final Statement stmt;

  public DuckdbIndexStoreImpl() throws SQLException {
    // TODO: Set connection properties.
    Properties connectionProperties = new Properties();
    String jdbcURL = "jdbc:duckdb:/tmp/duckdb_test/duckdb_test_db";
    conn = DriverManager.getConnection(jdbcURL, connectionProperties);
    stmt = conn.createStatement();

    // TODO: Use passed in folder. Remove replace once we use the passed in folder. Also, a random
    // TODO: Use INTERVAL for duration?
    // TODO: UUID for id, trace_id and parent_id. May be not since bytes can be string.
    // TODO: How is map stored? Need a test. chatgpt says it is encoded. Needs verification. Can't
    // uuid as folder name.

    // Create the table.
    stmt.execute(
        """
    CREATE OR REPLACE TABLE spans (
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
    """);
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
    String stringMap = "MAP(['http.method', 'http.status_code'], ['GET', '200'])";
    String numericMap = "MAP(['db.duration', 'cpu.usage'], [12.3, 55.5])";
    String integerMap = "MAP(['retry_count', 'attempt'], ['1', '2'])";

    // Template for SQL
    String insertTemplate =
        """
            INSERT INTO items VALUES (
                '%s', %s, '%s', '%s', TIMESTAMP '%s', %f,
                %s,
                %s,
                %s
            );
            """;

    // Build SQL string
    String sql =
        String.format(
            insertTemplate,
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
  public void commit() {}

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
