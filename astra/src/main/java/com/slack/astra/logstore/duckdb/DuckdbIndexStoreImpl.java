package com.slack.astra.logstore.duckdb;

import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;

import com.slack.astra.logstore.LogStore;
import com.slack.astra.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * RocksdbIndexStore stores a log message in a duckdb database.
 */
public class DuckdbIndexStoreImpl implements LogStore {
  private static final Logger LOG = LoggerFactory.getLogger(DuckdbIndexStoreImpl.class);

  public static final String INSERT_STATEMENT_TEMPLATE =
      """
          INSERT INTO spans VALUES (
              '%s', %s, '%s', '%s', '%s', %f,
              %s,
              %s,
              %s
          );
          """;
  public final MeterRegistry registry;
  public Counter messagesReceivedCounter;

  public static DuckdbIndexStoreImpl makeLogStore(
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
  // TODO: Remove REPLACE TABLE
  private static final String TABLE_SCHEMA =
      """
          CREATE OR REPLACE TABLE spans(
              id STRING,
              parent_id STRING,
              trace_id STRING,
              name STRING,
              timestamp BIGINT,
              duration FLOAT,
              stringMap MAP(STRING, STRING),
              numericMap MAP(STRING, FLOAT),
              integerMap MAP(STRING, STRING)
          );
          """;
  private final Connection conn;
  // TODO: Does single writer query slow down ingestion?
  // TODO: Use appender API for improved write performance. No support for MAP fields via appender.
  private final Statement writeStatement;
  // Create seperate statements for read and write path so we can make sure commit works. Needed for tests and exports to work correctly.
  // Statement output is also tied to last query. So, if we are using statement objects across queries we will see inconsistent results. For now it's all single reader and writer.
  // TODO: Use seperate statement for every query?
  private final Statement readStatement;
  private final String jdbcURL;

  public DuckdbIndexStoreImpl(File dataDirectory, MeterRegistry metricsRegistry)
      throws SQLException {
    registry = metricsRegistry;

    // TODO: Set connection properties.
    Properties connectionProperties = new Properties();
    // TODO: Use passed in folder. Remove replace once we use the passed in folder.
    jdbcURL = "jdbc:duckdb:/tmp/duckdb_test/duckdb_test_db";
    conn = DriverManager.getConnection(jdbcURL, connectionProperties);
    writeStatement = conn.createStatement();
    // Create the table.
    writeStatement.execute(TABLE_SCHEMA);
    // Disable auto-commit.
    conn.setAutoCommit(false);

    readStatement = conn.createStatement();

    messagesReceivedCounter = registry.counter(MESSAGES_RECEIVED_COUNTER);
  }

  /** TODO: Use insert for now. It's slow. So use appender once this works. */
  @Override
  public void addMessage(Trace.Span message) {
    messagesReceivedCounter.increment();

    String id = message.getId().toStringUtf8();
    String parentId =
        (message.getParentId() == null) ? "NULL" : "'" + message.getParentId().toStringUtf8() + "'";
    String traceId = message.getTraceId().toStringUtf8();
    String name = message.getName();
    long timestamp = message.getTimestamp();
    float duration = message.getDuration();
    // TODO: Sample data. Add real data.
    String stringMap = "MAP(['http.method', 'http.status_code'], ['GET', '200'])";
    String numericMap = "MAP(['db.duration', 'cpu.usage'], [12.3, 55.5])";
    String integerMap = "MAP(['retry_count', 'attempt'], ['1', '2'])";

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

    LOG.trace("Executing SQL:\n" + sql);
    try {
      writeStatement.execute(sql);
    } catch (SQLException e) {
      throw new IllegalArgumentException("SQL failed with exception: " + sql, e);
    }
  }

  @Override
  public SearcherManager getSearcherManager() {
    return null;
  }

  public ResultSet executeSQLQuery(String sql) throws SQLException {
    LOG.info("Executing read query: " + sql);
    return readStatement.executeQuery(sql);
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
  public void refresh() {
    // Commit commits the data to disk. No refresh is needed unlike Lucene.
  }

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
