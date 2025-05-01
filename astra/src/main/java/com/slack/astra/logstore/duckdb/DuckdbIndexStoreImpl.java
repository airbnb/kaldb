package com.slack.astra.logstore.duckdb;

import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;

import com.google.protobuf.ByteString;
import com.slack.astra.logstore.LogStore;
import com.slack.astra.logstore.search.LogIndexSearcher;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.schema.Schema;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
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

  private final String id = UUID.randomUUID().toString();

  public static final String INSERT_STATEMENT_TEMPLATE =
      """
          INSERT INTO spans VALUES (
              '%s', %s, '%s', '%s', '%s', %f,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s
          );
          """;
  public final MeterRegistry registry;
  public Counter messagesReceivedCounter;

  // TODO: UUID for id, trace_id and parent_id. May be not since bytes can be string.
  // TODO: How is map stored? Need a test. chatgpt says it is encoded. Needs verification.
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
              integerMap MAP(STRING, INT),
              longMap MAP(STRING, LONG),
              floatMap MAP(STRING, FLOAT),
              doubleMap MAP(STRING, DOUBLE),
              booleanMap MAP(STRING, BOOL),
              binaryMap MAP(STRING, VARBINARY)
          );
          """;
  private final Connection conn;
  // TODO: Does single writer query slow down ingestion?
  // TODO: Use appender API for improved write performance. No support for MAP fields via appender.
  private final Statement writeStatement;
  // Create seperate statements for read and write path so we can make sure commit works. Needed for
  // tests and exports to work correctly.
  // Statement output is also tied to last query. So, if we are using statement objects across
  // queries we will see inconsistent results. For now it's all single reader and writer.
  // TODO: Use seperate statement for every query?
  private final Statement readStatement;

  public DuckdbIndexStoreImpl(File dataDirectory, MeterRegistry metricsRegistry)
      throws SQLException {
    registry = metricsRegistry;

    // TODO: Set connection properties.
    Properties connectionProperties = new Properties();
    String jdbcURL = "jdbc:duckdb:" + dataDirectory.getAbsolutePath() + "/test_db.duckdb";
    conn = DriverManager.getConnection(jdbcURL, connectionProperties);
    conn.setAutoCommit(false);
    LOG.debug("Created database at: {}", jdbcURL);
    writeStatement = conn.createStatement();
    // Create the table.
    writeStatement.execute(TABLE_SCHEMA);
    LOG.trace("Created table at: {}", TABLE_SCHEMA);
    // Disable auto-commit.

    readStatement = conn.createStatement();

    messagesReceivedCounter = registry.counter(MESSAGES_RECEIVED_COUNTER);
  }

  public static LogStore makeLogStore(
      File dataDirectory, AstraConfigs.LuceneConfig luceneConfig, MeterRegistry metricsRegistry) {
    try {
      return new DuckdbIndexStoreImpl(dataDirectory, metricsRegistry);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to create duckDB store.", e);
    }
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

    // TODO: StringBuffer is more efficient?
    Map<String, String> stringTags = new HashMap<>();
    Map<String, Integer> intTags = new HashMap<>();
    Map<String, Long> longTags = new HashMap<>();
    Map<String, Double> doubleTags = new HashMap<>();
    Map<String, Float> floatTags = new HashMap<>();
    Map<String, Boolean> boolTags = new HashMap<>();
    Map<String, ByteString> binaryTags = new HashMap<>();
    // TODO: Sample data. Add real data.
    // TODO: Make case more efficient. Make this map more efficient since it's hot path.
    for (Trace.KeyValue tag : message.getTagsList()) {
      switch (tag.getFieldType()) {
        case Schema.SchemaFieldType.KEYWORD,
                Schema.SchemaFieldType.BYTE,
                Schema.SchemaFieldType.ID,
                Schema.SchemaFieldType.STRING,
                Schema.SchemaFieldType.TEXT ->
            stringTags.put(tag.getKey(), tag.getVStr());
        case Schema.SchemaFieldType.BINARY -> binaryTags.put(tag.getKey(), tag.getVBinary());
        case Schema.SchemaFieldType.IP, Schema.SchemaFieldType.DATE ->
            longTags.put(tag.getKey(), tag.getVInt64());
        case Schema.SchemaFieldType.BOOLEAN -> boolTags.put(tag.getKey(), tag.getVBool());
        case Schema.SchemaFieldType.LONG,
                Schema.SchemaFieldType.SCALED_LONG,
                Schema.SchemaFieldType.DOUBLE ->
            doubleTags.put(tag.getKey(), tag.getVFloat64());
        case Schema.SchemaFieldType.FLOAT, Schema.SchemaFieldType.HALF_FLOAT ->
            floatTags.put(tag.getKey(), tag.getVFloat32());
        case Schema.SchemaFieldType.INTEGER, Schema.SchemaFieldType.SHORT ->
            intTags.put(tag.getKey(), tag.getVInt32());
        // TODO: Index unknown type as string?
        default ->
            LOG.error(
                "unhandled field type: {} for tag {} in message {}",
                tag.getFieldType().getDescriptorForType(),
                tag,
                message);
      }
    }

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
            DuckDBMapConverter.convertStringMapToDuckDBMap(stringTags),
            DuckDBMapConverter.convertIntMapToDuckDBMap(intTags),
            DuckDBMapConverter.convertLongMapToDuckDBMap(longTags),
            DuckDBMapConverter.convertFloatMapToDuckDBMap(floatTags),
            DuckDBMapConverter.convertDoubleMapToDuckDBMap(doubleTags),
            DuckDBMapConverter.convertBooleanMapToDuckDBMap(boolTags),
            DuckDBMapConverter.convertBinaryMapToDuckDBMap(binaryTags));

    LOG.trace("Executing SQL:\n" + sql);
    try {
      writeStatement.execute(sql);
    } catch (SQLException e) {
      throw new IllegalArgumentException("SQL failed with exception: " + sql, e);
    }
  }

  /**
   * Converts a Java Map<String, String> to a DuckDB-compatible MAP literal string using
   * StringBuilder. Example output: map(['key1', 'key2'], ['value1', 'value2'])
   */
  public static String convertMapToDuckDBMap(Map<String, String> inputMap) {
    if (inputMap == null || inputMap.isEmpty()) {
      return "map([], [])";
    }

    // Estimate capacity roughly to avoid resizing
    int estimatedSize = inputMap.size() * 16;
    StringBuilder keys = new StringBuilder(estimatedSize);
    StringBuilder values = new StringBuilder(estimatedSize);

    keys.append("[");
    values.append("[");

    boolean first = true;
    for (Map.Entry<String, String> entry : inputMap.entrySet()) {
      if (!first) {
        keys.append(", ");
        values.append(", ");
      }

      keys.append("'").append(escape(entry.getKey())).append("'");
      values.append("'").append(escape(entry.getValue())).append("'");

      first = false;
    }

    keys.append("]");
    values.append("]");

    return "map(" + keys + ", " + values + ")";
  }

  private static String escape(String str) {
    return str == null ? "" : str.replace("'", "''");
  }

  @Override
  public SearcherManager getSearcherManager() {
    return null;
  }

  @Override
  public LogIndexSearcher getLogSearcher() {
    return new DuckdbSearcher();
  }

  @Override
  public String getId() {
    return id;
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

  // TODO: GetDirectory is used to get absolute path. We can replace the return value with an actual
  // file path.
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
