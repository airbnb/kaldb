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
    // uuid as folder name.
    stmt.execute(
        "CREATE OR REPLACE TABLE items (item VARCHAR, value DECIMAL(10, 2), count INTEGER)");
    // Create the table.
  }

  @Override
  public void addMessage(Trace.Span message) {}

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
