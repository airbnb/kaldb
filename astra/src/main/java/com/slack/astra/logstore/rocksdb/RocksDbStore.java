package com.slack.astra.logstore.rocksdb;

import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;

import com.slack.astra.logstore.LogStore;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.FSDirectory;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDbStore implements LogStore {
  static {
    RocksDB.loadLibrary();
  }

  private final RocksDB db;
  private final File dataDirectory;
  private final Counter messagesReceivedCounter;
  private final Counter messagesFailedCounter;

  // TODO: Replace luceneConfig with RocksdbConfig
  public RocksDbStore(File dataDirectory, MeterRegistry metricsRegistry) throws RocksDBException {
    this.dataDirectory = dataDirectory;
    messagesReceivedCounter = metricsRegistry.counter(MESSAGES_RECEIVED_COUNTER);
    messagesFailedCounter = metricsRegistry.counter(MESSAGES_FAILED_COUNTER);

    final Options options = new Options().setCreateIfMissing(true);
    this.db = RocksDB.open(options, dataDirectory.getAbsolutePath());
  }

  // id is key
  // message is the value
  @Override
  public void addMessage(Trace.Span message) {
    String key = message.getId().toStringUtf8();
    System.out.println("key added is: " + key);
    byte[] value = key.getBytes();
    try {
      db.put(value, value);
      messagesReceivedCounter.increment();
    } catch (RocksDBException e) {
      messagesFailedCounter.increment();
      throw new RuntimeException("failed to insert key", e);
    }
  }

  @Override
  public SearcherManager getSearcherManager() {
    throw new UnsupportedOperationException("Rocksdb doesn't support searcher manager");
  }

  @Override
  public void commit() {}

  @Override
  public void refresh() {}

  @Override
  public void finalMerge() {}

  @Override
  public boolean isOpen() {
    if (db != null) return true;
    return false;
  }

  @Override
  public void cleanup() throws IOException {
    db.close();
    FileUtils.deleteDirectory(dataDirectory);
  }

  @Override
  public FSDirectory getDirectory() {
    throw new UnsupportedOperationException("rocksdb");
  }

  @Override
  public IndexCommit getIndexCommit() {
    throw new UnsupportedOperationException("rocksdb");
  }

  @Override
  public IndexWriter getIndexWriter() {
    throw new UnsupportedOperationException("rocksdb");
  }

  @Override
  public void releaseIndexCommit(IndexCommit indexCommit) {
    throw new UnsupportedOperationException("rocksdb");
  }

  @Override
  public ConcurrentHashMap<String, LuceneFieldDef> getSchema() {
    return null;
  }

  @Override
  public void close() throws IOException {
    db.close();
  }
}
