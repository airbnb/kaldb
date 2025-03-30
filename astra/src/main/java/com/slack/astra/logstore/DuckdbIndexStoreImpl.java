package com.slack.astra.logstore;

import com.slack.astra.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.util.RuntimeHalterImpl;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/*
 * RocksdbIndexStore stores a log message in a duckdb database.
 */
public class DuckdbIndexStoreImpl implements LogStore {

  public DuckdbIndexStoreImpl() {

  }

  @Override
  public void addMessage(Trace.Span message) {

  }

  @Override
  public SearcherManager getSearcherManager() {
    return null;
  }

  @Override
  public void commit() {

  }

  @Override
  public void refresh() {

  }

  @Override
  public void finalMerge() {

  }

  @Override
  public boolean isOpen() {
    return false;
  }

  @Override
  public void cleanup() throws IOException {

  }

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
  public void releaseIndexCommit(IndexCommit indexCommit) {

  }

  @Override
  public ConcurrentHashMap<String, LuceneFieldDef> getSchema() {
    return null;
  }

  @Override
  public void close() throws IOException {

  }
}
