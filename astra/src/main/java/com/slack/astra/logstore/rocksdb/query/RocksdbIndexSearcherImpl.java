package com.slack.astra.logstore.rocksdb.query;

import com.google.common.base.Stopwatch;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.LogIndexSearcher;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.logstore.search.SourceFieldFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksdbIndexSearcherImpl implements LogIndexSearcher<LogMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(RocksdbIndexSearcherImpl.class);

  static {
    RocksDB.loadLibrary();
  }

  private final RocksDB db;

  public RocksdbIndexSearcherImpl(Path path) throws RocksDBException {
    LOG.info("Rocksdb file path is {}", path.toString());
    this.db = RocksDB.open(new Options(), path.toString());
    LOG.info("opened path is {}", path.toString());
  }

  public RocksdbIndexSearcherImpl(RocksDB db) {
    this.db = db;
  }

  @Override
  public SearchResult<LogMessage> search(
      String dataset,
      int howMany,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder) {
    try {
      Stopwatch elapsedTime = Stopwatch.createStarted();
      byte[] key = dataset.getBytes();
      System.out.println(dataset);
      if (!db.keyExists(key)) {
        throw new IllegalStateException("missing key: " + dataset);
      }

      byte[] result = db.get(key);
      elapsedTime.stop();
      List<LogMessage> results =
          List.of(
              new LogMessage(
                  "test", "test_type", new String(result), Instant.now(), Collections.emptyMap()));
      return new SearchResult<>(
          results, elapsedTime.elapsed(TimeUnit.MICROSECONDS), 0, 0, 1, 1, null);
    } catch (RocksDBException ex) {
      throw new IllegalStateException(
          "Error fetching and parsing a result from rocksdb index for key: " + dataset, ex);
    }
  }

  @Override
  public void close() throws IOException {
    this.db.close();
  }
}
