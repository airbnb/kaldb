package com.slack.astra.logstore.search;

import com.google.common.base.Stopwatch;
import com.slack.astra.logstore.LogMessage;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.crt.Log;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RocksdbIndexSearcherImpl implements LogIndexSearcher<LogMessage> {
    static {
        RocksDB.loadLibrary();
    }

    private static final Logger LOG = LoggerFactory.getLogger(RocksdbIndexSearcherImpl.class);
    private final RocksDB db;

    RocksdbIndexSearcherImpl(String path) throws RocksDBException {
        this.db = RocksDB.open(new Options(), path);
    }

    @Override
    public SearchResult<LogMessage> search(String dataset, int howMany, QueryBuilder queryBuilder, SourceFieldFilter sourceFieldFilter, AggregatorFactories.Builder aggregatorFactoriesBuilder) {
        try {
            Stopwatch elapsedTime = Stopwatch.createStarted();
            byte[] result = db.get(dataset.getBytes());
            elapsedTime.stop();
            List<LogMessage> results = List.of(new LogMessage("test", "test_type", new String(result), Instant.now(), Collections.emptyMap()));
            return new SearchResult<>(
                    results, elapsedTime.elapsed(TimeUnit.MICROSECONDS), 0, 0, 1, 1, null);
        } catch (RocksDBException ex) {
            throw new IllegalStateException("Error fetching and parsing a result from rocksdb index for key: " + dataset, ex);
        }
    }

    @Override
    public void close() throws IOException {
        this.db.close();
    }
}
