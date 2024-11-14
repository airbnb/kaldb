package com.slack.astra.logstore.rocksdb.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.toList;
import java.util.stream.Stream;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.LogIndexSearcher;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.logstore.search.SourceFieldFilter;

public class RocksdbIndexSearcherImpl implements LogIndexSearcher<LogMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(RocksdbIndexSearcherImpl.class);

  static {
    RocksDB.loadLibrary();
  }

  private final RocksDB db;

  public RocksdbIndexSearcherImpl(Path path) throws RocksDBException, IOException {
    LOG.info("Rocksdb file path is {}", path.toString());
    this.db = RocksDB.open(path.toString() + "rocksdb");
    LOG.info("opened path is {}", path.toString());
    // Ingest all SST files in the directory
    try (Stream<Path> files = Files.list(path)) {
      List<Path> sstFiles =
          files.filter(file -> file.toString().endsWith(".sst")).collect(toList());
      ingestSstFiles(sstFiles);
    }
  }

  // Method to ingest SST files
  public final void ingestSstFiles(List<Path> sstFiles) throws RocksDBException {
    IngestExternalFileOptions ingestOptions = new IngestExternalFileOptions();
    ingestOptions.setMoveFiles(true); // Move files instead of copying
    LOG.info("Ingesting SST files: {}", sstFiles);
    List<String> sstFilePaths = sstFiles.stream().map(Path::toString).collect(Collectors.toList());
    db.ingestExternalFile(sstFilePaths, ingestOptions);
    LOG.info("Ingested SST files: {}", sstFiles);
  }

  public RocksdbIndexSearcherImpl(RocksDB db) {
    this.db = db;
  }

  private byte[] extractKeyFromQueryBuilder(QueryBuilder queryBuilder) throws IOException {
    // Convert the QueryBuilder to a JSON string
    String queryJson = queryBuilder.toString();

    // Parse the JSON string
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(queryJson);

    // Extract the key from the JSON
    String primaryKeyBase64 =
        rootNode
            .path("bool")
            .path("filter")
            .get(1)
            .path("query_string")
            .path("query")
            .asText()
            .split(":")[1];
    String secondaryKeyBase64 =
        rootNode
            .path("bool")
            .path("filter")
            .get(1)
            .path("query_string")
            .path("query")
            .asText()
            .split(":")[2];

    byte[] primaryKeyBytes = Base64.getDecoder().decode(primaryKeyBase64);
    byte[] secondaryKeyBytes = Base64.getDecoder().decode(secondaryKeyBase64);
    int primaryKeySize = primaryKeyBytes.length;
    ByteBuffer buffer = ByteBuffer.allocate(4 + primaryKeySize + secondaryKeyBytes.length);
    buffer.putInt(primaryKeySize);
    buffer.put(primaryKeyBytes);
    buffer.put(secondaryKeyBytes);

    return buffer.array();
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
      byte[] prefix_key = extractKeyFromQueryBuilder(queryBuilder);
      System.out.println(dataset);
      boolean found = false;

      List<LogMessage> results = new ArrayList<>();
      try (RocksIterator iterator = db.newIterator()) {
        for (iterator.seek(prefix_key);
            iterator.isValid() && startsWith(iterator.key(), prefix_key);
            iterator.next()) {
          byte[] value = iterator.value();
          results.add(
              new LogMessage(
                  "test", "test_type", new String(value), Instant.now(), Collections.emptyMap()));
          found = true;
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to perform prefix query in RocksDB", e);
      }

      if (!found) {
        return new SearchResult<>(
            List.of(
                new LogMessage(
                    "test",
                    "test_type",
                    new String(prefix_key),
                    Instant.now(),
                    Collections.emptyMap())),
            elapsedTime.elapsed(TimeUnit.MICROSECONDS),
            0,
            0,
            1,
            1,
            null);
      }
      return new SearchResult<>(
          results, elapsedTime.elapsed(TimeUnit.MICROSECONDS), 0, 0, 1, 1, null);
    } catch (IOException ex) {
      throw new IllegalStateException(
          "Error fetching and parsing a result from rocksdb index for key: " + dataset, ex);
    }
  }

  private boolean startsWith(byte[] key, byte[] prefix) {
    if (key.length < prefix.length) {
      return false;
    }
    for (int i = 0; i < prefix.length; i++) {
      if (key[i] != prefix[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    this.db.close();
  }
}
