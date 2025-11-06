package com.slack.astra.chunk;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Simple single-thread benchmark that contrasts write and point-read throughput on RocksDB.
 *
 * <p>Defaults to loading ~10GB of data but the workload can be tuned via system properties:
 *
 * <ul>
 *   <li>-Drocksdb.benchmark.bytes=&lt;target-bytes&gt;
 *   <li>-Drocksdb.benchmark.valueSize=&lt;value-bytes&gt;
 *   <li>-Drocksdb.benchmark.queries=&lt;point-lookups&gt;
 * </ul>
 */
public final class RocksDbBenchmark {
  private static final long DEFAULT_BYTES = 1L * 1024 * 1024 * 1024; // 1GB target
  private static final long TARGET_DATA_BYTES =
      Long.getLong("rocksdb.benchmark.bytes", DEFAULT_BYTES);
  private static final int VALUE_SIZE_BYTES =
      Integer.getInteger("rocksdb.benchmark.valueSize", 4096);
  private static final long QUERY_SAMPLES =
      Long.getLong("rocksdb.benchmark.queries", 1_000_000_00L);
  private static final byte[] VALUE_TEMPLATE = buildValue();

  private RocksDbBenchmark() {}

  public static void main(final String[] args) throws IOException, RocksDBException {
    RocksDB.loadLibrary();

    if (VALUE_SIZE_BYTES <= 0) {
      throw new IllegalArgumentException("VALUE_SIZE_BYTES must be > 0");
    }

    final long entries = TARGET_DATA_BYTES / VALUE_SIZE_BYTES;
    if (entries == 0) {
      throw new IllegalStateException("Configured dataset is smaller than a single value");
    }

    final Path dbPath = Files.createTempDirectory("rocksdb-benchmark");
    System.out.printf(
        "Benchmark target: %,d entries x %dB (~%.2f GB)%n",
        entries, VALUE_SIZE_BYTES, bytesToGigabytes(entries * (long) VALUE_SIZE_BYTES));
    System.out.println("RocksDB path: " + dbPath);

    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, dbPath.toString())) {
      final long insertNanos = runInsertBenchmark(db, entries);
      final long queryNanos = runQueryBenchmark(db, entries);

      final long bytesWritten = entries * (long) (VALUE_SIZE_BYTES + Long.BYTES);
      System.out.printf(
          "Insert throughput: %.2f MB/s (%d ops in %.2f s)\n",
          throughputMBps(bytesWritten, insertNanos), entries, nanosToSeconds(insertNanos));

      final long queries = Math.min(entries, QUERY_SAMPLES);
      System.out.printf(
          "Query throughput: %.2f Mops/s (%d ops in %.2f s)\n",
          throughputMops(queries, queryNanos), queries, nanosToSeconds(queryNanos));
    }
  }

  private static long runInsertBenchmark(final RocksDB db, final long entries)
      throws RocksDBException {
    long inserted = 0;
    final long start = System.nanoTime();
    for (; inserted < entries; inserted++) {
      db.put(longToKey(inserted), VALUE_TEMPLATE);
    }
    return System.nanoTime() - start;
  }

  private static long runQueryBenchmark(final RocksDB db, final long entries)
      throws RocksDBException {
    long queries = Math.min(entries, QUERY_SAMPLES);
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    final long start = System.nanoTime();
    long hits = 0;
    for (long i = 0; i < queries; i++) {
      if (db.get(longToKey(random.nextLong(entries))) != null) {
        hits++;
      }
    }
    final long duration = System.nanoTime() - start;
    System.out.printf("Query hit rate: %.2f%%%n", (hits * 100.0) / queries);
    return duration;
  }

  private static byte[] longToKey(final long value) {
    final byte[] buffer = new byte[Long.BYTES];
    long current = value;
    for (int i = Long.BYTES - 1; i >= 0; i--) {
      buffer[i] = (byte) (current & 0xFF);
      current >>>= 8;
    }
    return buffer;
  }

  private static byte[] buildValue() {
    final byte[] value = new byte[VALUE_SIZE_BYTES];
    for (int i = 0; i < VALUE_SIZE_BYTES; i++) {
      value[i] = (byte) (i % 127);
    }
    return value;
  }

  private static double throughputMBps(final long bytes, final long durationNanos) {
    if (durationNanos == 0) {
      return 0;
    }
    final double megabytes = bytes / (1024d * 1024d);
    return megabytes / nanosToSeconds(durationNanos);
  }

  private static double throughputMops(final long ops, final long durationNanos) {
    if (durationNanos == 0) {
      return 0;
    }
    final double millions = ops / 1_000_000d;
    return millions / nanosToSeconds(durationNanos);
  }

  private static double nanosToSeconds(final long durationNanos) {
    return durationNanos / 1_000_000_000d;
  }

  private static double bytesToGigabytes(final long bytes) {
    return bytes / (1024d * 1024d * 1024d);
  }
}
