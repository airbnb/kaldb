package com.slack.astra.chunk;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.rocksdb.BackupEngine;
import org.rocksdb.BackupEngineOptions;
import org.rocksdb.Env;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

public class RocksDbSample {
  private static final Duration BACKUP_INTERVAL = Duration.ofSeconds(30);
  private static final Duration WRITE_INTERVAL = Duration.ofSeconds(1);
  private static final List<Path> BACKUP_DIRECTORIES = new java.util.concurrent.CopyOnWriteArrayList<>();
  public static final String DB_FILE_PATH;
  private static final Path DB_PATH;

  static {
    RocksDB.loadLibrary();
    try {
      DB_PATH = Files.createTempDirectory("rocksdb-sample");
      DB_FILE_PATH = DB_PATH.toString();
    } catch (IOException e) {
      throw new RuntimeException("Unable to create temp RocksDB directory", e);
    }
  }

  public static void main(final String[] args) throws RocksDBException {
    final ExecutorService writerExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "rocksdb-writer"));
    final ScheduledExecutorService backupScheduler =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "rocksdb-backup"));
    final AtomicBoolean running = new AtomicBoolean(true);
    final CountDownLatch shutdownLatch = new CountDownLatch(1);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  running.set(false);
                  writerExecutor.shutdownNow();
                  backupScheduler.shutdownNow();
                  shutdownLatch.countDown();
                },
                "rocksdb-sample-shutdown"));

    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, DB_FILE_PATH)) {
      System.out.println("DB opened at: " + DB_FILE_PATH);
      runSstDemo(db);

      writerExecutor.submit(() -> runWriteLoop(db, running));
      backupScheduler.scheduleAtFixedRate(
          () -> createBackupSnapshot(db),
          BACKUP_INTERVAL.toMillis(),
          BACKUP_INTERVAL.toMillis(),
          TimeUnit.MILLISECONDS);

      System.out.println(
          "Background writer started; backups run every " + BACKUP_INTERVAL.toSeconds() + " seconds.");
      System.out.println("Press Ctrl+C to stop the sample.");

      try {
        shutdownLatch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } finally {
      running.set(false);
      writerExecutor.shutdownNow();
      backupScheduler.shutdownNow();
      cleanupTempResources();
    }
  }

  private static void runSstDemo(final RocksDB db) throws RocksDBException {
    final String prefix1 = "sst1";
    final String sstPath1 = createSstFile(prefix1);
    ingestSstFile(db, sstPath1);
    System.out.println("SST read: " + new String(db.get((prefix1 + "sst_key_2").getBytes())));

    final String prefix2 = "sst2";
    final String sstPath2 = createSstFile(prefix2);
    ingestSstFile(db, sstPath2);
    System.out.println("SST2 read: " + new String(db.get((prefix2 + "sst_key_2").getBytes())));
  }

  private static String createSstFile(String prefix) throws RocksDBException {
    try {
      final String sstPath = Files.createTempFile("rocksdb-sample", ".sst").toString();
      try (final EnvOptions envOptions = new EnvOptions();
          final Options sstOptions = new Options();
          final SstFileWriter sstFileWriter = new SstFileWriter(envOptions, sstOptions)) {
        sstFileWriter.open(sstPath);
        sstFileWriter.put((prefix + "sst_key_1").getBytes(), (prefix + "sst_value_1").getBytes());
        sstFileWriter.put((prefix + "sst_key_2").getBytes(), (prefix + "sst_value_2").getBytes());
        sstFileWriter.finish();
      }
      return sstPath;
    } catch (IOException e) {
      throw new RuntimeException("Unable to create SST file", e);
    }
  }

  private static void ingestSstFile(final RocksDB db, final String sstPath)
      throws RocksDBException {
    try (final IngestExternalFileOptions options = new IngestExternalFileOptions()) {
      options.setMoveFiles(true);
      db.ingestExternalFile(Collections.singletonList(sstPath), options);
    }
  }

  private static void runWriteLoop(final RocksDB db, final AtomicBoolean running) {
    long counter = 0;
    while (running.get() && !Thread.currentThread().isInterrupted()) {
      try {
        final byte[] key = ("writer_key_" + counter).getBytes(StandardCharsets.UTF_8);
        final byte[] value = ("writer_value_" + counter).getBytes(StandardCharsets.UTF_8);
        db.put(key, value);
        counter++;
        if (counter%100 == 0){
            System.out.println("counter: " + counter);
        }
        Thread.sleep(WRITE_INTERVAL.toMillis());
      } catch (RocksDBException e) {
        System.err.println("Writer error: " + e.getMessage());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  private static void createBackupSnapshot(final RocksDB db) {
    try {
      final Path backupDir = Files.createTempDirectory("rocksdb-backup");
      BACKUP_DIRECTORIES.add(backupDir);
      System.out.println("Starting backup into: " + backupDir);
      try (final BackupEngineOptions options = new BackupEngineOptions(backupDir.toString());
          final BackupEngine backupEngine = BackupEngine.open(Env.getDefault(), options)) {
        backupEngine.createNewBackup(db, false);
        System.out.println("Backup completed: " + backupDir);
      }
    } catch (IOException | RocksDBException e) {
      System.err.println("Backup failed: " + e.getMessage());
    }
  }

  private static void cleanupTempResources() {
    BACKUP_DIRECTORIES.forEach(RocksDbSample::deleteDirectoryQuietly);
    deleteDirectoryQuietly(DB_PATH);
  }

  private static void deleteDirectoryQuietly(final Path dir) {
    if (dir == null) {
      return;
    }
    try (final Stream<Path> walk = Files.walk(dir)) {
      walk.sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException e) {
                  System.err.println("Failed to delete " + path + ": " + e.getMessage());
                }
              });
    } catch (IOException e) {
      System.err.println("Failed to clean directory " + dir + ": " + e.getMessage());
    }
  }
}
