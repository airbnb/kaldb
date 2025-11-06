package com.slack.astra.chunk;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

public class RocksDbSample {
  public static final String DB_FILE_PATH;

  static {
    RocksDB.loadLibrary();
    try {
      DB_FILE_PATH = Files.createTempDirectory("rocksdb-sample").toString();
    } catch (IOException e) {
      throw new RuntimeException("Unable to create temp RocksDB directory", e);
    }
  }

  public static void main(final String[] args) throws RocksDBException {
    final byte[] key1 = "key1".getBytes();
    final byte[] value1 = "value1".getBytes();

    try (final Options options = new Options().setCreateIfMissing(true)) {
      try (final RocksDB db = RocksDB.open(options, DB_FILE_PATH)) {
        db.put(key1, value1);
        System.out.println("Direct read: " + new String(db.get(key1)));

        String prefix1 = "sst1";
        final String sstPath = createSstFile(prefix1);
        ingestSstFile(db, sstPath);

        System.out.println("SST read: " + new String(db.get((prefix1 + "sst_key_2").getBytes())));
        System.out.println("Direct read: " + new String(db.get(key1)));
        String prefix2 = "sst2";
        final String sstPath2 = createSstFile(prefix2);
        ingestSstFile(db, sstPath2);
        System.out.println("SST read: " + new String(db.get((prefix1 + "sst_key_2").getBytes())));
        System.out.println("Direct read: " + new String(db.get(key1)));
        System.out.println("SST2 read: " + new String(db.get((prefix2 + "sst_key_2").getBytes())));
      }
    }
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
}
