package com.slack.astra.chunk;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDbSample {
    public static final String DB_FILE_PATH = "/Users/suman_karumuri/temp/rocksdb";

    static {
        RocksDB.loadLibrary();
    }

    public static void main(final String[] args) throws RocksDBException {
        final byte[] key1 = "key1".getBytes();

        // Create the DB.
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final RocksDB db = RocksDB.open(options, DB_FILE_PATH)) {
                db.put(key1, "value1".getBytes());
                System.out.println(new String(db.get(key1)));
            } catch (RocksDBException e) {
                System.err.println(STR."Error\{e.getMessage()}");
            }
        }

        // Open DB and read a key
        try (final RocksDB db = RocksDB.open(new Options(), DB_FILE_PATH)) {
            System.out.println("Second try");
            db.put(key1, "value2".getBytes());
            System.out.println(new String(db.get(key1)));
        } catch (RocksDBException e) {
            System.err.println(STR."Error\{e.getMessage()}");
        }
    }
}