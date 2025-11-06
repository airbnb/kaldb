Rocksdb benchmarks


Benchmark target: 2,621,440 entries x 4096B (~10.00 GB)
RocksDB path: /var/folders/j_/jhqlsqn93gsdvg22yfqzlyyc0000gn/T/rocksdb-benchmark4867180641980588801
Query hit rate: 100.00%
Insert throughput: 600.30 MB/s (2621440 ops in 17.09 s)
Query throughput: 0.02 Mops/s (1000000 ops in 55.79 s)


Benchmark target: 2,621,440 entries x 4096B (~10.00 GB)
RocksDB path: /var/folders/j_/jhqlsqn93gsdvg22yfqzlyyc0000gn/T/rocksdb-benchmark1175889750312344671
Query hit rate: 100.00%
Insert throughput: 579.03 MB/s (2621440 ops in 17.72 s)
Query throughput: 0.03 Mops/s (2621440 ops in 96.12 s)

Process finished with exit code 0


Benchmark target: 7,864,320 entries x 4096B (~30.00 GB)
RocksDB path: /var/folders/j_/jhqlsqn93gsdvg22yfqzlyyc0000gn/T/rocksdb-benchmark950026121298917897
Query hit rate: 100.00%
Insert throughput: 505.41 MB/s (7864320 ops in 60.90 s)
Query throughput: 0.01 Mops/s (7864320 ops in 696.58 s)

Process finished with exit code 0


ROCKSDB file structure

☁ ls -lh /var/folders/j_/jhqlsqn93gsdvg22yfqzlyyc0000gn/T/rocksdb-benchmark6656839021107673938 
total 215800
-rw-r--r--@ 1 suman  staff   5.6M Nov  5 19:45 000009.sst
-rw-r--r--@ 1 suman  staff   5.6M Nov  5 19:45 000011.sst
-rw-r--r--@ 1 suman  staff   5.6M Nov  5 19:45 000013.sst
-rw-r--r--@ 1 suman  staff   5.6M Nov  5 19:45 000015.sst
-rw-r--r--@ 1 suman  staff   5.6M Nov  5 19:45 000017.sst
-rw-r--r--@ 1 suman  staff   5.6M Nov  5 19:45 000019.sst
-rw-r--r--@ 1 suman  staff   5.6M Nov  5 19:45 000021.sst
-rw-r--r--@ 1 suman  staff   5.6M Nov  5 19:45 000023.sst
-rw-r--r--@ 1 suman  staff   5.6M Nov  5 19:45 000025.sst
-rw-r--r--@ 1 suman  staff   5.6M Nov  5 19:45 000027.sst
-rw-r--r--@ 1 suman  staff   5.6M Nov  5 19:45 000029.sst
-rw-r--r--@ 1 suman  staff   5.6M Nov  5 19:45 000031.sst
-rw-r--r--@ 1 suman  staff   5.6M Nov  5 19:45 000033.sst
-rw-r--r--@ 1 suman  staff   5.6M Nov  5 19:45 000035.sst
-rw-r--r--@ 1 suman  staff   5.6M Nov  5 19:45 000037.sst
-rw-r--r--@ 1 suman  staff    16M Nov  5 19:45 000038.log
-rw-r--r--@ 1 suman  staff   5.6M Nov  5 19:45 000039.sst
-rw-r--r--@ 1 suman  staff    16B Nov  5 19:45 CURRENT
-rw-r--r--@ 1 suman  staff    36B Nov  5 19:45 IDENTITY
-rw-r--r--@ 1 suman  staff     0B Nov  5 19:45 LOCK
-rw-r--r--@ 1 suman  staff    92K Nov  5 19:45 LOG
-rw-r--r--@ 1 suman  staff   3.5K Nov  5 19:45 MANIFEST-000005
-rw-r--r--@ 1 suman  staff   7.1K Nov  5 19:45 OPTIONS-000007

SST files are all of equal size.
CURRENT points to current MANIFEST file
MANIFEST-000005 points to current manifest file. 
LOG is operations LOG file.
*.log points is the WAL and contains the data and changes.
LOCK is the process lock file.
MANIFEST-000005 is manifest file version possibly updated after every sst file change.
OPTIONS-* file contains the DB options used for each store.
