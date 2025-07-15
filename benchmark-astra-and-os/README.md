Benchmarking Astra and OpenSearch
==================

Setup
----

1. Ensure you have Docker installed and running.
2. Clone the repository:
3. Assumes you have
   4. ruby
   5. docker
   6. astra checked out in an adjacent directory
3. setup the environment:
    ```bash
        ./setup_data.sh
        ./setup_clusters.sh
        ./setup_indices.sh
    ```
4. Run the benchmark:
   ```bash
    ./benchmark.rb
    ```

How do we do the variable size tests?


Maybe the benchmark could look like:

- destroy docker containers if they exist
- create and run docker containers, astra and OS
- for each size we want to test:
- add the next N (7500)
- run benchmark
report the results
