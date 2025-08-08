package com.slack.astra.benchmarks;

public class StressTestRunner {

  public static void main(String[] args) throws InterruptedException {
    System.out.println("Starting stress test to find fallover points...");

    // Test configurations: [RPS, spans/request, duration]
    int[][] testConfigs = {
      {1, 1, 30}, // Baseline
      {5, 1, 30},
      {10, 1, 30},
      {25, 1, 30},
      {50, 1, 30},
      {100, 1, 30},
      {10, 5, 30}, // More spans per request
      {10, 10, 30},
      {10, 25, 30},
      {25, 10, 30}, // Higher load combinations
      {50, 5, 30},
    };

    String[] walTypes = {"kafka", "s3"};

    for (String walType : walTypes) {
      System.out.printf("%n=== Testing %s WAL ===%n", walType.toUpperCase());

      for (int[] config : testConfigs) {
        int rps = config[0];
        int spans = config[1];
        int duration = config[2];

        System.out.printf("%nTest: %d RPS, %d spans/req%n", rps, spans);

        try {
          new BatchedThroughputBenchmark(rps, spans, duration, walType).runBenchmark();
          Thread.sleep(2000); // Brief pause between tests
        } catch (Exception e) {
          System.err.printf("Test failed: %s%n", e.getMessage());
        }
      }
    }
  }
}
