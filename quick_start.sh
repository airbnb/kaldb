#!/usr/bin/env bash
# start-astra.sh
echo "Starting quick start script."
echo "Building and starting docker container. Can take sometime..."
docker build -t slackhq/astra .
docker compose up -d  # run in detached mode
# wait for services to come up
sleep 60
# create Kafka topic
echo "Creating kafka container..."
docker exec kafka-container kafka-topics.sh --create --topic test-topic-in --bootstrap-server localhost:9092
# configure dataset
curl -XPOST ... CreateDatasetMetadata ...
curl -XPOST ... UpdatePartitionAssignment ...
echo "Setup complete. Ingest now."

