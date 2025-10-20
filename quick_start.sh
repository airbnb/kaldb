#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# Astra Quick Start Script
# Usage:
#   ./quick-start.sh [OPTIONS]
#
# Options:
#   --clean     Remove old Astra containers, volumes, and images, then rebuild fresh
#   --help      Show this help message
#
# Examples:
#   ./quick-start.sh           # Start Astra using existing containers/images
#   ./quick-start.sh --clean   # Full rebuild from scratch
#
# After you're done, you can clean everything up with:
#   ./clean-astra.sh
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Parse arguments
# ------------------------------------------------------------------------------
CLEAN_BUILD=false

for arg in "$@"; do
  case $arg in
    --clean)
      CLEAN_BUILD=true
      shift
      ;;
    --help)
      echo "Astra Quick Start Script"
      echo ""
      echo "Usage:"
      echo "  ./quick-start.sh [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --clean     Remove old Astra containers, volumes, and images, then rebuild fresh"
      echo "  --help      Show this help message"
      echo ""
      echo "Examples:"
      echo "  ./quick-start.sh           # Start Astra using existing containers/images"
      echo "  ./quick-start.sh --clean   # Full rebuild from scratch"
      echo ""
      echo "To clean up everything afterwards:"
      echo "  ./clean-astra.sh"
      echo ""
      exit 0
      ;;
  esac
done

echo "🚀 Starting Astra demo environment..."

# ------------------------------------------------------------------------------
# Step 1. Stop existing Astra containers (only those defined in docker-compose.yml)
# ------------------------------------------------------------------------------
echo "🧹 Stopping Astra containers from docker-compose.yml..."
if docker compose ps -q | grep . >/dev/null 2>&1; then
  docker compose down --remove-orphans
else
  echo "No Astra containers to stop."
fi

# ------------------------------------------------------------------------------
# Step 2. Handle clean build option
# ------------------------------------------------------------------------------
if [ "$CLEAN_BUILD" = true ]; then
  echo "🔥 Performing full clean build..."
  docker compose down -v --remove-orphans
  docker image prune -af --filter "label=astra-demo=true" || true
else
  echo "⚡ Skipping clean build (use --clean for a fresh start)."
fi

# ------------------------------------------------------------------------------
# Step 3. Build Astra image if necessary
# ------------------------------------------------------------------------------
if [ "$CLEAN_BUILD" = true ]; then
  echo "🔨 Building Astra Docker image..."
  docker build -t astra:latest --label astra-demo=true .
else
  echo "⚡ Using existing Astra Docker image (run with --clean to rebuild)."
fi

# ------------------------------------------------------------------------------
# Step 4. Start Astra stack
# ------------------------------------------------------------------------------
echo "📦 Starting Astra stack via Docker Compose..."
docker compose up -d

# ------------------------------------------------------------------------------
# Step 5. Wait for services to initialize
# ------------------------------------------------------------------------------
echo "⏳ Waiting for Astra services to initialize..."
sleep 30

# ------------------------------------------------------------------------------
# Step 6. Configure Kafka topic and Astra dataset
# ------------------------------------------------------------------------------
echo "📡 Creating Kafka topic (if not exists)..."
docker exec dep_kafka kafka-topics.sh \
  --create \
  --topic test-topic-in \
  --if-not-exists \
  --bootstrap-server localhost:9092 || true

echo "🧩 Creating Astra dataset metadata..."
curl -s -XPOST http://localhost:8081/api/v1/datasets \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test-dataset",
    "topic": "test-topic-in",
    "retention": "P7D"
  }' || echo "Dataset may already exist."

echo "📦 Updating Astra partition assignment..."
curl -s -XPOST http://localhost:8081/api/v1/partitions/assignments \
  -H "Content-Type: application/json" \
  -d '{
    "dataset": "test-dataset",
    "numPartitions": 1,
    "replicas": 1
  }' || echo "Partition assignment may already exist."

# ------------------------------------------------------------------------------
# Step 7. Summary
# ------------------------------------------------------------------------------
echo ""
echo "✅ Astra demo environment is ready!"
echo "   - Manager UI:   http://localhost:8083"
echo "   - Query API:    http://localhost:8081"
echo "   - Grafana:      http://localhost:3000"
echo ""
echo "To ingest sample data, run: ./ingest-demo-data.sh"
echo "To stop and remove everything, run: ./clean-astra.sh"
echo ""

