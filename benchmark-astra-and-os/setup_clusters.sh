#!/bin/zsh
set -eu
# cluster setup bits
# -----
source common.sh
# build & start opensearch
docker run -d -p 9200:9200 -p 9600:9600 -e "discovery.type=single-node" \
  -e "OPENSEARCH_INITIAL_ADMIN_PASSWORD=$OS_PW" opensearchproject/opensearch:latest


# build & start astra
pushd ../astra || exit 1
  # git checkout zparekh/local_bulk_ingest_api
  docker build -t slackhq/astra .
  docker compose up -d
popd

