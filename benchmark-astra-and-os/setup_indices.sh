#!/bin/zsh

set -eu

# Setup and fill indices in OpenSearch and Astra clusters.
source common.sh
# prep Opensearch

# delete index
curl -XDELETE "https://localhost:9200/test" -ku admin:$OS_PW
# create index with field mappings
curl -X PUT "https://localhost:9200/test" -H "Content-Type: application/json" -ku admin:$OS_PW \
 -d '{ "mappings": { "properties": { "dropoff_datetime": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss" }}}}'


load_ct_splits=${1:-10000}

echo "loading $load_ct_splits files into OpenSearch and Astra clusters"
# xargs -P parallelism
# load the first n files into opensearch and astra
for f in $(ls data/ready/* | head -n $load_ct_splits); do
  echo -n "$f\t"
  echo -n "OpenSearch: "
  curl --fail -H "Content-Type: application/x-ndjson" -XPOST "https://localhost:9200/_bulk" \
     --data-binary @$f  -ku admin:$OS_PW -s > /dev/null  && echo -n " DONE." || echo -n " FAILED."
  echo -n " Astra:"
  curl --fail -H "Content-Type: application/x-ndjson" -k -XPOST "http://localhost:8080/_local_bulk" --data-binary @$f -s > /dev/null && echo " DONE." || echo " FAILED."
done