#!/usr/bin/env bash
set -euo pipefail
export COMPOSE="docker compose"

if [ ! -d "./data" ]; then
  echo "Place your files in ./data first: yellow_tripdata_*.parquet and taxi_zone_lookup.csv"
  exit 1
fi

echo ">>> Creating /data/taxi in HDFS..."
$COMPOSE exec -T namenode1 bash -lc '
  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  hdfs dfs -mkdir -p /data/taxi
'

echo ">>> Uploading local ./data/* into HDFS /data/taxi"
$COMPOSE exec -T namenode1 bash -lc '
  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  hdfs dfs -put -f /data/host/* /data/taxi/
  hdfs dfs -ls -R /data/taxi
'

echo ">>> DONE."
