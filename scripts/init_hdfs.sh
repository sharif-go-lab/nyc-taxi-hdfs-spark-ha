#!/bin/bash
set -e

hdfs dfs -mkdir -p /data/taxi
hdfs dfs -put -f /data/yellow_tripdata_2023-01.parquet /data/taxi/
hdfs dfs -put -f /data/taxi_zone_lookup.csv /data/taxi/
