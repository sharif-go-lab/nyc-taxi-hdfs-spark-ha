#!/bin/bash
set -e

spark-submit \
  --class com.example.LongTripsJob \
  --master local \
  /jars/LongTripsJob.jar \
  --input hdfs:///data/taxi/yellow_tripdata_2023-01.parquet \
  --output hdfs:///output/q1_long_trips.parquet
