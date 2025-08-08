#!/usr/bin/env bash
set -euo pipefail
export COMPOSE="docker compose"

APP_JAR="/opt/spark-app/target/taxi-analytics-1.0.jar"
TRIPS="hdfs://hdfs-cluster/data/taxi/yellow_tripdata*.parquet"
ZONES="hdfs://hdfs-cluster/data/taxi/taxi_zone_lookup.csv"
OUT="hdfs://hdfs-cluster/output"

echo ">>> Building Spark app (Maven inside Docker)..."
$COMPOSE exec -T builder bash -lc 'mvn -q -e -DskipTests package'

echo ">>> Running Spark job inside Spark container..."
$COMPOSE exec -T spark bash -lc "
  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  /opt/bitnami/spark/bin/spark-submit \
    --master local[*] \
    --class org.bigdata.taxi.TaxiAnalytics \
    $APP_JAR \
    $TRIPS \
    $ZONES \
    $OUT
"

echo ">>> Listing outputs:"
$COMPOSE exec -T namenode1 bash -lc '
  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  hdfs dfs -ls -R /output || true
'

echo ">>> DONE."
