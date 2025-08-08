# Commands Used (for report)

## Cluster with Docker Compose
```bash
docker compose up -d
bash scripts/init-ha.sh
```

## Upload files to HDFS
```bash
bash scripts/put-data.sh
```

## Run Spark jobs from inside container
```bash
bash scripts/run-jobs.sh
```

## Exact HDFS I/O paths
- Input trips Parquet: `hdfs://hdfs-cluster/data/taxi/yellow_tripdata*.parquet`
- Input zones CSV:   `hdfs://hdfs-cluster/data/taxi/taxi_zone_lookup.csv`
- Output Q1:         `hdfs://hdfs-cluster/output/q1_long_trips.parquet`
- Output Q2:         `hdfs://hdfs-cluster/output/q2_avg_fare_by_zone.parquet`
- Output Q3:         `hdfs://hdfs-cluster/output/q3_revenue_by_borough.parquet`
- Output Q4:         `hdfs://hdfs-cluster/output/q4_max_tip_per_day.parquet`
```
