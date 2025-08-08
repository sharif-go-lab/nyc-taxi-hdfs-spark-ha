# NYC Taxi Analytics on HA HDFS + Spark (Docker)

A precise implementation of your Big Data assignment:
- **HA HDFS** (2× NameNode with automatic failover, 3× JournalNode, 3× DataNode)
- **ZooKeeper** for failover
- **Spark (Java)** app that reads from **HDFS** and writes outputs back to **HDFS** (Parquet)

Everything runs in Docker. You only interact with HDFS URIs like `hdfs://hdfs-cluster/...`.

## Prerequisites
- Docker & Docker Compose
- ~6–8 GB free RAM

> If Docker Hub is blocked for your IP, use a VPN or a mirror. Images used:
> `apache/hadoop:3`, `zookeeper:3.8`, `bitnami/spark:3.5`, `maven:3.9-eclipse-temurin-17`.

## Project layout
```
.
├── docker-compose.yaml
├── hadoop/
│   ├── core-site.xml
│   └── hdfs-site.xml
├── scripts/
│   ├── init-ha.sh          # bootstraps and starts HA HDFS
│   ├── put-data.sh         # uploads ./data/* to HDFS /data/taxi
│   └── run-jobs.sh         # builds and runs the Spark job
├── spark-app/
│   ├── pom.xml
│   └── src/main/java/org/bigdata/taxi/TaxiAnalytics.java
└── data/                   # (you put source files here)
```

## 1) Bring up containers
```bash
docker compose -f docker-compose.yaml up -d
```

## 2) Initialize HA HDFS
```bash
bash scripts/init-ha.sh
```
This formats `namenode1`, initializes shared edits in JournalNodes, bootstraps `namenode2`, formats ZooKeeper for ZKFC, starts ZKFC and all DataNodes.

- Web UI: **http://localhost:9870** (NN1), **http://localhost:9871** (NN2)
- Logical HDFS URI: **`hdfs://hdfs-cluster`**

## 3) Prepare data and upload to HDFS
Place these files into `./data` (on your host):
- `yellow_tripdata_<date>.parquet` (one or more files)
- `taxi_zone_lookup.csv`

Then run:
```bash
bash scripts/put-data.sh
```
This creates `/data/taxi` in HDFS and uploads your files there.

## 4) Build and run Spark (inside containers)
```bash
bash scripts/run-jobs.sh
```
This compiles the Java app with Maven inside the `builder` container and then uses `spark-submit` inside the `spark` container to run the job against HDFS.

Outputs (Parquet) are written under these HDFS paths:
- `/output/q1_long_trips.parquet`
- `/output/q2_avg_fare_by_zone.parquet`
- `/output/q3_revenue_by_borough.parquet`
- `/output/q4_max_tip_per_day.parquet`

You can inspect outputs via:
```bash
docker compose exec -T namenode1 bash -lc 'hdfs dfs -ls -R /output && hdfs dfs -head /output/q4_max_tip_per_day.parquet/_SUCCESS || true'
```

## 5) Running individual commands (for your report)

### Bring up and bootstrap
```bash
docker compose up -d
bash scripts/init-ha.sh
```

### Upload data
```bash
bash scripts/put-data.sh
```

### Run Spark job
```bash
bash scripts/run-jobs.sh
```

### Direct HDFS CLI examples (from within `namenode1`)
```bash
docker compose exec -T namenode1 bash -lc '
  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  hdfs dfs -ls /
  hdfs dfs -ls -R /data/taxi
  hdfs dfs -ls -R /output
'
```

## Notes
- The Spark job uses **only HDFS paths** (no local FS).
- The logical NameNode URI is `hdfs://hdfs-cluster` and is encoded in `hadoop/core-site.xml`. Spark learns it via `HADOOP_CONF_DIR`.
- The app uses Spark SQL `Dataset` API and produces 4 Parquet outputs as required.

## Troubleshooting
- **Ports 9870/9871 busy**: change the host ports in `docker-compose.yaml`.
- **JournalNode or NN crashes**: `docker compose logs <service>` and verify volume permissions. Containers run as root, so normally fine.
- **Docker Hub 403 (sanctions)**: use a VPN or pull from a mirror/registry within your network.
