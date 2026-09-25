# NYC Taxi Analytics on HA HDFS and Spark

A Docker Compose setup for a highly available HDFS cluster, plus a Java Spark job that analyzes New York City yellow taxi trips stored in it. The job reads trip records (Parquet) and the taxi zone lookup table (CSV) from HDFS, answers four questions with the Spark `Dataset` API, and writes each answer back to HDFS as Parquet. Every path it uses is an HDFS URI under the logical nameservice `hdfs://hdfs-cluster`, so jobs keep working when the active NameNode changes.

## Cluster

| Service | Count | Role |
|---|---|---|
| `namenode1`, `namenode2` | 2 | Active and standby NameNode (`nn1`, `nn2`). Each also runs a ZKFC that fails over automatically. |
| `journalnode1` to `journalnode3` | 3 | Quorum Journal Manager that holds the shared edit log. |
| `datanode1` to `datanode3` | 3 | Block storage. |
| `zookeeper` | 1 | Leader election for the ZKFCs. |
| `builder` | 1 | Maven 3.9 with Java 17, for building the Spark job. |
| `spark` | 1 | Spark 3.5, used to run `spark-submit` in local mode against HDFS. |

The Hadoop configuration is in `hadoop/` and is mounted into every Hadoop container and the Spark container:

- `core-site.xml` sets `fs.defaultFS` to `hdfs://hdfs-cluster` and points the failover controllers at `zookeeper:2181`.
- `hdfs-site.xml` defines the nameservice `hdfs-cluster` with `nn1` (`namenode1:9820`) and `nn2` (`namenode2:9820`), the shared edits directory on the three JournalNodes, the client failover proxy, and automatic failover. Fencing is `shell(/bin/true)`, so no SSH is needed inside containers, and HDFS permissions are turned off.

Each NameNode, JournalNode and DataNode keeps its data in a named Docker volume, so the file system survives container restarts.

## Requirements

- Docker with Compose v2
- 6 to 8 GB of free memory
- Access to the images `apache/hadoop:3`, `zookeeper:3.8`, `bitnami/spark:3.5` and `maven:3.9-eclipse-temurin-17`. If Docker Hub is blocked where you are, use a mirror or a VPN.

Every service is pinned to `linux/amd64`, so on an ARM machine (for example Apple Silicon) the containers run under emulation and are noticeably slower.

## Running it

### 1. Start the containers

```bash
docker compose up -d
```

The Hadoop containers start idle. The next script starts the HDFS daemons inside them.

### 2. Initialize and start HDFS

```bash
bash scripts/init-ha.sh
```

The script:

1. formats `namenode1` (only the first time) and starts it;
2. initializes the shared edit log on the JournalNodes;
3. bootstraps `namenode2` as a standby (only the first time) and starts it;
4. formats the failover state in ZooKeeper and starts a ZKFC next to each NameNode;
5. starts the three DataNodes;
6. prints the HA state of `nn1` and `nn2`, which should be one `active` and one `standby`.

The NameNode web UIs are at http://localhost:9870 (`nn1`) and http://localhost:9871 (`nn2`).

Formatting and bootstrapping happen only on the first run, when no NameNode metadata exists yet. After restarting the whole stack, run the script again to start the daemons.

### 3. Upload the data

Download the yellow taxi trip records (one or more `yellow_tripdata_*.parquet` files) and `taxi_zone_lookup.csv` from the [NYC TLC trip record data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), put them in `./data`, then run:

```bash
bash scripts/put-data.sh
```

`./data` is mounted read-only into `namenode1`, and the script copies everything in it to `/data/taxi` in HDFS. The folder is git-ignored.

### 4. Build and run the Spark job

```bash
bash scripts/run-jobs.sh
```

This builds `spark-app` with `mvn package` in the `builder` container, then runs it in the `spark` container:

```bash
spark-submit --master local[*] --class org.bigdata.taxi.TaxiAnalytics \
  /opt/spark-app/target/taxi-analytics-1.0.jar \
  "hdfs://hdfs-cluster/data/taxi/yellow_tripdata*.parquet" \
  hdfs://hdfs-cluster/data/taxi/taxi_zone_lookup.csv \
  hdfs://hdfs-cluster/output
```

It lists `/output` when it finishes.

## The analysis

`TaxiAnalytics.java` casts the columns it needs (pickup and drop-off times, passenger count, distance, fare, total, tip, pickup and drop-off location IDs) to the right types, joins trips with the zone table on `LocationID` where needed, and writes four Parquet datasets, overwriting earlier results:

| Output (under `hdfs://hdfs-cluster/output/`) | Question |
|---|---|
| `q1_long_trips.parquet` | Trips with more than 2 passengers and more than 5 miles, with a `duration_minutes` column, longest first. |
| `q2_avg_fare_by_zone.parquet` | Average `fare_amount` per pickup zone (zone and borough), highest first. |
| `q3_revenue_by_borough.parquet` | Sum of `total_amount` and number of trips per drop-off borough, highest revenue first. |
| `q4_max_tip_per_day.parquet` | Largest `tip_amount` for each pickup date, in date order. |

The zone joins are left joins, so trips whose location ID isn't in the lookup table are grouped under a null zone and borough.

To look at a result, query it with `spark-sql` in the Spark container:

```bash
docker compose exec -T spark bash -lc 'export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  /opt/bitnami/spark/bin/spark-sql -e "SELECT * FROM parquet.\`hdfs://hdfs-cluster/output/q3_revenue_by_borough.parquet\`"'
```

## Useful HDFS commands

Run these inside `namenode1` (or any Hadoop container):

```bash
docker compose exec -T namenode1 bash -lc '
  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  hdfs haadmin -getServiceState nn1
  hdfs haadmin -getServiceState nn2
  hdfs dfsadmin -report
  hdfs dfs -ls -R /data/taxi
  hdfs dfs -ls -R /output
'
```

To watch a failover, stop the container of the active NameNode and check the other one. If `nn1` is active:

```bash
docker compose stop namenode1
docker compose exec -T namenode2 bash -lc 'HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop hdfs haadmin -getServiceState nn2'
```

Within a few seconds the ZKFC on `namenode2` should promote `nn2` to `active`. To bring `namenode1` back, start the container and then its NameNode and ZKFC. It rejoins as the standby:

```bash
docker compose start namenode1
docker compose exec -T namenode1 bash -lc 'export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  hdfs --daemon start namenode && hdfs --daemon start zkfc'
```

[COMMANDS.md](COMMANDS.md) is a short list of the commands used, with the exact HDFS input and output paths.

## Project layout

```
docker-compose.yaml        ZooKeeper, 3 JournalNodes, 2 NameNodes, 3 DataNodes, Maven, Spark
hadoop/                    core-site.xml and hdfs-site.xml (HA nameservice hdfs-cluster)
scripts/init-ha.sh         formats, bootstraps and starts HDFS with automatic failover
scripts/put-data.sh        copies ./data into /data/taxi on HDFS
scripts/run-jobs.sh        builds the Spark job and runs it against HDFS
spark-app/                 Maven project with TaxiAnalytics.java (Java 17, Spark 3.5.1)
COMMANDS.md                command summary and HDFS paths
```

## Troubleshooting

- If ports 9870 or 9871 are taken, change the host ports of `namenode1` and `namenode2` in `docker-compose.yaml`.
- If a NameNode or JournalNode stops, check `docker compose logs <service>` and the logs under `/opt/hadoop/logs` inside the container.
- If `hdfs dfs` commands can't connect after restarting the stack, the daemons aren't running yet. Run `scripts/init-ha.sh` again.

## Authors

- [Kasra Siavashpour](https://github.com/kasra-sia)
- [Ardalan Siavashpour](https://github.com/Ardalan-Sia)
