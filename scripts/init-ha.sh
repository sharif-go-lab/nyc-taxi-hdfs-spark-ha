#!/usr/bin/env bash
set -euo pipefail

export COMPOSE="docker compose"

echo ">>> Waiting for JournalNodes to be reachable..."
$COMPOSE exec -T journalnode1 bash -lc 'true'
$COMPOSE exec -T journalnode2 bash -lc 'true'
$COMPOSE exec -T journalnode3 bash -lc 'true'

echo ">>> Formatting NameNode1 if needed..."
$COMPOSE exec -T namenode1 bash -lc '
  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  if [ ! -f /opt/hadoop/dfs/name/current/VERSION ]; then
    echo "[namenode1] formatting fresh namespace"
    hdfs namenode -format -force
  else
    echo "[namenode1] already formatted"
  fi
'

echo ">>> Starting NameNode1..."
$COMPOSE exec -T namenode1 bash -lc '
  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  hdfs --daemon start namenode
  sleep 3
  hdfs --daemon status namenode || true
'

echo ">>> Initialize shared edits on NameNode1 (ok to be already-initialized)..."
$COMPOSE exec -T namenode1 bash -lc '
  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  hdfs namenode -initializeSharedEdits -force || true
'

echo ">>> Bootstrap standby on NameNode2 if needed..."
$COMPOSE exec -T namenode2 bash -lc '
  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  if [ ! -f /opt/hadoop/dfs/name/current/VERSION ]; then
    hdfs namenode -bootstrapStandby -force
  else
    echo "[namenode2] already bootstrapped"
  fi
'

echo ">>> Starting NameNode2..."
$COMPOSE exec -T namenode2 bash -lc '
  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  hdfs --daemon start namenode
  sleep 3
  hdfs --daemon status namenode || true
'

echo ">>> Formatting ZooKeeper for automatic failover (first run only)..."
$COMPOSE exec -T namenode1 bash -lc '
  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  hdfs zkfc -formatZK -force || true
'

echo ">>> Starting ZKFC on both NameNodes..."
$COMPOSE exec -T namenode1 bash -lc 'hdfs --daemon start zkfc || true'
$COMPOSE exec -T namenode2 bash -lc 'hdfs --daemon start zkfc || true'

echo ">>> Starting DataNodes..."
for dn in datanode1 datanode2 datanode3; do
  $COMPOSE exec -T "$dn" bash -lc '
    export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
    hdfs --daemon start datanode || true
  '
done

echo ">>> Checking HA states..."
$COMPOSE exec -T namenode1 bash -lc '
  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  hdfs haadmin -getServiceState nn1 || true
  hdfs haadmin -getServiceState nn2 || true
  echo "Web UIs: http://localhost:9870 (nn1), http://localhost:9871 (nn2)"
'

echo ">>> DONE."
