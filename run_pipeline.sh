#!/usr/bin/env bash
# End-to-end pipeline: bring up Kafka (Docker) + Hadoop (native, see below),
# stream the dataset through Kafka into HDFS, run the three MapReduce jobs,
# pull results back down as CSVs, and validate them against a pandas
# groupby.
#
# Why Hadoop runs natively instead of in Docker: Apache/bde2020 only
# publish amd64 Hadoop images. On Apple Silicon that means every JVM
# instruction is emulated (QEMU/Rosetta), and a JVM under emulation is
# brutally slow -- NameNode alone took 10+ minutes of CPU without finishing
# startup in testing. Native Homebrew Hadoop (see hadoop-conf/) is the same
# Java bytecode running on a native ARM64 JVM: daemons start in seconds.
# Kafka stays in Docker because its image genuinely is multi-arch.
#
# Usage: ./run_pipeline.sh [--replicate N] [--skip-infra] [--skip-streaming] [--skip-validate]
set -euo pipefail
cd "$(dirname "$0")"
PROJECT_ROOT="$(pwd)"

export HADOOP_CONF_DIR="${PROJECT_ROOT}/hadoop-conf"
export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
PY="${PROJECT_ROOT}/.venv/bin/python"

REPLICATE=1
SKIP_INFRA=false
SKIP_STREAMING=false
SKIP_VALIDATE=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --replicate) REPLICATE="$2"; shift 2 ;;
    --skip-infra) SKIP_INFRA=true; shift ;;
    --skip-streaming) SKIP_STREAMING=true; shift ;;
    --skip-validate) SKIP_VALIDATE=true; shift ;;
    *) echo "unknown arg: $1"; exit 1 ;;
  esac
done

log() { echo -e "\n>>> $*"; }

# --------------------------------------------------------------- infra ---
if [[ "$SKIP_INFRA" == false ]]; then
  log "starting Kafka (Docker)"
  docker compose up -d kafka
  log "waiting for Kafka to accept connections on localhost:9092"
  until (echo > /dev/tcp/localhost/9092) 2>/dev/null; do sleep 2; done

  log "ensuring native Hadoop daemons are running"
  running() { jps | grep -q "$1"; }
  running NameNode        || hdfs --daemon start namenode
  running DataNode        || hdfs --daemon start datanode
  running ResourceManager || yarn --daemon start resourcemanager
  running NodeManager     || yarn --daemon start nodemanager

  log "waiting for HDFS namenode web UI (http://localhost:9870)"
  until curl -sf -o /dev/null http://localhost:9870; do sleep 2; done
  log "waiting for HDFS to leave safe mode"
  hdfs dfsadmin -safemode wait
  log "waiting for YARN resourcemanager (http://localhost:8088)"
  until curl -sf -o /dev/null http://localhost:8088; do sleep 2; done
fi

log "creating HDFS directories"
hdfs dfs -mkdir -p /data/raw/student_depression
hdfs dfs -mkdir -p /data/processed

# ------------------------------------------------------------ streaming ---
if [[ "$SKIP_STREAMING" == false ]]; then
  log "starting HDFS-writer consumer (background)"
  "$PY" consumer/consumer.py > /tmp/bda-consumer.log 2>&1 &
  CONSUMER_PID=$!
  sleep 3

  log "streaming dataset through Kafka producer (replicate=${REPLICATE})"
  "$PY" producer/producer.py --replicate "${REPLICATE}"

  log "waiting for consumer to flush final batches"
  sleep 20
  kill -TERM "$CONSUMER_PID"
  wait "$CONSUMER_PID" 2>/dev/null || true

  log "raw files landed in HDFS:"
  hdfs dfs -ls /data/raw/student_depression
fi

# -------------------------------------------------------------- jobs -----
STREAMING_JAR="$(find /opt/homebrew/opt/hadoop/libexec/share/hadoop/tools/lib -name 'hadoop-streaming*.jar' | head -1)"

run_job() {
  local name=$1; local combiner=$2; local reducer=$3; local reduces=$4
  log "running MapReduce job: ${name}"
  hdfs dfs -rm -r -f "/data/processed/${name}" || true

  local files="mapreduce/common.py,mapreduce/${name}/mapper.py"
  local extra_args=()
  if [[ -n "$combiner" ]]; then
    files="${files},mapreduce/${name}/${combiner}"
    extra_args+=(-combiner "python3 ${combiner}")
  fi
  if [[ -n "$reducer" ]]; then
    files="${files},mapreduce/${name}/${reducer}"
    extra_args+=(-reducer "python3 ${reducer}")
  fi

  # bash 3.2 (macOS default) throws "unbound variable" on a plain empty
  # array expansion under `set -u`; the `+"${arr[@]}"` guard avoids it.
  hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces="${reduces}" \
    -files "$files" \
    -mapper "python3 mapper.py" \
    "${extra_args[@]+"${extra_args[@]}"}" \
    -input /data/raw/student_depression \
    -output "/data/processed/${name}"
}

run_job baseline "" "" 0
run_job job1 combiner.py reducer.py 1
run_job job2 combiner.py reducer.py 1

# ------------------------------------------------------------ getmerge ---
# Hadoop Streaming's TextOutputFormat treats a reducer/mapper line with no
# tab in it as "key" with an empty "value", then rejoins key+sep+value on
# write -- so every line comes back with a spurious trailing tab. Strip it
# on the way out instead of via getmerge directly, since getmerge can't be
# piped through a filter.
log "merging job outputs back to local data/"
hdfs dfs -cat /data/processed/baseline/part-* | sed $'s/\t*$//' > data/_baseline_merged.csv
hdfs dfs -cat /data/processed/job1/part-* | sed $'s/\t*$//' > data/_job1_merged.csv
hdfs dfs -cat /data/processed/job2/part-* | sed $'s/\t*$//' > data/_job2_merged.csv

log "post-processing merged outputs into final deliverable CSVs"
{
  echo "id,age,gender,cgpa,sleep_duration,academic_pressure,study_satisfaction,depression_status"
  cat data/_baseline_merged.csv
} > data/processed/baseline_student_depression.csv

cp data/_job1_merged.csv data/processed/processed_student_health_summary.csv

"$PY" mapreduce/job2/split_output.py data/_job2_merged.csv data/processed

rm -f data/_baseline_merged.csv data/_job1_merged.csv data/_job2_merged.csv

log "deliverables written to data/processed/:"
ls -la data/processed/

# ----------------------------------------------------------- validate ----
if [[ "$SKIP_VALIDATE" == false ]]; then
  log "validating MapReduce output against pandas groupby"
  "$PY" validation/validate.py
fi

log "pipeline complete. Launch the dashboard with:"
echo "  .venv/bin/python dashboard/app.py"
echo "  then open http://localhost:8050"
