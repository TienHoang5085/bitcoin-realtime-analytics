#!/usr/bin/env bash
# Fault-tolerance test helper, run on Laptop 1 from the repo root (Git Bash).
#
#   tests/fault_tolerance/ft.sh prepare          copy lakehouse_v3 to ft/, reset topic ft-test
#   NODE2_IP=<ip> tests/fault_tolerance/ft.sh driver   start the streaming driver (container ft-driver)
#   tests/fault_tolerance/ft.sh feed FROM COUNT  send COUNT chunks of 300 messages, one a minute
#   tests/fault_tolerance/ft.sh report           save the driver log and summarize it
#   tests/fault_tolerance/ft.sh verify           reconcile Bronze/Silver/Gold/MongoDB
#   tests/fault_tolerance/ft.sh stop             stop and remove ft-driver
#
# Everything is written to s3a://crypto-lake/ft/, topic ft-test and database
# crypto_analytics_ft. Real tables, topics and collections are never touched.
set -euo pipefail
export MSYS_NO_PATHCONV=1

REPO=$(pwd -W 2>/dev/null || pwd)
HERE="$REPO/tests/fault_tolerance"
OUT="$HERE/out"
NET=bitcoin-realtime-analytics_crypto-net
IMAGE=crypto-analytics/spark:latest
MONGO_ADMIN="mongodb://root:changeme@localhost:27017/?authSource=admin"
mkdir -p "$OUT"
say() { echo "$(date -u +%H:%M:%S) $*"; }

common_env=(
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 -e S3_ENDPOINT=http://minio:9000
  -e AWS_ACCESS_KEY_ID=admin -e AWS_SECRET_ACCESS_KEY=password -e AWS_REGION=us-east-1
  -e MONGO_URI=mongodb://root:changeme@mongodb:27017 -e MONGO_DB=crypto_analytics_ft
  -e DELTA_BASE_PATH=s3a://crypto-lake/ft/lakehouse -e SPARK_CHECKPOINT_BASE=s3a://crypto-lake/ft/checkpoints
  -e PYTHONPATH=/opt/spark/work-dir/spark:/opt/spark/work-dir
  -v "$REPO/spark:/opt/spark/work-dir/spark:ro" -v "$HERE:/ft:ro" -w /opt/spark/work-dir
)
delta_confs=(
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
)

case "${1:-}" in
prepare)
  docker run --rm --network $NET --entrypoint sh bitnamilegacy/minio-client:latest -c \
    "mc alias set m http://minio:9000 admin password >/dev/null && mc rm -r --force m/crypto-lake/ft/ >/dev/null 2>&1; mc mirror --quiet m/crypto-lake/lakehouse_v3 m/crypto-lake/ft/lakehouse >/dev/null && echo 'delta copied to ft/'"
  docker exec crypto-mongodb mongosh --quiet "$MONGO_ADMIN" --eval \
    "const t = db.getSiblingDB('crypto_analytics_ft'); t.dropDatabase(); db.getSiblingDB('crypto_analytics_v3').gold_ohlcv.aggregate([{\$out: {db: 'crypto_analytics_ft', coll: 'gold_ohlcv'}}]); print('mongo copied', t.gold_ohlcv.countDocuments())"
  docker exec crypto-kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic ft-test >/dev/null 2>&1 || true
  sleep 3
  docker exec crypto-kafka kafka-topics --bootstrap-server kafka:29092 --create --topic ft-test --partitions 5
  ;;
driver)
  : "${NODE2_IP:?set NODE2_IP to the IP of Laptop 2}"
  docker run -d --name ft-driver --network $NET --network-alias spark-driver --memory 1g \
    --add-host "spark-worker-2:$NODE2_IP" -p 7078:7078 -p 7079:7079 -p 4041:4040 \
    -e SPARK_MASTER=spark://spark-master:7077 "${common_env[@]}" \
    $IMAGE /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    --conf spark.driver.memory=512m --conf spark.executor.memory=1g --conf spark.executor.cores=2 \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.driver.host=spark-driver --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.driver.port=7078 --conf spark.driver.blockManager.port=7079 \
    --conf spark.blockManager.port=7090 \
    --conf "spark.driver.extraJavaOptions=-Xlog:gc:file=/tmp/gc.log:uptime -Dlog4j2.configurationFile=/ft/log4j2-ft.properties" \
    "${delta_confs[@]}" /ft/ft_job.py
  until docker logs ft-driver 2>&1 | grep -q "query started"; do
    [ "$(docker inspect -f '{{.State.Running}}' ft-driver)" = "true" ] || { docker logs ft-driver 2>&1 | tail -30; exit 1; }
    sleep 3
  done
  say "driver up; Spark UI http://localhost:4041, master UI http://localhost:8082"
  ;;
feed)
  from=${2:?FROM chunk}; count=${3:?COUNT}
  for n in $(seq "$from" $((from + count - 1))); do
    python "$HERE/gen_feed.py" "$n" | docker exec -i crypto-kafka kafka-console-producer \
      --bootstrap-server kafka:29092 --topic ft-test --property parse.key=true --property "key.separator=|"
    say "fed chunk $n"
    sleep 60
  done
  ;;
report)
  log="$OUT/driver-$(date -u +%Y%m%d-%H%M%S).log"
  docker logs ft-driver > "$log" 2>&1
  docker cp ft-driver:/tmp/gc.log "${log%.log}-gc.log" >/dev/null 2>&1 || true
  say "saved $log"
  python "$HERE/report.py" "$log"
  ;;
verify)
  docker run --rm --network $NET --memory 1500m "${common_env[@]}" $IMAGE \
    /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 1g \
    --conf spark.sql.shuffle.partitions=4 "${delta_confs[@]}" /ft/verify.py 2>&1 \
    | grep -E "^RESULT|Error|Traceback" | tee "$OUT/verify-$(date -u +%Y%m%d-%H%M%S).txt"
  ;;
stop)
  docker rm -f ft-driver >/dev/null 2>&1 && say "ft-driver removed" || true
  ;;
*)
  sed -n '2,13p' "$0"; exit 1
  ;;
esac
