#!/usr/bin/env bash
#
# A/B benchmark for single-task adaptive executor (GUC ON vs OFF)
#
# Usage:
#   bash run_bench.sh [--setup]
#
# Options:
#   --setup     — Create and load the bench_kv table (1M rows) before running.
#                 Drops existing bench_kv if present. Only needed once.
#
# Environment overrides:
#   PORT        — coordinator port          (default: 9700)
#   DB          — database name             (default: citus)
#   DURATION    — seconds per pgbench run   (default: 60)
#   ITERATIONS  — iterations per config     (default: 3)
#   CLIENTS     — space-separated clients   (default: "1 8 32")
#   THREADS     — space-separated threads   (default: "1 8 16")
#   WORKLOADS   — space-separated workloads (default: "select update insert mixed")
#   SCRIPT_DIR  — pgbench script directory  (default: bench_scripts)
#   RESULTS_DIR — output directory          (default: bench_results/<timestamp>)
#   NUM_ROWS    — rows to load with --setup (default: 1000000)
#   SHARD_COUNT — shard count for --setup   (default: 32)
#
# Examples:
#   bash run_bench.sh --setup                          # first run: load data + benchmark
#   DURATION=10 ITERATIONS=2 bash run_bench.sh         # quick benchmark
#   CLIENTS="1 4" THREADS="1 4" WORKLOADS="select" bash run_bench.sh
#
set -euo pipefail

DO_SETUP=0
if [ "${1:-}" = "--setup" ]; then
    DO_SETUP=1
    shift
fi

PORT="${PORT:-9700}"
DB="${DB:-citus}"
DURATION="${DURATION:-60}"
ITERATIONS="${ITERATIONS:-3}"
RESULTS_DIR="${RESULTS_DIR:-bench_results/$(date +%Y%m%d_%H%M%S)}"
SCRIPT_DIR="${SCRIPT_DIR:-bench_scripts}"
NUM_ROWS="${NUM_ROWS:-1000000}"
SHARD_COUNT="${SHARD_COUNT:-32}"
mkdir -p "$RESULTS_DIR"

PSQL="psql -p $PORT -d $DB -X --no-psqlrc"

# ---------------------------------------------------------------
# Setup: create and load bench_kv
# ---------------------------------------------------------------
if [ "$DO_SETUP" -eq 1 ]; then
    echo "=== Setup: creating bench_kv table ($NUM_ROWS rows, $SHARD_COUNT shards) ==="
    SETUP_START=$(date +%s)

    $PSQL -q <<EOF
DROP TABLE IF EXISTS bench_kv;
SET citus.shard_count TO $SHARD_COUNT;
SET citus.shard_replication_factor TO 1;

CREATE TABLE bench_kv (
    kid bigint PRIMARY KEY,
    val int NOT NULL DEFAULT 0,
    payload text
);

SELECT create_distributed_table('bench_kv', 'kid');

-- Load rows in batches of 100k to avoid OOM on large loads.
-- Each row has a ~200-byte random text payload to simulate realistic OLTP rows.
DO \$\$
DECLARE
    batch_size int := 100000;
    total int := $NUM_ROWS;
    lo int := 1;
    hi int;
BEGIN
    WHILE lo <= total LOOP
        hi := LEAST(lo + batch_size - 1, total);
        EXECUTE format(
            'INSERT INTO bench_kv (kid, val, payload)
             SELECT g, 0, md5(random()::text) || md5(random()::text) ||
                    md5(random()::text) || md5(random()::text) ||
                    md5(random()::text) || md5(random()::text)
             FROM generate_series(%s, %s) g',
            lo, hi
        );
        RAISE NOTICE 'Loaded rows % to %', lo, hi;
        lo := hi + 1;
    END LOOP;
END
\$\$;

ANALYZE bench_kv;
EOF

    SETUP_END=$(date +%s)
    echo "=== Setup complete in $((SETUP_END - SETUP_START))s ==="
    ROW_COUNT=$($PSQL -tAc "SELECT count(*) FROM bench_kv;")
    echo "=== bench_kv: $ROW_COUNT rows ==="
    echo ""
fi

# Map workload name to script file (bash 3 compatible)
script_for_workload() {
  case "$1" in
    select) echo "select.sql" ;;
    update) echo "update.sql" ;;
    insert) echo "insert.sql" ;;
    mixed)  echo "mixed.sql"  ;;
    *)      echo >&2 "Unknown workload: $1"; exit 1 ;;
  esac
}

# Parse space-separated env vars into arrays
IFS=' ' read -ra CLIENTS <<< "${CLIENTS:-1 8 32}"
IFS=' ' read -ra THREADS <<< "${THREADS:-1 8 16}"
IFS=' ' read -ra WORKLOAD_LIST <<< "${WORKLOADS:-select update insert mixed}"

reload_guc() {
  local val="$1"
  for p in $PORT $((PORT+1)) $((PORT+2)); do
    psql -p "$p" -d "$DB" -c "ALTER SYSTEM SET citus.enable_single_task_execution = $val;" >/dev/null 2>&1
    psql -p "$p" -d "$DB" -c "SELECT pg_reload_conf();" >/dev/null 2>&1
  done
  sleep 1
}

reset_guc() {
  for p in $PORT $((PORT+1)) $((PORT+2)); do
    psql -p "$p" -d "$DB" -c "ALTER SYSTEM RESET citus.enable_single_task_execution;" >/dev/null 2>&1
    psql -p "$p" -d "$DB" -c "SELECT pg_reload_conf();" >/dev/null 2>&1
  done
}

echo "=== Benchmark started at $(date) ==="
echo "Port: $PORT | DB: $DB | Duration: ${DURATION}s | Iterations: $ITERATIONS"
echo "Clients: ${CLIENTS[*]} | Threads: ${THREADS[*]}"
echo "Workloads: ${WORKLOAD_LIST[*]}"
echo "Results dir: $RESULTS_DIR"
echo ""

for guc_val in on off; do
  reload_guc "$guc_val"

  # verify
  actual=$(psql -p $PORT -d $DB -tAc "SHOW citus.enable_single_task_execution;")
  echo "=== GUC = $guc_val (verified: $actual) ==="

  for workload in "${WORKLOAD_LIST[@]}"; do
    script="${SCRIPT_DIR}/$(script_for_workload "$workload")"
    for idx in "${!CLIENTS[@]}"; do
      c=${CLIENTS[$idx]}
      j=${THREADS[$idx]}
      tag="${workload}_c${c}_${guc_val}"

      for iter in $(seq 1 $ITERATIONS); do
        outfile="${RESULTS_DIR}/${tag}_iter${iter}.txt"
        echo "$(date +%H:%M:%S) >>> $tag  iter=$iter  (c=$c j=$j T=$DURATION)"

        pgbench -p $PORT -d $DB \
          -f "$script" \
          -c "$c" -j "$j" -T "$DURATION" \
          --no-vacuum \
          2>&1 | tee "$outfile"

        echo ""
      done
    done
  done
done

# restore default
reset_guc

echo ""
echo "=== All runs complete at $(date) ==="
echo "=== Results in $RESULTS_DIR ==="

# --- Collect CSV ---
CSV="$RESULTS_DIR/summary.csv"
echo "workload,clients,guc,iter,tps,lat_avg_ms" > "$CSV"

for f in "$RESULTS_DIR"/*.txt; do
  base=$(basename "$f" .txt)
  workload=$(echo "$base" | cut -d_ -f1)
  clients=$(echo "$base" | sed 's/.*_c\([0-9]*\)_.*/\1/')
  guc=$(echo "$base" | grep -oP '(on|off)(?=_iter)')
  iter=$(echo "$base" | grep -oP 'iter\K[0-9]+')

  tps=$(grep 'tps = ' "$f" | grep -v initial | awk '{print $3}')
  lat=$(grep 'latency average' "$f" | awk '{print $4}')

  echo "$workload,$clients,$guc,$iter,$tps,$lat" >> "$CSV"
done

echo ""
echo "=== Summary CSV: $CSV ==="
cat "$CSV"
