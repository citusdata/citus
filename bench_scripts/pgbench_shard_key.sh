#!/usr/bin/env bash
#
# pgbench benchmark for batched adaptive executor (PR #5195)
#
# Creates the bench_skew table (~100M rows, ~500 distinct shard keys,
# each key has 100K–300K rows), then runs a point-query workload
# (SELECT by random shard_key) via pgbench at various batch sizes
# and client counts.
#
# Prerequisites:
#   - Citus cluster running (coordinator on $PORT, default 9700)
#   - pgbench on $PATH
#   - Enough disk/memory for ~100M rows
#
# Usage:
#   ./pgbench_shard_key.sh [--main] [port] [database]
#
# Options:
#   --main      — Run against Citus main branch (no batch size GUC, just
#                 iterate over client counts)
#
# Environment overrides:
#   CLIENTS     — space-separated client counts   (default: "1 4 8")
#   BATCH_SIZES — space-separated batch sizes      (default: "0 1000 10000 100000")
#   DURATION    — pgbench run duration in seconds  (default: 60)
#   SKIP_LOAD   — set to 1 to skip table creation  (default: 0)
#

set -euo pipefail

MAIN_MODE=0
if [ "${1:-}" = "--main" ]; then
    MAIN_MODE=1
    shift
fi

PORT="${1:-9700}"
DB="${2:-citus}"

CLIENTS="${CLIENTS:-1 4 8}"
BATCH_SIZES="${BATCH_SIZES:-0 1000 10000 100000}"
DURATION="${DURATION:-60}"
SKIP_LOAD="${SKIP_LOAD:-0}"

NUM_KEYS=500
MIN_ROWS=100000
MAX_ROWS=300000
SHARD_COUNT=32

PSQL="psql -p $PORT -d $DB -X --no-psqlrc"
PGBENCH="pgbench -p $PORT -d $DB"

RESULTS_DIR="bench_results/pgbench_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

echo "=== pgbench Shard-Key Point-Query Benchmark ==="
if [ "$MAIN_MODE" -eq 1 ]; then
    echo "Mode: main (no batch size GUC)"
else
    echo "Mode: batched adaptive executor"
fi
echo "Port: $PORT | DB: $DB"
echo "Clients: $CLIENTS"
if [ "$MAIN_MODE" -eq 0 ]; then
    echo "Batch sizes: $BATCH_SIZES"
fi
echo "Duration: ${DURATION}s per run"
echo "Results: $RESULTS_DIR"
echo ""

# ---------------------------------------------------------------
# Setup: create and load bench_skew (unless SKIP_LOAD=1)
# ---------------------------------------------------------------
if [ "$SKIP_LOAD" -ne 1 ]; then
    echo "--- Setup: creating bench_skew table (~100M rows) ---"
    SETUP_START=$(date +%s)

    $PSQL -q <<EOF
DROP TABLE IF EXISTS bench_skew;
SET citus.shard_count TO $SHARD_COUNT;
SET citus.shard_replication_factor TO 1;

CREATE TABLE bench_skew (
    shard_key int NOT NULL,
    id bigint NOT NULL,
    val int,
    payload text
);
SELECT create_distributed_table('bench_skew', 'shard_key');

DO \$\$
DECLARE
    k int;
    rows_for_key int;
    running_id bigint := 0;
    batch_start bigint;
BEGIN
    SET client_min_messages TO WARNING;
    SET citus.multi_shard_modify_mode TO 'sequential';

    FOR k IN 1..$NUM_KEYS LOOP
        rows_for_key := $MIN_ROWS +
            ((hashint4(k) & x'7fffffff'::int) % ($MAX_ROWS - $MIN_ROWS + 1));
        batch_start := running_id + 1;
        running_id := running_id + rows_for_key;

        INSERT INTO bench_skew (shard_key, id, val, payload)
        SELECT k,
               batch_start + g - 1,
               (random() * 1000000)::int,
               repeat('x', 50)
        FROM generate_series(1, rows_for_key) g;

        IF k % 50 = 0 THEN
            RAISE NOTICE 'Inserted % keys, running_id = %', k, running_id;
        END IF;
    END LOOP;

    RAISE NOTICE 'Total rows inserted: %', running_id;
END \$\$;

ANALYZE bench_skew;
EOF

    SETUP_END=$(date +%s)
    echo "Setup took $((SETUP_END - SETUP_START)) seconds."
else
    echo "--- Skipping table setup (SKIP_LOAD=1) ---"
fi

ROW_COUNT=$($PSQL -t -A -c "SELECT count(*) FROM bench_skew;")
KEY_COUNT=$($PSQL -t -A -c "SELECT count(DISTINCT shard_key) FROM bench_skew;")
echo "Rows: $ROW_COUNT | Distinct shard_keys: $KEY_COUNT"
echo ""

# ---------------------------------------------------------------
# Run workloads
# ---------------------------------------------------------------
echo "=== Running workloads ==="
echo ""

# Header for summary
SUMMARY_FILE="$RESULTS_DIR/summary.txt"
if [ "$MAIN_MODE" -eq 1 ]; then
    printf "%-12s %-8s %-10s %-10s %-12s\n" "mode" "clients" "tps" "avg_ms" "stddev_ms" > "$SUMMARY_FILE"
    printf "%-12s %-8s %-10s %-10s %-12s\n" "----------" "-------" "--------" "--------" "----------" >> "$SUMMARY_FILE"
else
    printf "%-12s %-8s %-10s %-10s %-12s\n" "batch_size" "clients" "tps" "avg_ms" "stddev_ms" > "$SUMMARY_FILE"
    printf "%-12s %-8s %-10s %-10s %-12s\n" "----------" "-------" "--------" "--------" "----------" >> "$SUMMARY_FILE"
fi

run_pgbench() {
    local TAG="$1"
    local LABEL="$2"
    local SCRIPT="$3"
    local C="$4"

    LOG_FILE="$RESULTS_DIR/${TAG}.log"

    echo "  ${LABEL}  clients=$C  (${DURATION}s) ..."

    $PGBENCH \
        -c "$C" \
        -j "$C" \
        -T "$DURATION" \
        -f "$SCRIPT" \
        --no-vacuum \
        -r \
        > "$LOG_FILE" 2>&1 || true

    # Extract metrics from pgbench output (|| true to avoid set -e on no match)
    TPS=$(grep 'tps = ' "$LOG_FILE" | grep -v 'including' | head -1 | awk '{print $3}' || true)
    AVG_MS=$(grep 'latency average' "$LOG_FILE" | awk '{print $4}' || true)
    STDDEV_MS=$(grep 'latency stddev' "$LOG_FILE" | awk '{print $4}' || true)

    TPS="${TPS:-N/A}"
    AVG_MS="${AVG_MS:-N/A}"
    STDDEV_MS="${STDDEV_MS:-N/A}"

    printf "    tps=%s  avg_latency=%s ms  stddev=%s ms\n" "$TPS" "$AVG_MS" "$STDDEV_MS"
    printf "%-12s %-8s %-10s %-10s %-12s\n" "$LABEL" "$C" "$TPS" "$AVG_MS" "$STDDEV_MS" >> "$SUMMARY_FILE"
}

if [ "$MAIN_MODE" -eq 1 ]; then
    # Main mode: no batch size GUC, just iterate over client counts
    MAIN_SCRIPT="$RESULTS_DIR/query_main.sql"
    cat > "$MAIN_SCRIPT" <<PGBENCH_SQL
\set shard_key random(1, $NUM_KEYS)
SELECT id, val, payload FROM bench_skew WHERE shard_key = :shard_key;
PGBENCH_SQL

    for C in $CLIENTS; do
        run_pgbench "main_c${C}" "main" "$MAIN_SCRIPT" "$C"
    done
    echo ""
else
    # Batched mode: iterate batch sizes × client counts
    for BATCH in $BATCH_SIZES; do
        BATCH_LABEL="$BATCH"
        if [ "$BATCH" -eq 0 ]; then
            BATCH_LABEL="off"
        fi

        BATCH_SCRIPT="$RESULTS_DIR/query_batch${BATCH}.sql"
        cat > "$BATCH_SCRIPT" <<PGBENCH_SQL
SET citus.executor_batch_size TO $BATCH;
\set shard_key random(1, $NUM_KEYS)
SELECT id, val, payload FROM bench_skew WHERE shard_key = :shard_key;
PGBENCH_SQL

        for C in $CLIENTS; do
            run_pgbench "batch${BATCH}_c${C}" "$BATCH_LABEL" "$BATCH_SCRIPT" "$C"
        done
        echo ""
    done
fi

echo ""
echo "=== Summary ==="
cat "$SUMMARY_FILE"
echo ""
echo "Full logs: $RESULTS_DIR/"
echo "Done."
