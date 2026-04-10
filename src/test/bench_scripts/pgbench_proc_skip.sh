#!/usr/bin/env bash
#
# pgbench_proc_skip.sh — Benchmark for citus.enable_procedure_transaction_skip
#
# Compares single-shard procedure workloads (INSERT, UPDATE, SELECT, MIXED)
# with the transaction-skip optimization ON vs OFF.
#
# This optimization allows single-statement procedures that execute a
# single task on a single shard to skip coordinated (2PC) transactions.
#
# Prerequisites:
#   - Citus cluster running (coordinator on PORT, default 9700)
#   - pgbench on PATH
#
# Usage:
#   ./pgbench_proc_skip.sh [port] [database]
#
# Environment overrides:
#   CLIENTS     — space-separated client counts   (default: "1 4 8")
#   DURATION    — pgbench run duration in seconds  (default: 60)
#   SKIP_LOAD   — set to 1 to skip table creation  (default: 0)
#   ITERATIONS  — number of times to repeat        (default: 1)
#   WORKLOADS   — space-separated list             (default: "insert update select mixed")
#   NUM_KEYS    — number of shard keys to seed     (default: 100000)
#   SHARD_COUNT — number of shards                 (default: 32)
#
# Additional pgbench/libpq options can be passed via PGOPTIONS, e.g.:
#   PGOPTIONS="-c citus.enable_local_execution=off" ./pgbench_proc_skip.sh
#

set -euo pipefail

PORT="${1:-9700}"
DB="${2:-citus}"

CLIENTS="${CLIENTS:-1 4 8}"
DURATION="${DURATION:-60}"
SKIP_LOAD="${SKIP_LOAD:-0}"
ITERATIONS="${ITERATIONS:-1}"
WORKLOADS="${WORKLOADS:-insert update select mixed}"
NUM_KEYS="${NUM_KEYS:-100000}"
SHARD_COUNT="${SHARD_COUNT:-32}"

PSQL="psql -p $PORT -d $DB -X --no-psqlrc"

RESULTS_DIR="bench_results/proc_skip_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

echo "=== pgbench Procedure Transaction-Skip Benchmark ==="
echo "Port: $PORT | DB: $DB"
echo "Clients: $CLIENTS"
echo "Workloads: $WORKLOADS"
echo "Duration: ${DURATION}s per run"
echo "Iterations: $ITERATIONS"
echo "Keys: $NUM_KEYS | Shards: $SHARD_COUNT"
echo "Results: $RESULTS_DIR"
echo ""

# ---------------------------------------------------------------
# Setup: create table, procedures, and seed data
# ---------------------------------------------------------------
if [ "$SKIP_LOAD" -ne 1 ]; then
    echo "--- Setup: creating bench_procs table and procedures ---"
    SETUP_START=$(date +%s)

    $PSQL -q <<EOF
DROP TABLE IF EXISTS bench_procs CASCADE;
SET citus.shard_count TO $SHARD_COUNT;
SET citus.shard_replication_factor TO 1;

CREATE TABLE bench_procs (
    dkey int PRIMARY KEY,
    payload text,
    val numeric
);
SELECT create_distributed_table('bench_procs', 'dkey');

-- Seed table
INSERT INTO bench_procs
SELECT g, md5(g::text), (random() * 19999)::numeric
FROM generate_series(1, $NUM_KEYS) g;

ANALYZE bench_procs;

-- INSERT procedure (ON CONFLICT DO NOTHING for idempotency)
CREATE OR REPLACE PROCEDURE proc_insert(p_dkey int, p_payload text, p_val numeric)
LANGUAGE plpgsql AS \$\$
BEGIN
    INSERT INTO bench_procs VALUES (p_dkey, p_payload, p_val)
    ON CONFLICT DO NOTHING;
END;
\$\$;

-- UPDATE procedure (point update by shard key)
CREATE OR REPLACE PROCEDURE proc_update(p_dkey int, p_val numeric)
LANGUAGE plpgsql AS \$\$
BEGIN
    UPDATE bench_procs SET val = p_val WHERE dkey = p_dkey;
END;
\$\$;

-- SELECT procedure (point read by shard key)
CREATE OR REPLACE PROCEDURE proc_select(p_dkey int)
LANGUAGE plpgsql AS \$\$
DECLARE _payload text; _val numeric;
BEGIN
    SELECT payload, val INTO _payload, _val
    FROM bench_procs WHERE dkey = p_dkey;
END;
\$\$;

-- DELETE procedure (point delete by shard key)
CREATE OR REPLACE PROCEDURE proc_delete(p_dkey int)
LANGUAGE plpgsql AS \$\$
BEGIN
    DELETE FROM bench_procs WHERE dkey = p_dkey;
END;
\$\$;
EOF

    SETUP_END=$(date +%s)
    echo "Setup took $((SETUP_END - SETUP_START)) seconds."
else
    echo "--- Skipping table setup (SKIP_LOAD=1) ---"
fi

ROW_COUNT=$($PSQL -t -A -c "SELECT count(*) FROM bench_procs;")
echo "Rows: $ROW_COUNT"
echo ""

# ---------------------------------------------------------------
# Reseed helper — called before each (workload, mode) pair
# to ensure consistent starting state.
# ---------------------------------------------------------------
reseed_table() {
    $PSQL -q <<EOF
TRUNCATE bench_procs;
INSERT INTO bench_procs
SELECT g, md5(g::text), (random() * 19999)::numeric
FROM generate_series(1, $NUM_KEYS) g;
ANALYZE bench_procs;
EOF
}

# ---------------------------------------------------------------
# Generate pgbench scripts
# ---------------------------------------------------------------
cat > "$RESULTS_DIR/insert.sql" <<PGBENCH_SQL
\set dkey random(1, $NUM_KEYS)
\set val random(1, 19999)
CALL proc_insert(:dkey, md5(random()::text), :val);
PGBENCH_SQL

cat > "$RESULTS_DIR/update.sql" <<PGBENCH_SQL
\set dkey random(1, $NUM_KEYS)
\set val random(1, 19999)
CALL proc_update(:dkey, :val);
PGBENCH_SQL

cat > "$RESULTS_DIR/select.sql" <<PGBENCH_SQL
\set dkey random(1, $NUM_KEYS)
CALL proc_select(:dkey);
PGBENCH_SQL

cat > "$RESULTS_DIR/delete.sql" <<PGBENCH_SQL
\set dkey random(1, $NUM_KEYS)
CALL proc_delete(:dkey);
PGBENCH_SQL

# ---------------------------------------------------------------
# Run workloads
# ---------------------------------------------------------------
echo "=== Running workloads ==="
echo ""

SUMMARY_FILE="$RESULTS_DIR/summary.txt"
printf "%-10s %-8s %-5s %-8s %-10s %-10s %-12s\n" \
    "workload" "skip" "iter" "clients" "tps" "avg_ms" "stddev_ms" \
    > "$SUMMARY_FILE"
printf "%-10s %-8s %-5s %-8s %-10s %-10s %-12s\n" \
    "--------" "------" "----" "-------" "--------" "--------" "----------" \
    >> "$SUMMARY_FILE"

run_pgbench() {
    local TAG="$1"
    local WORKLOAD="$2"
    local MODE="$3"
    local C="$4"
    local ITER="$5"
    shift 5
    local FILES=("$@")

    local LOG_FILE="$RESULTS_DIR/${TAG}.log"

    echo "  ${WORKLOAD}  skip=${MODE}  clients=${C}  iter=${ITER}  (${DURATION}s) ..."

    local CMD=(pgbench -p "$PORT" -d "$DB"
               -c "$C" -j "$C" -T "$DURATION"
               --no-vacuum -r)
    for f in "${FILES[@]}"; do
        CMD+=(-f "$f")
    done

    PGOPTIONS="${PGOPTIONS:-} -c citus.enable_procedure_transaction_skip=${MODE}" \
        "${CMD[@]}" > "$LOG_FILE" 2>&1 || true

    # Extract metrics from pgbench output
    local TPS AVG_MS STDDEV_MS
    TPS=$(grep 'tps = ' "$LOG_FILE" | grep -v 'including' | head -1 \
          | awk '{print $3}' || true)
    AVG_MS=$(grep 'latency average' "$LOG_FILE" \
          | awk '{print $4}' || true)
    STDDEV_MS=$(grep 'latency stddev' "$LOG_FILE" \
          | awk '{print $4}' || true)

    TPS="${TPS:-N/A}"
    AVG_MS="${AVG_MS:-N/A}"
    STDDEV_MS="${STDDEV_MS:-N/A}"

    printf "    tps=%s  avg_latency=%s ms  stddev=%s ms\n" \
        "$TPS" "$AVG_MS" "$STDDEV_MS"
    printf "%-10s %-8s %-5s %-8s %-10s %-10s %-12s\n" \
        "$WORKLOAD" "$MODE" "$ITER" "$C" "$TPS" "$AVG_MS" "$STDDEV_MS" \
        >> "$SUMMARY_FILE"
}

for ITER in $(seq 1 "$ITERATIONS"); do
    if [ "$ITERATIONS" -gt 1 ]; then
        echo "--- Iteration $ITER of $ITERATIONS ---"
    fi

    for WORKLOAD in $WORKLOADS; do
        echo "--- Workload: $WORKLOAD ---"

        for MODE in on off; do
            echo "  Reseeding table ($NUM_KEYS rows) ..."
            reseed_table

            for C in $CLIENTS; do
                case "$WORKLOAD" in
                    insert)
                        run_pgbench "${WORKLOAD}_${MODE}_c${C}_iter${ITER}" \
                            "$WORKLOAD" "$MODE" "$C" "$ITER" \
                            "$RESULTS_DIR/insert.sql"
                        ;;
                    update)
                        run_pgbench "${WORKLOAD}_${MODE}_c${C}_iter${ITER}" \
                            "$WORKLOAD" "$MODE" "$C" "$ITER" \
                            "$RESULTS_DIR/update.sql"
                        ;;
                    select)
                        run_pgbench "${WORKLOAD}_${MODE}_c${C}_iter${ITER}" \
                            "$WORKLOAD" "$MODE" "$C" "$ITER" \
                            "$RESULTS_DIR/select.sql"
                        ;;
                    delete)
                        run_pgbench "${WORKLOAD}_${MODE}_c${C}_iter${ITER}" \
                            "$WORKLOAD" "$MODE" "$C" "$ITER" \
                            "$RESULTS_DIR/delete.sql"
                        ;;
                    mixed)
                        # 20% insert, 30% update, 40% select, 10% delete
                        run_pgbench "${WORKLOAD}_${MODE}_c${C}_iter${ITER}" \
                            "$WORKLOAD" "$MODE" "$C" "$ITER" \
                            "$RESULTS_DIR/insert.sql@2" \
                            "$RESULTS_DIR/update.sql@3" \
                            "$RESULTS_DIR/select.sql@4" \
                            "$RESULTS_DIR/delete.sql@1"
                        ;;
                    *)
                        echo "  Unknown workload: $WORKLOAD — skipping"
                        ;;
                esac
            done
        done
        echo ""
    done
done

echo ""
echo "=== Summary ==="
cat "$SUMMARY_FILE"
echo ""
echo "Full logs: $RESULTS_DIR/"
echo "Done."
