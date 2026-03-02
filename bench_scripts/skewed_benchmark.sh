#!/usr/bin/env bash
#
# Skewed large-table benchmark for batched adaptive executor (PR #5195)
#
# 100M rows, ~500 distinct shard keys, each key has 100K–300K rows.
# This stresses the executor with realistic skew: some shards are much
# larger than others, and individual LIMIT queries may early-terminate
# across batches.
#
# Prerequisites:
#   - Citus cluster running (coordinator on $PORT, default 9700)
#   - Citus extension loaded in $DB (default "citus")
#   - ~10 GB disk for the table + indexes
#
# Usage:
#   ./skewed_benchmark.sh [port] [database]
#

set -euo pipefail

PORT="${1:-9700}"
DB="${2:-citus}"
TOTAL_ROWS=100000000
SHARD_COUNT=32
PAYLOAD_SIZE=50

PSQL="psql -p $PORT -d $DB -X --no-psqlrc"

echo "=== Skewed Large-Table Benchmark (100M rows) ==="
echo "Port: $PORT | DB: $DB | Rows: $TOTAL_ROWS | Shards: $SHARD_COUNT"
echo ""

# --- Setup ---
echo "--- Setup: creating bench_skew table ---"
echo "    ~500 distinct shard keys, 100K–300K rows each"
SETUP_START=$(date +%s)

$PSQL -q <<'EOF'
DROP TABLE IF EXISTS bench_skew;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

CREATE TABLE bench_skew (
    shard_key int NOT NULL,
    id bigint NOT NULL,
    val int,
    payload text
);
SELECT create_distributed_table('bench_skew', 'shard_key');

-- Generate ~500 distinct shard_key values, each with 100K–300K rows.
-- We use a two-step approach:
--   1. Build a key table with per-key row counts drawn uniformly from [100000,300000]
--   2. Cross-join with generate_series to produce the rows
--
-- This avoids holding 100M rows in memory at once by inserting in chunks.

DO $$
DECLARE
    num_keys int := 500;
    min_rows_per_key int := 100000;
    max_rows_per_key int := 300000;
    k int;
    rows_for_key int;
    running_id bigint := 0;
    batch_start bigint;
BEGIN
    -- Disable timing/notices for bulk insert
    SET client_min_messages TO WARNING;
    SET citus.multi_shard_modify_mode TO 'sequential';

    FOR k IN 1..num_keys LOOP
        -- Deterministic but varying count per key: 100K + hash-derived offset
        rows_for_key := min_rows_per_key +
            ((hashint4(k) & x'7fffffff'::int) % (max_rows_per_key - min_rows_per_key + 1));
        batch_start := running_id + 1;
        running_id := running_id + rows_for_key;

        INSERT INTO bench_skew (shard_key, id, val, payload)
        SELECT k,
               batch_start + g - 1,
               (random() * 1000000)::int,
               repeat('x', 50)
        FROM generate_series(1, rows_for_key) g;

        IF k % 50 = 0 THEN
            RAISE NOTICE 'Inserted % keys so far, running_id = %', k, running_id;
        END IF;
    END LOOP;

    RAISE NOTICE 'Total rows inserted: %', running_id;
END $$;
EOF

SETUP_END=$(date +%s)
echo "Setup took $((SETUP_END - SETUP_START)) seconds."

ROW_COUNT=$($PSQL -t -A -c "SELECT count(*) FROM bench_skew;")
KEY_COUNT=$($PSQL -t -A -c "SELECT count(DISTINCT shard_key) FROM bench_skew;")
echo "Actual rows: $ROW_COUNT | Distinct keys: $KEY_COUNT"
echo ""

# --- Test 1: Full scan throughput at different batch sizes ---
echo "--- Test 1: Full table scan throughput (COPY to /dev/null) ---"
echo ""

for BATCH in 0 1000 10000 100000; do
    LABEL="$BATCH"
    if [ "$BATCH" -eq 0 ]; then LABEL="off (run-to-completion)"; fi

    TIMES=""
    for RUN in 1 2 3; do
        T=$($PSQL -c "\\timing on" \
            -c "SET citus.executor_batch_size TO $BATCH;" \
            -c "COPY (SELECT * FROM bench_skew) TO '/dev/null';" 2>&1 \
            | grep "^Time:" | head -1 | awk '{print $2}')
        TIMES="$TIMES ${T}"
    done
    printf "  batch_size=%-20s => %s ms\n" "$LABEL" "$TIMES"
done
echo ""

# --- Test 2: Single-key scan (100K–300K rows, one shard hit) ---
echo "--- Test 2: Single shard-key scan (point query, all rows for one key) ---"
echo ""

SAMPLE_KEY=$($PSQL -t -A -c "SELECT shard_key FROM bench_skew LIMIT 1;")
KEY_ROWS=$($PSQL -t -A -c "SELECT count(*) FROM bench_skew WHERE shard_key = $SAMPLE_KEY;")
echo "  shard_key=$SAMPLE_KEY has $KEY_ROWS rows"
echo ""

for BATCH in 0 1000 10000 100000; do
    LABEL="$BATCH"
    if [ "$BATCH" -eq 0 ]; then LABEL="off"; fi

    TIMES=""
    for RUN in 1 2 3; do
        T=$($PSQL -c "\\timing on" \
            -c "SET citus.executor_batch_size TO $BATCH;" \
            -c "COPY (SELECT * FROM bench_skew WHERE shard_key = $SAMPLE_KEY) TO '/dev/null';" 2>&1 \
            | grep "^Time:" | head -1 | awk '{print $2}')
        TIMES="$TIMES ${T}"
    done
    printf "  batch_size=%-5s => %s ms\n" "$LABEL" "$TIMES"
done
echo ""

# --- Test 3: LIMIT early termination (streaming, no ORDER BY) ---
echo "--- Test 3: LIMIT without ORDER BY (early termination opportunity) ---"
echo ""

for LIM in 100 10000 1000000; do
    echo "  LIMIT $LIM:"
    for BATCH in 0 1000 10000 100000; do
        LABEL="$BATCH"
        if [ "$BATCH" -eq 0 ]; then LABEL="off"; fi

        TIMES=""
        for RUN in 1 2 3; do
            T=$($PSQL -c "\\timing on" \
                -c "SET citus.executor_batch_size TO $BATCH;" \
                -c "SELECT val FROM bench_skew LIMIT $LIM;" 2>&1 \
                | grep "^Time:" | head -1 | awk '{print $2}')
            TIMES="$TIMES ${T}"
        done
        printf "    batch_size=%-5s => %s ms\n" "$LABEL" "$TIMES"
    done
    echo ""
done

# --- Test 4: LIMIT with ORDER BY (blocking Sort, no early termination) ---
echo "--- Test 4: LIMIT with ORDER BY (Sort blocks early termination) ---"
echo ""

for LIM in 100 10000; do
    echo "  LIMIT $LIM ORDER BY val:"
    for BATCH in 0 10000 100000; do
        LABEL="$BATCH"
        if [ "$BATCH" -eq 0 ]; then LABEL="off"; fi

        T=$($PSQL -c "\\timing on" \
            -c "SET citus.executor_batch_size TO $BATCH;" \
            -c "SELECT val FROM bench_skew ORDER BY val LIMIT $LIM;" 2>&1 \
            | grep "^Time:" | head -1 | awk '{print $2}')
        printf "    batch_size=%-5s => %s ms\n" "$LABEL" "$T"
    done
    echo ""
done

# --- Test 5: Memory profile (cursor-based, measure across batches) ---
echo "--- Test 5: Memory profile across batches (cursor over full table) ---"
echo ""

for BATCH in 1000 10000 100000; do
    echo "  batch_size=$BATCH:"
    $PSQL -t -A <<EOF | grep -v '^\s*$' | grep '^After' | while IFS= read -r LINE; do echo "    $LINE"; done
SET citus.executor_batch_size TO $BATCH;
BEGIN;
DECLARE c NO SCROLL CURSOR FOR SELECT * FROM bench_skew;

FETCH 1 FROM c;
SELECT 'After batch 1: total=' || total_bytes || ' used=' || used_bytes
FROM pg_backend_memory_contexts WHERE name = 'AdaptiveExecutor';

MOVE FORWARD $((BATCH * 10 - 1)) FROM c;
SELECT 'After ~10 batches: total=' || total_bytes || ' used=' || used_bytes
FROM pg_backend_memory_contexts WHERE name = 'AdaptiveExecutor';

MOVE FORWARD $((BATCH * 90)) FROM c;
SELECT 'After ~100 batches: total=' || total_bytes || ' used=' || used_bytes
FROM pg_backend_memory_contexts WHERE name = 'AdaptiveExecutor';

CLOSE c;
END;
EOF
    echo ""
done

# --- Test 6: Aggregation throughput ---
echo "--- Test 6: Aggregation (count/sum per shard_key) ---"
echo ""

for BATCH in 0 10000 100000; do
    LABEL="$BATCH"
    if [ "$BATCH" -eq 0 ]; then LABEL="off"; fi

    TIMES=""
    for RUN in 1 2 3; do
        T=$($PSQL -c "\\timing on" \
            -c "SET citus.executor_batch_size TO $BATCH;" \
            -c "SELECT shard_key, count(*), sum(val) FROM bench_skew GROUP BY shard_key;" 2>&1 \
            | grep "^Time:" | head -1 | awk '{print $2}')
        TIMES="$TIMES ${T}"
    done
    printf "  batch_size=%-5s => %s ms\n" "$LABEL" "$TIMES"
done
echo ""

# --- Cleanup ---
echo "--- Cleanup ---"
read -p "Drop bench_skew table? [y/N] " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    $PSQL -q -c "DROP TABLE IF EXISTS bench_skew;"
    echo "Dropped."
else
    echo "Kept bench_skew for further analysis."
fi
