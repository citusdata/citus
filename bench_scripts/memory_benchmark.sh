#!/usr/bin/env bash
#
# Memory benchmark for batched adaptive executor (PR #5195)
#
# Measures AdaptiveExecutor memory context size at different batch sizes
# and checks for per-batch memory leaks across many batches.
#
# Prerequisites:
#   - Citus cluster running (coordinator on $PORT, default 9700)
#   - Citus extension loaded in $DB (default "citus")
#
# Usage:
#   ./memory_benchmark.sh [port] [database]
#

set -euo pipefail

PORT="${1:-9700}"
DB="${2:-citus}"
ROWS=300000
PAYLOAD_SIZE=100

PSQL="psql -p $PORT -d $DB -X --no-psqlrc"

echo "=== Batched Adaptive Executor Memory Benchmark ==="
echo "Port: $PORT | DB: $DB | Rows: $ROWS | Payload: ${PAYLOAD_SIZE}B"
echo ""

# --- Setup ---
echo "--- Setup: creating bench_mem table with $ROWS rows ---"
$PSQL -q <<EOF
DROP TABLE IF EXISTS bench_mem;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
CREATE TABLE bench_mem (id int, payload text);
SELECT create_distributed_table('bench_mem', 'id');
INSERT INTO bench_mem SELECT i, repeat('x', $PAYLOAD_SIZE) FROM generate_series(1, $ROWS) i;
EOF
echo "Done."
echo ""

# --- Test 1: Memory at different batch sizes ---
echo "--- Test 1: AdaptiveExecutor memory at different batch sizes ---"
echo "(Measured after FETCH 1 from a cursor over all $ROWS rows)"
echo ""

for BATCH in 1000 10000 100000 1000000; do
    RESULT=$($PSQL -t -A <<EOF
SET citus.executor_batch_size TO $BATCH;
BEGIN;
DECLARE c NO SCROLL CURSOR FOR SELECT id, payload FROM bench_mem ORDER BY id;
FETCH 1 FROM c;
SELECT name || '|' || total_bytes || '|' || used_bytes
FROM pg_backend_memory_contexts
WHERE name = 'AdaptiveExecutor';
CLOSE c;
END;
EOF
)
    # Extract the memory line (skip the FETCH row)
    MEM_LINE=$(echo "$RESULT" | grep 'AdaptiveExecutor')
    TOTAL=$(echo "$MEM_LINE" | cut -d'|' -f2)
    USED=$(echo "$MEM_LINE" | cut -d'|' -f3)
    printf "  batch_size=%7d => total=%6d bytes, used=%6d bytes\n" "$BATCH" "$TOTAL" "$USED"
done
echo ""

# --- Test 2: Large memory contexts comparison ---
echo "--- Test 2: All contexts > 50kB (batch=1000 vs batch=1000000) ---"
echo ""

for BATCH in 1000 1000000; do
    echo "  batch_size=$BATCH:"
    $PSQL -t -A <<EOF | grep -v '^\s*$' | while IFS='|' read -r NAME TOTAL USED; do
SET citus.executor_batch_size TO $BATCH;
BEGIN;
DECLARE c NO SCROLL CURSOR FOR SELECT id, payload FROM bench_mem ORDER BY id;
FETCH 1 FROM c;
SELECT name || '|' || total_bytes || '|' || used_bytes
FROM pg_backend_memory_contexts
WHERE total_bytes > 50000
ORDER BY total_bytes DESC LIMIT 10;
CLOSE c;
END;
EOF
        # Skip the FETCH data row
        if [[ "$NAME" =~ ^[0-9] ]]; then continue; fi
        printf "    %-25s total=%8d  used=%8d\n" "$NAME" "$TOTAL" "$USED"
    done
    echo ""
done

# --- Test 3: Per-batch memory leak detection ---
echo "--- Test 3: AdaptiveExecutor memory across batches (leak detection) ---"
echo "(batch_size=1000, measuring at 1, 5, 50, 200 batches consumed)"
echo ""

$PSQL -t -A <<EOF | grep -v '^\s*$' | grep -v '^[0-9]' | while IFS= read -r LINE; do echo "  $LINE"; done
SET citus.executor_batch_size TO 1000;
BEGIN;
DECLARE c NO SCROLL CURSOR FOR SELECT id, payload FROM bench_mem ORDER BY id;

FETCH 1 FROM c;
SELECT 'After batch   1: total=' || total_bytes || ' used=' || used_bytes
FROM pg_backend_memory_contexts WHERE name = 'AdaptiveExecutor';

MOVE FORWARD 4999 FROM c;
SELECT 'After batch   5: total=' || total_bytes || ' used=' || used_bytes
FROM pg_backend_memory_contexts WHERE name = 'AdaptiveExecutor';

MOVE FORWARD 45000 FROM c;
SELECT 'After batch  50: total=' || total_bytes || ' used=' || used_bytes
FROM pg_backend_memory_contexts WHERE name = 'AdaptiveExecutor';

MOVE FORWARD 150000 FROM c;
SELECT 'After batch 200: total=' || total_bytes || ' used=' || used_bytes
FROM pg_backend_memory_contexts WHERE name = 'AdaptiveExecutor';

CLOSE c;
END;
EOF
echo ""

# --- Test 4: Throughput at different batch sizes ---
echo "--- Test 4: Throughput (COPY to /dev/null, 3 runs each) ---"
echo ""

for BATCH in 1000 10000 100000 1000000; do
    TIMES=""
    for RUN in 1 2 3; do
        T=$($PSQL -c "\\timing on" -c "SET citus.executor_batch_size TO $BATCH;" \
            -c "COPY (SELECT id, payload FROM bench_mem ORDER BY id) TO '/dev/null';" 2>&1 \
            | grep "^Time:" | head -1 | awk '{print $2}')
        TIMES="$TIMES $T"
    done
    printf "  batch_size=%7d => %s ms\n" "$BATCH" "$TIMES"
done
echo ""

# --- Cleanup ---
echo "--- Cleanup ---"
$PSQL -q -c "DROP TABLE IF EXISTS bench_mem;"
echo "Done."
