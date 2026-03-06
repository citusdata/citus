#!/bin/bash
# Clean A/B comparison of fast path ON vs OFF
PORT=9700
DB=citus
DURATION=10
ITERS=5

echo "=== FAST PATH OFF ==="
psql -p $PORT -d $DB -tA -c "ALTER SYSTEM SET citus.enable_single_task_fast_path = off;"
psql -p $PORT -d $DB -tA -c "SELECT pg_reload_conf();"
sleep 1
for i in $(seq 1 $ITERS); do
    pgbench -p $PORT -d $DB -f bench_scripts/select.sql -c 1 -j 1 -T $DURATION --no-vacuum 2>&1 | grep "tps ="
done

echo ""
echo "=== FAST PATH ON ==="
psql -p $PORT -d $DB -tA -c "ALTER SYSTEM SET citus.enable_single_task_fast_path = on;"
psql -p $PORT -d $DB -tA -c "SELECT pg_reload_conf();"
sleep 1
for i in $(seq 1 $ITERS); do
    pgbench -p $PORT -d $DB -f bench_scripts/select.sql -c 1 -j 1 -T $DURATION --no-vacuum 2>&1 | grep "tps ="
done
