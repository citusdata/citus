#!/usr/bin/env bash
set -euo pipefail

PORT=9700
DB=citus
DURATION=120
ITERATIONS=5
RESULTS_DIR="bench_results/c32_rerun_$(date +%Y%m%d_%H%M%S)"
SCRIPT_DIR="bench_scripts"
mkdir -p "$RESULTS_DIR"

declare -A SCRIPTS=(
  [select]=select.sql
  [update]=update.sql
  [insert]=insert.sql
  [mixed]=mixed.sql
)

CLIENTS=32
THREADS=16

reload_guc() {
  local val="$1"
  for p in 9700 9701 9702; do
    psql -p "$p" -d "$DB" -c "ALTER SYSTEM SET citus.enable_single_task_fast_path = $val;" >/dev/null 2>&1
    psql -p "$p" -d "$DB" -c "SELECT pg_reload_conf();" >/dev/null 2>&1
  done
  sleep 1
}

reset_guc() {
  for p in 9700 9701 9702; do
    psql -p "$p" -d "$DB" -c "ALTER SYSTEM RESET citus.enable_single_task_fast_path;" >/dev/null 2>&1
    psql -p "$p" -d "$DB" -c "SELECT pg_reload_conf();" >/dev/null 2>&1
  done
}

echo "=== c=32 Re-run started at $(date) ==="
echo "=== DURATION=${DURATION}s  ITERATIONS=${ITERATIONS}  THREADS=${THREADS} ==="
echo "=== Results dir: $RESULTS_DIR ==="
echo ""

for guc_val in on off; do
  reload_guc "$guc_val"
  actual=$(psql -p $PORT -d $DB -tAc "SHOW citus.enable_single_task_fast_path;")
  echo "=== GUC = $guc_val (verified: $actual) ==="

  for workload in select update insert mixed; do
    script="${SCRIPT_DIR}/${SCRIPTS[$workload]}"
    tag="${workload}_c${CLIENTS}_${guc_val}"

    for iter in $(seq 1 $ITERATIONS); do
      outfile="${RESULTS_DIR}/${tag}_iter${iter}.txt"
      echo "$(date +%H:%M:%S) >>> $tag  iter=$iter  (c=$CLIENTS j=$THREADS T=$DURATION)"

      pgbench -p $PORT -d $DB \
        -f "$script" \
        -c "$CLIENTS" -j "$THREADS" -T "$DURATION" \
        --no-vacuum \
        2>&1 | tee "$outfile"

      echo ""
    done
  done
done

reset_guc

echo ""
echo "=== All c=32 re-runs complete at $(date) ==="

# --- Collect CSV ---
CSV="$RESULTS_DIR/summary.csv"
echo "workload,clients,guc,iter,tps,lat_avg_ms" > "$CSV"

for f in "$RESULTS_DIR"/*.txt; do
  base="${f%.txt}"
  base="$(basename "$base")"
  workload="${base%%_c*}"
  rest="${base#*_c}"
  clients="${rest%%_*}"
  rest2="${rest#*_}"
  guc="${rest2%%_*}"
  iter="${rest2##*iter}"

  tps=$(grep '^tps' "$f" | awk '{print $3}')
  lat=$(grep 'latency average' "$f" | awk '{print $4}')

  echo "$workload,$clients,$guc,$iter,$tps,$lat"
done | sort -t, -k1,1 -k2,2n -k3,3 -k4,4n >> "$CSV"

echo ""
echo "=== Summary CSV ==="
cat "$CSV"
