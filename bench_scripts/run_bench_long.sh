#!/usr/bin/env bash
set -euo pipefail

PORT=9700
DB=citus
DURATION=300
ITERATIONS=15
RESULTS_DIR="bench_results/long_run_$(date +%Y%m%d_%H%M%S)"
SCRIPT_DIR="bench_scripts"
mkdir -p "$RESULTS_DIR"

declare -A SCRIPTS=(
  [select]=select.sql
  [update]=update.sql
  [insert]=insert.sql
  [mixed]=mixed.sql
)

CLIENTS=(1 8 32)
THREADS=(1 8 16)

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

total_runs=$((4 * ${#CLIENTS[@]} * 2 * ITERATIONS))
total_secs=$((total_runs * DURATION))
echo "=== Long benchmark started at $(date) ==="
echo "=== ${total_runs} runs × ${DURATION}s = ${total_secs}s (~$((total_secs / 3600))h) ==="
echo "=== Results dir: $RESULTS_DIR ==="
echo ""

run=0
for guc_val in on off; do
  reload_guc "$guc_val"

  actual=$(psql -p $PORT -d $DB -tAc "SHOW citus.enable_single_task_fast_path;")
  echo "=== GUC = $guc_val (verified: $actual) ==="

  for workload in select update insert mixed; do
    script="${SCRIPT_DIR}/${SCRIPTS[$workload]}"
    for idx in "${!CLIENTS[@]}"; do
      c=${CLIENTS[$idx]}
      j=${THREADS[$idx]}
      tag="${workload}_c${c}_${guc_val}"

      for iter in $(seq 1 $ITERATIONS); do
        run=$((run + 1))
        outfile="${RESULTS_DIR}/${tag}_iter${iter}.txt"
        eta_secs=$(( (total_runs - run) * DURATION ))
        eta_h=$((eta_secs / 3600))
        eta_m=$(( (eta_secs % 3600) / 60 ))
        echo "$(date +%H:%M:%S) >>> [$run/$total_runs] $tag iter=$iter (c=$c j=$j T=${DURATION}s) ETA: ${eta_h}h${eta_m}m"

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

reset_guc

echo ""
echo "=== All runs complete at $(date) ==="
echo "=== Results in $RESULTS_DIR ==="

# --- Collect CSV ---
CSV="$RESULTS_DIR/summary.csv"
echo "workload,clients,guc,iter,tps,lat_avg_ms" > "$CSV"

for f in "$RESULTS_DIR"/*.txt; do
  base=$(basename "${f%.txt}")
  workload="${base%%_c*}"
  rest="${base#*_c}"
  clients="${rest%%_*}"
  rest2="${rest#*_}"
  guc="${rest2%%_*}"
  iter="${rest2##*iter}"

  tps=$(grep '^tps' "$f" | awk '{print $3}')
  lat=$(grep 'latency average' "$f" | awk '{print $4}')

  echo "$workload,$clients,$guc,$iter,$tps,$lat" >> "$CSV"
done

echo ""
echo "=== Summary CSV: $CSV ==="
head -5 "$CSV"
echo "... ($(wc -l < "$CSV") total lines)"
