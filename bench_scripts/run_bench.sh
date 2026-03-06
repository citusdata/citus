#!/usr/bin/env bash
set -euo pipefail

PORT=9700
DB=citus
DURATION=60
ITERATIONS=3
RESULTS_DIR="bench_results/$(date +%Y%m%d_%H%M%S)"
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

echo "=== Benchmark started at $(date) ==="
echo "=== Results dir: $RESULTS_DIR ==="
echo ""

for guc_val in on off; do
  reload_guc "$guc_val"

  # verify
  actual=$(psql -p $PORT -d $DB -tAc "SHOW citus.enable_single_task_fast_path;")
  echo "=== GUC = $guc_val (verified: $actual) ==="

  for workload in select update insert mixed; do
    script="${SCRIPT_DIR}/${SCRIPTS[$workload]}"
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
