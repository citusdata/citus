#!/usr/bin/env bash
#
# bench_citus_caching.sh — Self-contained benchmark for Citus prepared statement caching
#
# Usage:
#   ./bench_citus_caching.sh [OPTIONS]
#
# Options:
#   -p PORT        Coordinator port (default: 9700)
#   -d DATABASE    Database name (default: postgres)
#   -i ITERATIONS  Number of iterations per workload/concurrency combo (default: 3)
#   -T DURATION    Duration in seconds for each pgbench run (default: 30)
#   -r ROWS        Number of rows in benchmark table (default: 10000)
#   -c CLIENTS     Comma-separated client counts (default: 1,4,8,32)
#   -o OUTDIR      Output directory for results (default: bench_results/<timestamp>)
#   -h             Show this help
#
set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
PORT=9700
DATABASE=postgres
ITERATIONS=3
DURATION=30
ROWS=10000
CLIENTS="1,4,8,32"
OUTDIR=""

usage() {
    sed -n '3,14p' "$0" | sed 's/^# \?//'
    exit 0
}

while getopts "p:d:i:T:r:c:o:h" opt; do
    case "$opt" in
        p) PORT="$OPTARG" ;;
        d) DATABASE="$OPTARG" ;;
        i) ITERATIONS="$OPTARG" ;;
        T) DURATION="$OPTARG" ;;
        r) ROWS="$OPTARG" ;;
        c) CLIENTS="$OPTARG" ;;
        o) OUTDIR="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="${OUTDIR:-bench_results/${TIMESTAMP}}"
mkdir -p "$OUTDIR"

PSQL="psql -p $PORT -d $DATABASE -X -q"
PGBENCH="pgbench -p $PORT -d $DATABASE --protocol=prepared"

IFS=',' read -ra CLIENT_COUNTS <<< "$CLIENTS"

# ── Temp files for pgbench scripts ────────────────────────────────────────────
BENCH_SELECT=$(mktemp /tmp/bench_select.XXXXXX.sql)
BENCH_MIXED=$(mktemp /tmp/bench_mixed.XXXXXX.sql)
trap 'rm -f "$BENCH_SELECT" "$BENCH_MIXED"' EXIT

cat > "$BENCH_SELECT" << 'SQL'
\set id random(1, 10000)
SELECT id, val, payload FROM bench_dist WHERE id = :id;
SQL

cat > "$BENCH_MIXED" << 'SQL'
\set id random(1, 10000)
SELECT id, val, payload FROM bench_dist WHERE id = :id;
\set uid random(1, 10000)
\set newval random(1, 1000000)
UPDATE bench_dist SET val = :newval WHERE id = :uid;
SQL

# ── Banner ────────────────────────────────────────────────────────────────────
cat << EOF

╔══════════════════════════════════════════════════════════════╗
║         Citus Prepared Statement Caching Benchmark          ║
╚══════════════════════════════════════════════════════════════╝

  Coordinator port : $PORT
  Database         : $DATABASE
  Iterations       : $ITERATIONS per config
  Duration         : ${DURATION}s per run
  Table rows       : $ROWS
  Client counts    : ${CLIENTS}
  Output           : $OUTDIR/

EOF

# ── Setup benchmark table ─────────────────────────────────────────────────────
echo "── Setting up benchmark table (bench_dist, $ROWS rows) ──"

$PSQL << SQL
DROP TABLE IF EXISTS bench_dist;
CREATE TABLE bench_dist (
    id     bigint PRIMARY KEY,
    val    bigint,
    payload text
);
SELECT create_distributed_table('bench_dist', 'id');
INSERT INTO bench_dist
SELECT s, (random() * 1000000)::bigint, md5(s::text)
FROM generate_series(1, $ROWS) s;
ANALYZE bench_dist;
SQL

echo "  ✓ Table ready"
echo ""

# ── Verify GUC exists ────────────────────────────────────────────────────────
if ! $PSQL -c "SHOW citus.enable_prepared_statement_caching" &>/dev/null; then
    echo "ERROR: GUC citus.enable_prepared_statement_caching not found."
    echo "       Make sure you are running the branch with caching support."
    exit 1
fi
echo "  ✓ GUC available"
echo ""

# ── Run a single benchmark ───────────────────────────────────────────────────
#  run_bench <workload_name> <script_file> <clients> <guc_on|guc_off> <iter>
run_bench() {
    local name="$1" script="$2" clients="$3" guc_mode="$4" iter="$5"
    local outfile="${OUTDIR}/${name}_c${clients}_${guc_mode}_iter${iter}.txt"
    local env_opts=""

    if [[ "$guc_mode" == "on" ]]; then
        env_opts="-c citus.enable_prepared_statement_caching=on"
    fi

    PGOPTIONS="$env_opts" $PGBENCH \
        -f "$script" \
        -c "$clients" \
        -j "$clients" \
        -T "$DURATION" \
        > "$outfile" 2>&1

    # Extract key metrics
    local tps latency
    tps=$(grep "^tps" "$outfile" | grep -oP '[\d.]+' | head -1)
    latency=$(grep "^latency average" "$outfile" | grep -oP '[\d.]+' | head -1)

    echo "$tps $latency"
}

# ── Main benchmark loop ──────────────────────────────────────────────────────
declare -A RESULTS  # key: "workload:clients:guc" → "tps1,tps2,..."

WORKLOADS=("select:$BENCH_SELECT" "mixed:$BENCH_MIXED")

total_runs=$(( ${#WORKLOADS[@]} * ${#CLIENT_COUNTS[@]} * 2 * ITERATIONS ))
current_run=0

for wl_entry in "${WORKLOADS[@]}"; do
    IFS=':' read -r wl_name wl_script <<< "$wl_entry"

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Workload: $wl_name"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    for c in "${CLIENT_COUNTS[@]}"; do
        echo ""
        echo "  ── clients=$c ──"

        for guc in off on; do
            key="${wl_name}:${c}:${guc}"
            tps_list=""
            lat_list=""

            for (( iter=1; iter<=ITERATIONS; iter++ )); do
                current_run=$((current_run + 1))
                printf "    [%d/%d] %-8s c=%-3s guc=%-3s iter=%d ... " \
                    "$current_run" "$total_runs" "$wl_name" "$c" "$guc" "$iter"

                result=$(run_bench "$wl_name" "$wl_script" "$c" "$guc" "$iter")
                read -r tps latency <<< "$result"

                printf "TPS=%-10s Latency=%sms\n" "$tps" "$latency"

                tps_list="${tps_list:+$tps_list,}$tps"
                lat_list="${lat_list:+$lat_list,}$latency"
            done

            RESULTS["${key}:tps"]="$tps_list"
            RESULTS["${key}:lat"]="$lat_list"
        done
    done
done

echo ""
echo ""

# ── Generate summary ──────────────────────────────────────────────────────────

calc_stats() {
    # Given comma-separated numbers, output: mean min max
    local nums="$1"
    echo "$nums" | tr ',' '\n' | awk '
        NR==1 { min=$1; max=$1; sum=$1; next }
        { sum+=$1; if($1<min)min=$1; if($1>max)max=$1 }
        END { printf "%.1f %.1f %.1f", sum/NR, min, max }
    '
}

SUMMARY="$OUTDIR/summary.txt"

{
    echo "╔══════════════════════════════════════════════════════════════════════════════════════════╗"
    echo "║                    Citus Prepared Statement Caching — Benchmark Summary                 ║"
    echo "╠══════════════════════════════════════════════════════════════════════════════════════════╣"
    echo "║  Date: $(date)                                              ║"
    echo "║  Duration: ${DURATION}s/run, ${ITERATIONS} iterations, ${ROWS} rows                                          ║"
    echo "╚══════════════════════════════════════════════════════════════════════════════════════════╝"
    echo ""

    for wl_entry in "${WORKLOADS[@]}"; do
        IFS=':' read -r wl_name _ <<< "$wl_entry"

        echo "┌─────────────────────────────────────────────────────────────────────────────────────┐"
        echo "│  Workload: $wl_name"
        echo "├──────────┬────────────────────────┬────────────────────────┬────────────────────────┤"
        printf "│ %-8s │ %-22s │ %-22s │ %-22s │\n" "Clients" "TPS (OFF)" "TPS (ON)" "Δ TPS"
        echo "├──────────┼────────────────────────┼────────────────────────┼────────────────────────┤"

        for c in "${CLIENT_COUNTS[@]}"; do
            off_tps="${RESULTS[${wl_name}:${c}:off:tps]}"
            on_tps="${RESULTS[${wl_name}:${c}:on:tps]}"
            off_lat="${RESULTS[${wl_name}:${c}:off:lat]}"
            on_lat="${RESULTS[${wl_name}:${c}:on:lat]}"

            read -r off_mean off_min off_max <<< "$(calc_stats "$off_tps")"
            read -r on_mean on_min on_max <<< "$(calc_stats "$on_tps")"
            read -r off_lat_mean _ _ <<< "$(calc_stats "$off_lat")"
            read -r on_lat_mean _ _ <<< "$(calc_stats "$on_lat")"

            delta=$(awk "BEGIN { if ($off_mean > 0) printf \"%.1f\", (($on_mean - $off_mean) / $off_mean) * 100; else print \"N/A\" }")

            printf "│ %-8s │ %8s (%s-%s) │ %8s (%s-%s) │ %21s%% │\n" \
                "$c" "$off_mean" "$off_min" "$off_max" "$on_mean" "$on_min" "$on_max" "$delta"
        done

        echo "├──────────┼────────────────────────┼────────────────────────┼────────────────────────┤"
        printf "│ %-8s │ %-22s │ %-22s │ %-22s │\n" "Clients" "Latency (OFF) ms" "Latency (ON) ms" "Δ Latency"
        echo "├──────────┼────────────────────────┼────────────────────────┼────────────────────────┤"

        for c in "${CLIENT_COUNTS[@]}"; do
            off_lat="${RESULTS[${wl_name}:${c}:off:lat]}"
            on_lat="${RESULTS[${wl_name}:${c}:on:lat]}"

            read -r off_lat_mean off_lat_min off_lat_max <<< "$(calc_stats "$off_lat")"
            read -r on_lat_mean on_lat_min on_lat_max <<< "$(calc_stats "$on_lat")"

            delta=$(awk "BEGIN { if ($off_lat_mean > 0) printf \"%.1f\", (($on_lat_mean - $off_lat_mean) / $off_lat_mean) * 100; else print \"N/A\" }")

            printf "│ %-8s │ %8s (%s-%s) │ %8s (%s-%s) │ %21s%% │\n" \
                "$c" "$off_lat_mean" "$off_lat_min" "$off_lat_max" "$on_lat_mean" "$on_lat_min" "$on_lat_max" "$delta"
        done

        echo "└──────────┴────────────────────────┴────────────────────────┴────────────────────────┘"
        echo ""
    done

    echo "Raw results in: $OUTDIR/"
    echo "Each file: <workload>_c<clients>_<on|off>_iter<N>.txt"

} | tee "$SUMMARY"

# ── Also write a CSV for easy import ─────────────────────────────────────────
CSV="$OUTDIR/results.csv"
echo "workload,clients,guc,iteration,tps,latency_ms" > "$CSV"

for wl_entry in "${WORKLOADS[@]}"; do
    IFS=':' read -r wl_name _ <<< "$wl_entry"
    for c in "${CLIENT_COUNTS[@]}"; do
        for guc in off on; do
            IFS=',' read -ra tps_arr <<< "${RESULTS[${wl_name}:${c}:${guc}:tps]}"
            IFS=',' read -ra lat_arr <<< "${RESULTS[${wl_name}:${c}:${guc}:lat]}"
            for (( i=0; i<${#tps_arr[@]}; i++ )); do
                echo "${wl_name},${c},${guc},$((i+1)),${tps_arr[$i]},${lat_arr[$i]}" >> "$CSV"
            done
        done
    done
done

echo ""
echo "CSV written to: $CSV"
echo "Done."
