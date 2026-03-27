#!/usr/bin/env python3
"""
bench_citus_caching.py — Benchmark for Citus prepared statement caching

Rewrite of bench_citus_caching.sh in Python for macOS compatibility
(macOS ships bash 3.2 which lacks associative arrays).

Usage:
    python3 bench_citus_caching.py [OPTIONS]

Options:
    -p PORT        Coordinator port (default: 9700)
    -d DATABASE    Database name (default: postgres)
    -i ITERATIONS  Number of iterations per workload/concurrency combo (default: 3)
    -T DURATION    Duration in seconds for each pgbench run (default: 30)
    -r ROWS        Number of rows in benchmark table (default: 10000)
    -c CLIENTS     Comma-separated client counts (default: 1,4,8,32)
    -w WORKLOADS   Comma-separated workload names (default: select,insert,update,mixed)
    -o OUTDIR      Output directory for results (default: bench_results/<timestamp>)
    -h             Show this help
"""

import argparse
import csv
import math
import os
import re
import statistics
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

# ── Workload SQL templates ────────────────────────────────────────────────────

WORKLOAD_SQL = {
    "select": """\
\\set id random(1, {rows})
SELECT id, val, payload FROM bench_dist WHERE id = :id;
""",
    "insert": """\
\\set id random({rows_plus1}, {rows_times10})
\\set val random(1, 1000000)
INSERT INTO bench_dist (id, val, payload) VALUES (:id, :val, md5(:id::text))
ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;
""",
    "update": """\
\\set id random(1, {rows})
\\set newval random(1, 1000000)
UPDATE bench_dist SET val = :newval WHERE id = :id;
""",
    "mixed": """\
\\set id random(1, {rows})
SELECT id, val, payload FROM bench_dist WHERE id = :id;
\\set uid random(1, {rows})
\\set newval random(1, 1000000)
UPDATE bench_dist SET val = :newval WHERE id = :uid;
""",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Citus prepared statement caching benchmark"
    )
    parser.add_argument("-p", "--port", type=int, default=9700, help="Coordinator port")
    parser.add_argument("-d", "--database", default="postgres", help="Database name")
    parser.add_argument(
        "-i", "--iterations", type=int, default=3, help="Iterations per config"
    )
    parser.add_argument(
        "-T", "--duration", type=int, default=30, help="Seconds per pgbench run"
    )
    parser.add_argument(
        "-r", "--rows", type=int, default=10000, help="Rows in benchmark table"
    )
    parser.add_argument(
        "-c", "--clients", default="1,8,32", help="Comma-separated client counts"
    )
    parser.add_argument(
        "-w",
        "--workloads",
        default="select,insert,update,mixed",
        help="Comma-separated workload names",
    )
    parser.add_argument("-o", "--outdir", default=None, help="Output directory")
    return parser.parse_args()


def run_psql(port, database, sql):
    """Run a SQL command via psql. Returns (returncode, stdout, stderr)."""
    result = subprocess.run(
        ["psql", "-p", str(port), "-d", database, "-X", "-q"],
        input=sql,
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout, result.stderr


def run_pgbench(port, database, script_path, clients, duration, guc_on):
    """Run pgbench and return the raw output text."""
    env = os.environ.copy()
    if guc_on:
        env["PGOPTIONS"] = "-c citus.enable_prepared_statement_caching=on"
    else:
        # Explicitly clear PGOPTIONS so a leftover value doesn't leak
        env.pop("PGOPTIONS", None)

    threads = min(clients, os.cpu_count() or 1)
    cmd = [
        "pgbench",
        "-p", str(port),
        "-d", database,
        "--protocol=prepared",
        "-f", str(script_path),
        "-c", str(clients),
        "-j", str(threads),
        "-T", str(duration),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    return result.stdout + result.stderr


def extract_metrics(pgbench_output):
    """Extract TPS and latency from pgbench output."""
    tps = None
    latency = None

    m = re.search(r"tps = ([\d.]+)", pgbench_output)
    if m:
        tps = float(m.group(1))

    m = re.search(r"latency average = ([\d.]+)", pgbench_output)
    if m:
        latency = float(m.group(1))

    return tps, latency


def calc_stats(values):
    """Return (mean, min, max, stdev) for a list of numbers."""
    if not values:
        return (0.0, 0.0, 0.0, 0.0)
    mean = statistics.mean(values)
    lo = min(values)
    hi = max(values)
    sd = statistics.stdev(values) if len(values) > 1 else 0.0
    return (mean, lo, hi, sd)


def print_summary(results, workload_names, client_counts, duration, iterations, rows, outdir):
    """Print and save the summary table."""
    lines = []

    def out(s=""):
        lines.append(s)
        print(s)

    out("=" * 92)
    out("  Citus Prepared Statement Caching — Benchmark Summary")
    out(f"  Date: {datetime.now()}")
    out(f"  Duration: {duration}s/run, {iterations} iterations, {rows} rows")
    out("=" * 92)

    for wl in workload_names:
        out()
        out(f"--- {wl.upper()} workload ---")
        out(
            f"{'Clients':>8} | {'TPS OFF':>16} | {'TPS ON':>16} | "
            f"{'Δ TPS':>8} | {'Lat OFF (ms)':>14} | {'Lat ON (ms)':>14} | {'Δ Lat':>8}"
        )
        out("-" * 95)

        for c in client_counts:
            off_key = (wl, c, "off")
            on_key = (wl, c, "on")

            if off_key not in results or on_key not in results:
                continue

            off_tps_stats = calc_stats(results[off_key]["tps"])
            on_tps_stats = calc_stats(results[on_key]["tps"])
            off_lat_stats = calc_stats(results[off_key]["lat"])
            on_lat_stats = calc_stats(results[on_key]["lat"])

            tps_delta = (
                (on_tps_stats[0] - off_tps_stats[0]) / off_tps_stats[0] * 100
                if off_tps_stats[0] > 0
                else 0
            )
            lat_delta = (
                (on_lat_stats[0] - off_lat_stats[0]) / off_lat_stats[0] * 100
                if off_lat_stats[0] > 0
                else 0
            )

            out(
                f"{c:>8} | "
                f"{off_tps_stats[0]:>10.1f} ±{off_tps_stats[3]:>4.0f} | "
                f"{on_tps_stats[0]:>10.1f} ±{on_tps_stats[3]:>4.0f} | "
                f"{tps_delta:>+7.2f}% | "
                f"{off_lat_stats[0]:>12.4f}ms | "
                f"{on_lat_stats[0]:>12.4f}ms | "
                f"{lat_delta:>+7.2f}%"
            )

    out()
    out("=" * 92)

    # Statistical significance
    out()
    out("--- Statistical Significance (Welch t-test) ---")
    out(
        f"{'Workload':>10} {'Clients':>8} | {'OFF mean':>12} | {'ON mean':>12} | "
        f"{'Δ%':>8} | {'t-stat':>8} | {'Significant?':>14}"
    )
    out("-" * 85)

    for wl in workload_names:
        for c in client_counts:
            off_key = (wl, c, "off")
            on_key = (wl, c, "on")
            if off_key not in results or on_key not in results:
                continue

            off_tps = results[off_key]["tps"]
            on_tps = results[on_key]["tps"]

            n1, n2 = len(off_tps), len(on_tps)
            m1, m2 = statistics.mean(off_tps), statistics.mean(on_tps)
            s1 = statistics.stdev(off_tps) if n1 > 1 else 0.001
            s2 = statistics.stdev(on_tps) if n2 > 1 else 0.001

            se = math.sqrt(s1 ** 2 / n1 + s2 ** 2 / n2)
            t_stat = (m2 - m1) / se if se > 0 else 0
            delta = (m2 - m1) / m1 * 100 if m1 > 0 else 0

            # |t| > 2.18 ~ p<0.05 for df≈12 (two-tailed)
            sig = "YES (p<0.05)" if abs(t_stat) > 2.18 else "no"

            out(
                f"{wl:>10} {c:>8} | {m1:>12.1f} | {m2:>12.1f} | "
                f"{delta:>+7.2f}% | {t_stat:>+8.2f} | {sig:>14}"
            )

    out()
    out(f"Raw results in: {outdir}/")

    # Write summary file
    summary_path = os.path.join(outdir, "summary.txt")
    with open(summary_path, "w") as f:
        f.write("\n".join(lines) + "\n")


def write_csv(results, workload_names, client_counts, outdir):
    """Write per-iteration CSV for easy import."""
    csv_path = os.path.join(outdir, "results.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["workload", "clients", "guc", "iteration", "tps", "latency_ms"])
        for wl in workload_names:
            for c in client_counts:
                for guc in ("off", "on"):
                    key = (wl, c, guc)
                    if key not in results:
                        continue
                    for i, (tps, lat) in enumerate(
                        zip(results[key]["tps"], results[key]["lat"]), 1
                    ):
                        writer.writerow([wl, c, guc, i, f"{tps:.3f}", f"{lat:.3f}"])
    print(f"\nCSV written to: {csv_path}")


def main():
    args = parse_args()

    port = args.port
    database = args.database
    iterations = args.iterations
    duration = args.duration
    rows = args.rows
    client_counts = [int(x) for x in args.clients.split(",")]
    workload_names = [w.strip() for w in args.workloads.split(",")]
    outdir = args.outdir or f"bench_results/{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    os.makedirs(outdir, exist_ok=True)

    # ── Banner ────────────────────────────────────────────────────────────────
    print(
        f"""
╔══════════════════════════════════════════════════════════════╗
║         Citus Prepared Statement Caching Benchmark          ║
╚══════════════════════════════════════════════════════════════╝

  Coordinator port : {port}
  Database         : {database}
  Iterations       : {iterations} per config
  Duration         : {duration}s per run
  Table rows       : {rows}
  Workloads        : {', '.join(workload_names)}
  Client counts    : {', '.join(str(c) for c in client_counts)}
  Output           : {outdir}/
"""
    )

    # ── Setup benchmark table ─────────────────────────────────────────────────
    print("── Setting up benchmark table (bench_dist, {} rows) ──".format(rows))

    setup_sql = f"""\
DROP TABLE IF EXISTS bench_dist;
CREATE TABLE bench_dist (
    id     bigint PRIMARY KEY,
    val    bigint,
    payload text
);
SELECT create_distributed_table('bench_dist', 'id');
INSERT INTO bench_dist
SELECT s, (random() * 1000000)::bigint, md5(s::text)
FROM generate_series(1, {rows}) s;
ANALYZE bench_dist;
"""
    rc, stdout, stderr = run_psql(port, database, setup_sql)
    if rc != 0:
        print(f"ERROR setting up table:\n{stderr}", file=sys.stderr)
        sys.exit(1)
    print("  ✓ Table ready\n")

    # ── Verify GUC exists ─────────────────────────────────────────────────────
    rc, _, _ = run_psql(
        port, database, "SHOW citus.enable_prepared_statement_caching"
    )
    if rc != 0:
        print(
            "ERROR: GUC citus.enable_prepared_statement_caching not found.\n"
            "       Make sure you are running the branch with caching support.",
            file=sys.stderr,
        )
        sys.exit(1)
    print("  ✓ GUC available\n")

    # ── Write workload SQL files to temp dir ──────────────────────────────────
    tmpdir = tempfile.mkdtemp(prefix="bench_citus_")
    script_paths = {}
    for wl_name in workload_names:
        if wl_name not in WORKLOAD_SQL:
            print(f"ERROR: unknown workload '{wl_name}'. "
                  f"Available: {', '.join(WORKLOAD_SQL.keys())}", file=sys.stderr)
            sys.exit(1)
        sql = WORKLOAD_SQL[wl_name].format(
            rows=rows,
            rows_plus1=rows + 1,
            rows_times10=rows * 10,
        )
        path = os.path.join(tmpdir, f"{wl_name}.sql")
        with open(path, "w") as f:
            f.write(sql)
        script_paths[wl_name] = path

    # ── Main benchmark loop ───────────────────────────────────────────────────
    results = {}  # (workload, clients, guc) -> {"tps": [...], "lat": [...]}

    total_runs = len(workload_names) * len(client_counts) * 2 * iterations
    current_run = 0

    for wl_name in workload_names:
        print("━" * 58)
        print(f"  Workload: {wl_name}")
        print("━" * 58)

        for c in client_counts:
            print(f"\n  ── clients={c} ──")

            for guc in ("off", "on"):
                key = (wl_name, c, guc)
                results[key] = {"tps": [], "lat": []}

                for iteration in range(1, iterations + 1):
                    current_run += 1
                    sys.stdout.write(
                        f"    [{current_run}/{total_runs}] {wl_name:<8s} "
                        f"c={c:<3d} guc={guc:<3s} iter={iteration} ... "
                    )
                    sys.stdout.flush()

                    output = run_pgbench(
                        port, database, script_paths[wl_name],
                        c, duration, guc_on=(guc == "on"),
                    )

                    # Save raw output
                    outfile = os.path.join(
                        outdir, f"{wl_name}_c{c}_{guc}_iter{iteration}.txt"
                    )
                    with open(outfile, "w") as f:
                        f.write(output)

                    tps, latency = extract_metrics(output)
                    if tps is None or latency is None:
                        print(f"FAILED (could not parse pgbench output)")
                        print(f"  Output saved to: {outfile}", file=sys.stderr)
                        continue

                    results[key]["tps"].append(tps)
                    results[key]["lat"].append(latency)
                    print(f"TPS={tps:<12.3f} Latency={latency}ms")

    print("\n")

    # ── Clean up temp files ───────────────────────────────────────────────────
    for path in script_paths.values():
        os.unlink(path)
    os.rmdir(tmpdir)

    # ── Generate summary and CSV ──────────────────────────────────────────────
    print_summary(results, workload_names, client_counts, duration, iterations, rows, outdir)
    write_csv(results, workload_names, client_counts, outdir)
    print("Done.")


if __name__ == "__main__":
    main()
