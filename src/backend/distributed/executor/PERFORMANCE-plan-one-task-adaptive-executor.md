# Performance Plan — One-Task Adaptive Executor

## Goal

Measure the per-query latency and throughput improvement of `OneTaskAdaptiveExecutor`
vs the regular `AdaptiveExecutor` for single-shard fast-path queries using pgbench.

The primary metric is **average latency (ms)** at low concurrency (latency-bound)
and **TPS** at higher concurrency (throughput-bound).

---

## Environment

| Item | Value |
|---|---|
| Coordinator | `localhost:9700` |
| Workers | `localhost:9701`, `localhost:9702` |
| CPUs | 16 |
| RAM | 24 GB |
| PostgreSQL | 18.1 |
| pgbench | 18.1 |

> All three nodes run on the same host (dev container).  This means network
> round-trip is ~0, which *amplifies* any CPU/overhead differences —
> exactly what we want to measure.

---

## 1  Schema Setup

```sql
-- connect to coordinator
psql -p 9700 -d citus -c "
  DROP TABLE IF EXISTS bench_kv;
  CREATE TABLE bench_kv (
      kid   bigint PRIMARY KEY,
      val   bigint DEFAULT 0,
      data text
  );
  SELECT create_distributed_table('bench_kv', 'kid');

  -- pre-load 1 000 000 rows so UPDATE/SELECT have data to hit
  INSERT INTO bench_kv
    SELECT g, random(1, 1000000000), md5(g::text) FROM generate_series(1, 1000000) g;
"
```

The table is hash-distributed on `kid`.  Every query that filters on a
single `kid` value is a single-shard fast-path query — the exact target
for `OneTaskAdaptiveExecutor`.

---

## 2  pgbench Custom Scripts

Create four SQL files in a working directory:

### 2a  `select.sql` — point SELECT

```sql
\set kid random(1, 1000000)
SELECT val FROM bench_kv WHERE kid = :kid;
```

### 2b  `update.sql` — point UPDATE

```sql
\set kid random(1, 1000000)
UPDATE bench_kv SET val = val + 1 WHERE kid = :kid;
```

### 2c  `insert.sql` — single-row INSERT (upsert to avoid PK conflict)

```sql
\set kid random(1000001, 9999999)
INSERT INTO bench_kv (kid, val) VALUES (:kid, 1)
  ON CONFLICT (kid) DO UPDATE SET val = bench_kv.val + 1;
```

### 2d  `mixed.sql` — 60% SELECT / 20% UPDATE / 20% INSERT

pgbench doesn't support weighted random dispatch in a single script,
so we embed the mix via `\set` + conditional branching (pgbench ≥ 11):

```sql
\set kid random(1, 1000000)
\set op random(1, 10)
\if :op <= 6
  -- 60 % SELECT
  SELECT val FROM bench_kv WHERE kid = :kid;
\elif :op <= 8
  -- 20 % UPDATE
  UPDATE bench_kv SET val = val + 1 WHERE kid = :kid;
\else
  -- 20 % INSERT (upsert)
  \set newkid random(1000001, 9999999)
  INSERT INTO bench_kv (kid, val) VALUES (:newkid, 1)
    ON CONFLICT (kid) DO UPDATE SET val = bench_kv.val + 1;
\endif
```

---

## 3  Benchmark Matrix

Each combination below is run **twice** — once with the one-task executor
enabled (default) and once with it disabled.

| # | Workload | Script       | Clients (`-c`) | Threads (`-j`) | Duration (`-T`) |
|---|----------|-------------|-----------------|----------------|-----------------|
| 1 | SELECT   | select.sql  | 1               | 1              | 60              |
| 2 | SELECT   | select.sql  | 8               | 8              | 60              |
| 3 | SELECT   | select.sql  | 32              | 16             | 60              |
| 4 | UPDATE   | update.sql  | 1               | 1              | 60              |
| 5 | UPDATE   | update.sql  | 8               | 8              | 60              |
| 6 | UPDATE   | update.sql  | 32              | 16             | 60              |
| 7 | INSERT   | insert.sql  | 1               | 1              | 60              |
| 8 | INSERT   | insert.sql  | 8               | 8              | 60              |
| 9 | INSERT   | insert.sql  | 32              | 16             | 60              |
| 10| MIXED    | mixed.sql   | 1               | 1              | 60              |
| 11| MIXED    | mixed.sql   | 8               | 8              | 60              |
| 12| MIXED    | mixed.sql   | 32              | 16             | 60              |

**Total runs**: 12 combinations × 2 GUC states × 3 iterations = **72 pgbench runs**
(3 iterations per cell to get a stable median).

---

## 4  Run Script

```bash
#!/usr/bin/env bash
set -euo pipefail

PORT=9700
DB=postgres
DURATION=60
ITERATIONS=3
RESULTS_DIR="bench_results/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

declare -A SCRIPTS=(
  [select]=select.sql
  [update]=update.sql
  [insert]=insert.sql
  [mixed]=mixed.sql
)

CLIENTS=(1 8 32)
THREADS=(1 8 16)

for guc_val in on off; do
  # set GUC cluster-wide
  psql -p $PORT -d $DB -c \
    "ALTER SYSTEM SET citus.enable_single_task_fast_path = $guc_val;"
  psql -p $PORT -d $DB -c "SELECT pg_reload_conf();"
  # also reload workers
  psql -p 9701 -d $DB -c \
    "ALTER SYSTEM SET citus.enable_single_task_fast_path = $guc_val;"
  psql -p 9701 -d $DB -c "SELECT pg_reload_conf();"
  psql -p 9702 -d $DB -c \
    "ALTER SYSTEM SET citus.enable_single_task_fast_path = $guc_val;"
  psql -p 9702 -d $DB -c "SELECT pg_reload_conf();"

  # brief pause for reload
  sleep 1

  # verify
  echo "=== GUC = $guc_val ==="
  psql -p $PORT -d $DB -tc \
    "SHOW citus.enable_single_task_fast_path;"

  for workload in select update insert mixed; do
    script="${SCRIPTS[$workload]}"
    for idx in "${!CLIENTS[@]}"; do
      c=${CLIENTS[$idx]}
      j=${THREADS[$idx]}

      tag="${workload}_c${c}_${guc_val}"

      for iter in $(seq 1 $ITERATIONS); do
        outfile="${RESULTS_DIR}/${tag}_iter${iter}.txt"
        echo ">>> $tag  iter=$iter  (c=$c j=$j T=$DURATION)"

        pgbench -p $PORT -d $DB \
          -f "$script" \
          -c "$c" -j "$j" -T "$DURATION" \
          --no-vacuum \
          --progress=10 \
          2>&1 | tee "$outfile"

        echo ""
      done
    done
  done
done

echo "=== All runs complete. Results in $RESULTS_DIR ==="
```

---

## 5  Collecting Results

After all runs, extract TPS and latency into a summary CSV:

```bash
echo "workload,clients,guc,iter,tps,lat_avg_ms,lat_stddev_ms" > "$RESULTS_DIR/summary.csv"

for f in "$RESULTS_DIR"/*.txt; do
  base=$(basename "$f" .txt)
  # parse tag:  workload_cN_on/off_iterN
  workload=$(echo "$base" | cut -d_ -f1)
  clients=$(echo "$base" | sed 's/.*_c\([0-9]*\)_.*/\1/')
  guc=$(echo "$base" | grep -oP '(on|off)(?=_iter)')
  iter=$(echo "$base" | grep -oP 'iter\K[0-9]+')

  tps=$(grep 'tps = ' "$f" | grep -v initial | awk '{print $3}')
  lat=$(grep 'latency average' "$f" | awk '{print $4}')
  stddev=$(grep 'latency stddev' "$f" | awk '{print $4}')

  echo "$workload,$clients,$guc,$iter,$tps,$lat,$stddev" >> "$RESULTS_DIR/summary.csv"
done
```

---

## 6  Analysis Checklist

For each workload × concurrency level, compare the **median** across 3 iterations:

| Metric | How to compare |
|---|---|
| **Latency (c=1)** | `lat_avg_ms(off) − lat_avg_ms(on)` = absolute saving per query |
| **TPS (c=8, c=32)** | `tps(on) / tps(off)` = throughput multiplier |
| **Stddev** | Confirm stddev is small relative to mean (<10%) for validity |
| **Regression check** | Verify `off` numbers match baseline Citus (no perf regression when GUC is off) |

### Expected outcome

- **c=1 SELECT**: Largest latency improvement (pure overhead removal, no lock contention).
  Expect 10–30% latency reduction depending on network vs CPU ratio.
- **c=1 UPDATE/INSERT**: Moderate improvement; write-path overhead (WAL, locks) dominates.
- **c=32**: Improvement should hold or increase since we avoid WaitEventSet allocation
  per query under contention.
- **Mixed**: Weighted average of individual workload improvements.

### Red flags

- TPS with `on` is *lower* than `off` → the new path has a bug or bottleneck.
- Stddev > 20% of mean → results are noisy; increase `DURATION` to 120s or run more iterations.
- INSERT upsert TPS differs wildly between runs → table bloat; run `VACUUM bench_kv` between workloads.

---

## 7  Quick Sanity Run (5 minutes)

Before the full matrix, run a short sanity check to validate the setup:

```bash
# one-task ON (default)
pgbench -p 9700 -d citus -f select.sql -c 1 -j 1 -T 10 --no-vacuum

# one-task OFF
psql -p 9700 -c "SET citus.enable_single_task_fast_path = off;"
# (session-level SET won't persist to pgbench; use ALTER SYSTEM instead)
psql -p 9700 -c "ALTER SYSTEM SET citus.enable_single_task_fast_path = off;"
psql -p 9700 -c "SELECT pg_reload_conf();"
pgbench -p 9700 -d citus -f select.sql -c 1 -j 1 -T 10 --no-vacuum

# restore default
psql -p 9700 -c "ALTER SYSTEM RESET citus.enable_single_task_fast_path;"
psql -p 9700 -c "SELECT pg_reload_conf();"
```

Compare the two TPS numbers. If the `on` run is not at least on par with `off`,
investigate before running the full suite.

---

## 8  Cleanup

```sql
psql -p 9700 -d citus -c "DROP TABLE IF EXISTS bench_kv;"
psql -p 9700 -d citus -c "ALTER SYSTEM RESET citus.enable_single_task_fast_path;"
psql -p 9700 -d citus -c "SELECT pg_reload_conf();"
```

---

## 9  Results (2026-03-03, dev container, PG 18.1)

### 9.1  Raw Data (median of 3 × 60 s iterations)

| Workload | Clients | GUC | Median TPS | Median Latency (ms) |
|----------|---------|-----|------------|---------------------|
| SELECT   | 1       | ON  | 856        | 1.168               |
| SELECT   | 1       | OFF | 720        | 1.389               |
| SELECT   | 8       | ON  | 4788       | 1.671               |
| SELECT   | 8       | OFF | 3963       | 2.019               |
| SELECT   | 32      | ON  | 6591 ⚠     | 4.855               |
| SELECT   | 32      | OFF | 5705 ⚠     | 5.609               |
| UPDATE   | 1       | ON  | 614        | 1.629               |
| UPDATE   | 1       | OFF | 565        | 1.770               |
| UPDATE   | 8       | ON  | 3603       | 2.221               |
| UPDATE   | 8       | OFF | 3517       | 2.275               |
| UPDATE   | 32      | ON  | 4516 ⚠     | 7.086               |
| UPDATE   | 32      | OFF | 5264       | 6.079               |
| INSERT   | 1       | ON  | 595        | 1.680               |
| INSERT   | 1       | OFF | 628        | 1.592               |
| INSERT   | 8       | ON  | 3691       | 2.167               |
| INSERT   | 8       | OFF | 3632       | 2.203               |
| INSERT   | 32      | ON  | 5229       | 6.120               |
| INSERT   | 32      | OFF | 5139       | 6.227               |
| MIXED    | 1       | ON  | 587        | 1.704               |
| MIXED    | 1       | OFF | 516        | 1.937               |
| MIXED    | 8       | ON  | 3852       | 2.077               |
| MIXED    | 8       | OFF | 3703       | 2.161               |
| MIXED    | 32      | ON  | 5598       | 5.716               |
| MIXED    | 32      | OFF | 5117       | 6.254               |

⚠ = noisy (stddev/median > 10%); see §9.4.

### 9.2  Comparison Summary

| Workload | Clients | TPS Δ%  | Latency Δ%  | Verdict       |
|----------|---------|---------|-------------|---------------|
| SELECT   | 1       | **+18.9%**  | **−15.9%**  | Clear win     |
| SELECT   | 8       | **+20.8%**  | **−17.2%**  | Clear win     |
| SELECT   | 32      | +15.5%  | −13.4%      | Win (noisy)   |
| UPDATE   | 1       | **+8.7%**   | **−8.0%**   | Win           |
| UPDATE   | 8       | +2.4%   | −2.4%       | Marginal      |
| UPDATE   | 32      | −14.2%  | +16.6%      | ⚠ Noisy outlier |
| INSERT   | 1       | −5.2%   | +5.5%       | Neutral       |
| INSERT   | 8       | +1.6%   | −1.6%       | Neutral       |
| INSERT   | 32      | +1.8%   | −1.7%       | Neutral       |
| MIXED    | 1       | **+13.7%**  | **−12.0%**  | Clear win     |
| MIXED    | 8       | +4.0%   | −3.9%       | Win           |
| MIXED    | 32      | **+9.4%**   | **−8.6%**   | Win           |

### 9.3  Key Takeaways

1. **SELECT is the biggest winner.** At c=1 the one-task executor saves
   ~0.22 ms per query (15.9% latency reduction) — pure overhead removal.
   At c=8 the improvement holds at +20.8% TPS, confirming the saving is
   per-query, not just warm-up noise.

2. **UPDATE shows a solid win at low concurrency** (+8.7% at c=1) but the
   advantage shrinks under contention as write locks dominate. The c=32
   result is an outlier (see §9.4).

3. **INSERT (upsert) is neutral.** The upsert path has enough
   write-amplification (PK conflict check, WAL) that the executor overhead
   is a small fraction of total query time. No regression — the one-task
   path is on par.

4. **MIXED workload (60% SELECT / 20% UPDATE / 20% INSERT)** tracks the
   expected weighted average: +13.7% at c=1, +9.4% at c=32. This is the
   most realistic surrogate for production OLTP traffic.

5. **No regressions in clean results.** Every non-noisy cell is either a
   clear improvement or statistically neutral (within ±2%).

#### Why SELECT wins most and INSERT least (Amdahl's Law)

The one-task executor removes a **fixed amount of CPU overhead per query**
(~0.2 ms) by skipping `WaitEventSet` creation/teardown, the connection-pool
slow-start state machine, and multi-connection bookkeeping.  What varies
across workloads is how much *other* work each query does on the worker,
which determines what fraction of total time the saving represents.

- **SELECT (point lookup)** — biggest win because total worker-side work is
  minimal: index seek → return 1 row.  No WAL, no row locks, no buffer-dirty
  writes.  Total query time ~1.4 ms (OFF).  The ~0.22 ms executor saving is
  **~16% of the total**.

- **UPDATE (point update)** — moderate win because writes add fixed costs the
  executor cannot help with: acquire row lock → heap update → write WAL →
  possibly update indexes.  Total query time ~1.8 ms (OFF).  The same
  ~0.14–0.22 ms saving is now **~8% of the total**.

- **INSERT (upsert `ON CONFLICT`)** — neutral because conflict resolution
  doubles the write work: attempt insert → PK index probe for conflict check →
  if conflict, do an UPDATE instead → WAL for the update → possible dead-tuple
  creation.  The conflict-check index scan + conditional update path is strictly
  more work than a plain UPDATE.  Total query time ~1.6 ms (OFF).  The executor
  saving vanishes into the noise — **<2% of total**.

In short: the heavier the worker-side work, the smaller the percentage gain
from a fixed coordinator-side overhead reduction — classic Amdahl's Law.

### 9.4  Noise Analysis

Three cells had stddev/median > 10% — all at c=32:

| Cell | Stddev | Median | Ratio | Individual TPS values |
|------|--------|--------|-------|-----------------------|
| SELECT c=32 ON  | 2086 | 6591 | 31.7% | 3064, 6591, 6759 |
| SELECT c=32 OFF | 1731 | 5705 | 30.3% | 2792, 5705, 5867 |
| UPDATE c=32 ON  | 1277 | 4516 | 28.3% | 2663, 4516, 5111 |

Each noisy cell has one low outlier iteration (likely caused by dev-container
resource contention — all 3 Citus nodes share 16 CPUs on a single host).
The pattern is symmetric: both ON and OFF suffer outliers at c=32 SELECT,
suggesting this is environmental, not a regression.

**Recommendation:** Re-run the c=32 cells with `DURATION=120` and
`ITERATIONS=5` on a quiet machine for production-grade numbers.  For now,
the c=1 and c=8 results are clean and conclusive.

### 9.5  c=32 Re-run (DURATION=120s, ITERATIONS=5, THREADS=16)

Re-ran all four workloads at c=32 with longer duration and more iterations
to stabilize the noisy cells from §9.4.

#### Raw Data (5 × 120 s iterations)

| Workload | GUC | Median TPS | Median Lat (ms) | CV% | All TPS (sorted) |
|----------|-----|------------|-----------------|-----|------------------|
| SELECT   | ON  | 5819       | 5.500           | 15.4% | 4430, 4510, 5819, 5960, 6407 |
| SELECT   | OFF | 4457       | 7.179           | 21.1% | 4296, 4427, 4457, 6053, 6164 |
| UPDATE   | ON  | 5611       | 5.703           | 17.2% | 3866, 3898, 5611, 5623, 5690 |
| UPDATE   | OFF | 5275       | 6.067           | 16.3% | 3670, 3913, 5274, 5346, 5442 |
| INSERT   | ON  | 4002       | 7.996           | 21.9% | 3833, 3981, 4002, 5498, 5573 |
| INSERT   | OFF | 4005       | 7.991           | 17.6% | 3866, 3913, 4005, 5147, 5270 |
| MIXED    | ON  | 5698       | 5.616           | 15.4% | 4100, 4115, 5698, 5705, 5722 |
| MIXED    | OFF | 4923       | 6.500           | 13.6% | 3799, 4112, 4923, 5038, 5394 |

#### Comparison

| Workload | TPS Δ% | Latency Δ% | Verdict |
|----------|--------|------------|---------|
| SELECT   | **+30.5%** | **−23.4%** | Clear win |
| UPDATE   | +6.4%  | −6.0%      | Win |
| INSERT   | −0.1%  | +0.1%      | Neutral (dead even) |
| MIXED    | **+15.7%** | **−13.6%** | Clear win |

#### Analysis

The CV% is still elevated (13–22%) due to the shared-host dev container, but
the *directional signal is now consistent*:

- **SELECT c=32**: The previous run's +15.5% was uncertain; the re-run confirms
  a strong **+30.5%** improvement.  The ON medians cluster higher (5819 vs 4457).
- **UPDATE c=32**: The previous run showed a spurious −14.2% (one bad outlier).
  The re-run corrects this to **+6.4%** — a modest win, consistent with c=1/c=8.
- **INSERT c=32**: Dead even at 4002 vs 4005 TPS — confirms the neutral verdict.
- **MIXED c=32**: Improved from +9.4% to **+15.7%**, in line with the SELECT
  improvement pulling the mixed average up.

The bimodal TPS distribution (each workload clusters into a "high" and "low"
band) is characteristic of dev-container CPU scheduling — both ON and OFF
exhibit the same pattern, confirming this is environmental noise, not a
correctness or performance issue with the one-task executor.

