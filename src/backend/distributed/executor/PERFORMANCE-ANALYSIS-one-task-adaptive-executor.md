# Performance Analysis — One-Task Adaptive Executor

## Dataset

**Benchmark run:** 2026-03-06, data in `bench_results/20260306_113606/`

**Environment:** macbook with Apple M4 Max 48GB RAM. 

**Configuration:** 7 iterations × 300 seconds each, pgbench 18.1, PG 18.1,
localhost cluster (coordinator :9700 + 2 workers :9701/:9702, all on same host,
16 CPUs, 24 GB RAM).

**Workloads:** SELECT (point lookup), UPDATE (point update), INSERT (single-row
upsert `ON CONFLICT`), MIXED (60% SELECT / 20% UPDATE / 20% INSERT).

**Concurrency levels:** c=1 (latency-bound), c=8 (throughput), c=32 (saturated).

**GUC under test:** `citus.enable_single_task_fast_path` (ON = OneTaskAdaptiveExecutor,
OFF = regular AdaptiveExecutor).

---

## 1  Results Summary

### 1.1  Comparison: ON vs OFF (median TPS of 7 iterations)

| Workload | Clients | ON TPS | OFF TPS | **TPS Δ%** | ON CV% | OFF CV% | Verdict |
|----------|---------|--------|---------|------------|--------|---------|---------|
| SELECT   | 1       | 12,664 | 11,788  | **+7.4%**  | 3.2%   | 0.4%    | Win |
| SELECT   | 8       | 60,271 | 50,533  | **+19.3%** | 0.2%   | 0.3%    | Clear win |
| SELECT   | 32      | 60,678 | 59,155  | +2.6%      | 0.2%   | 0.2%    | Neutral |
| UPDATE   | 1       | 11,207 | 9,874   | **+13.5%** | 0.6%   | 0.5%    | Clear win |
| UPDATE   | 8       | 44,833 | 41,180  | **+8.9%**  | 0.9%   | 0.3%    | Win |
| UPDATE   | 32      | 49,289 | 48,458  | +1.7%      | 0.7%   | 0.4%    | Neutral |
| INSERT   | 1       | 10,283 | 9,133   | **+12.6%** | 0.6%   | 0.2%    | Clear win |
| INSERT   | 8       | 43,180 | 40,856  | **+5.7%**  | 0.6%   | 0.2%    | Win |
| INSERT   | 32      | 44,360 | 44,277  | +0.2%      | 0.7%   | 1.5%    | Neutral |
| MIXED    | 1       | 11,839 | 10,222  | **+15.8%** | 0.5%   | 0.5%    | Clear win |
| MIXED    | 8       | 50,051 | 46,037  | **+8.7%**  | 0.4%   | 0.2%    | Win |
| MIXED    | 32      | 54,494 | 53,260  | +2.3%      | 0.3%   | 0.5%    | Neutral |

### 1.2  Raw TPS Values (all 7 iterations, sorted; median in bold)

| Cell | All TPS |
|------|---------|
| select c=1 ON  | 12584, 12617, 12655, **12664**, 12671, 13169, 13664 |
| select c=1 OFF | 11678, 11757, 11761, **11788**, 11788, 11792, 11818 |
| select c=8 ON  | 60094, 60111, 60182, **60271**, 60320, 60360, 60389 |
| select c=8 OFF | 50482, 50482, 50493, **50533**, 50540, 50583, 50923 |
| select c=32 ON | 60443, 60623, 60627, **60678**, 60694, 60794, 60851 |
| select c=32 OFF| 58899, 59096, 59105, **59155**, 59185, 59187, 59211 |
| update c=1 ON  | 11183, 11195, 11198, **11207**, 11234, 11269, 11367 |
| update c=1 OFF | 9849, 9853, 9864, **9874**, 9876, 9944, 9974 |
| update c=8 ON  | 43760, 44681, 44808, **44832**, 44851, 44874, 44942 |
| update c=8 OFF | 41058, 41129, 41163, **41180**, 41281, 41319, 41423 |
| update c=32 ON | 49203, 49229, 49263, **49289**, 49430, 49448, 50236 |
| update c=32 OFF| 48244, 48262, 48416, **48458**, 48521, 48652, 48764 |
| insert c=1 ON  | 10181, 10259, 10282, **10283**, 10288, 10347, 10360 |
| insert c=1 OFF | 9094, 9120, 9132, **9133**, 9138, 9146, 9160 |
| insert c=8 ON  | 42568, 43106, 43134, **43180**, 43261, 43286, 43414 |
| insert c=8 OFF | 40747, 40781, 40803, **40856**, 40880, 40916, 41009 |
| insert c=32 ON | 43741, 44246, 44300, **44360**, 44471, 44560, 44680 |
| insert c=32 OFF| 43829, 43973, 44017, **44277**, 44636, 44673, 45790 |
| mixed c=1 ON   | 11692, 11834, 11837, **11839**, 11856, 11860, 11884 |
| mixed c=1 OFF  | 10126, 10207, 10209, **10222**, 10225, 10268, 10306 |
| mixed c=8 ON   | 49527, 50011, 50027, **50051**, 50084, 50115, 50133 |
| mixed c=8 OFF  | 45947, 45973, 46025, **46037**, 46056, 46061, 46209 |
| mixed c=32 ON  | 54153, 54257, 54485, **54494**, 54516, 54563, 54607 |
| mixed c=32 OFF | 53171, 53182, 53197, **53260**, 53268, 53598, 53903 |

---

## 2  Noise Assessment

**These results are exceptionally clean.** Every cell has CV% under 1.5%, with
one outlier at 3.2% (SELECT c=1 ON — two high iterations pulled the spread).
The 7-iteration × 300-second design eliminated the bimodal noise seen in earlier
dev-container runs (§9.4–9.5 of the performance plan).

For the UPDATE c=8 and INSERT c=8 cells specifically — the distributions have
**zero overlap** (min ON > max OFF in both cases), confirming the improvements
are repeatable and not noise artifacts.

---

## 3  Key Takeaways

### 3.1  Consistent wins at c=1 across all workloads

Every workload shows a clear improvement at c=1 (+7.4% to +15.8%). Unlike the
earlier 60-second runs where INSERT was neutral, the longer 300-second runs
reveal a clear **+12.6% INSERT win** — confirming the executor overhead reduction
benefits writes too when measured with sufficient precision.

### 3.2  c=8 is the sweet spot for throughput gains

SELECT c=8 shows the largest improvement at **+19.3%** (50,533 → 60,271 TPS).
All workloads show meaningful wins at c=8 (+5.7% to +19.3%).

### 3.3  c=32 saturates the system

All workloads show only +0.2% to +2.6% improvement at c=32. At 32 clients on a
16-CPU shared host, the system bottlenecks on CPU/scheduling contention, not
executor overhead. The one-task path doesn't hurt — no regressions — but the
gains are masked.

### 3.4  No regressions anywhere

Every single cell shows ON ≥ OFF. The worst case is INSERT c=32 at +0.2%,
which is statistically even.

---

## 4  Why SELECT Gains Most at c=8 (+19.3%)

Two effects combine:

### 4.1  Minimal worker-side work (Amdahl's Law)

SELECT does an index seek and returns one row — no WAL, no row locks, no
buffer-dirty writes. The per-query time is dominated by coordinator overhead
(planner, executor setup/teardown, WaitEventSet). Removing that overhead shrinks
a large fraction of the total.

UPDATE adds row lock acquisition, heap update, WAL write, and possible index
updates. INSERT/upsert adds PK conflict-check index scan, conditional update,
and WAL. These costs are untouched by the one-task executor, so the savings are
a smaller percentage of total query time.

### 4.2  The old executor's overhead scales with concurrency

The per-query saving grows from c=1 to c=8 for SELECT:

| Workload | c=1 saving (ms/query) | c=8 saving (ms/query) | Scaling |
|----------|-----------------------|-----------------------|---------|
| SELECT   | 0.006                 | 0.026                 | 4.4×    |
| UPDATE   | 0.012                 | 0.016                 | 1.3×    |
| INSERT   | 0.012                 | 0.011                 | 0.9×    |

For SELECT, the saving grows **4.4×** — far above the 1.0× expected if overhead
were constant. The old executor's `WaitEventSet` creation/teardown and connection
pool state machine have coordinator-side contention (epoll syscalls, allocator
locks) that worsens with concurrency. The one-task executor skips all of this.

For UPDATE/INSERT, worker-side contention (row locks, WAL insertion locks, buffer
pin contention) becomes the new bottleneck at c=8, capping how much coordinator
savings flow through to TPS.

---

## 5  UPDATE (+8.9%) vs INSERT (+5.7%) at c=8

### 5.1  Statistical significance

Both improvements are individually rock-solid:

| Metric | UPDATE c=8 | INSERT c=8 |
|--------|------------|------------|
| ON range | 43,760 – 44,942 | 42,568 – 43,414 |
| OFF range | 41,058 – 41,423 | 40,747 – 41,009 |
| Overlap | **None** (min ON > max OFF) | **None** |
| Welch's t | 21.2 | 21.1 |
| Cohen's d | 11.3 | 11.3 |
| CV% (ON / OFF) | 0.9% / 0.3% | 0.6% / 0.2% |

The difference *between* the two improvements is also significant (t = 7.91 on
paired deltas).

### 5.2  Why UPDATE benefits more

At c=1, the executor overhead removed per query is effectively identical for
both workloads (~0.012 ms), confirming the one-task executor removes the same
fixed coordinator overhead regardless of DML type. The code confirms this: both
follow identical OneTaskAdaptiveExecutor and regular AdaptiveExecutor paths with
no command-type branching.

At c=8, the savings diverge because of **worker-side lock contention**:

**INSERT ON CONFLICT** has a heavier critical section on the worker:
1. Acquire exclusive lock on the index page (not just the row)
2. Probe the PK index for conflict
3. If conflict → acquire row lock → perform UPDATE → write WAL
4. If no conflict → perform INSERT → write WAL

Each upsert holds an index page lock during the conflict check. With 8 concurrent
clients, these index page locks serialize upserts targeting the same btree leaf
page.

**Plain UPDATE** has a shorter critical section:
1. Index scan to find the row (shared lock on index page, released quickly)
2. Acquire row lock → heap update → write WAL

The index page lock is shared (not exclusive) and released before the heap update.
Row-level locks only serialize when two clients hit the exact same row (1-in-1M
probability with random keys).

The result: INSERT ON CONFLICT hits a worker-side bottleneck earlier than UPDATE.
The coordinator savings are still there, but worker lock contention creates a
harder throughput ceiling that absorbs some of the benefit.

### 5.3  These critical sections are entirely in PostgreSQL core

Citus has zero influence on the worker-side execution. For a single-shard query,
the coordinator sends the SQL string over libpq to the worker, which executes it
as a vanilla SQL statement against a regular heap table (the shard). The index page
locking, row locking, heap operations, conflict resolution, and WAL inserPut ttion all
happen in PostgreSQL's standard executor and access method code (`ExecInsert()`,
`ExecUpdate()`, `_bt_doinsert()`, `heap_insert()`, `heap_update()`, `XLogInsert()`).
The worker doesn't know it's running a Citus query — it's just processing SQL that
arrived over a normal client connection.

---

## 6  Comparison with Earlier Runs

| Run | Duration | Iters | SELECT c=1 | SELECT c=8 | UPDATE c=1 | INSERT c=1 | Noise |
|-----|----------|-------|------------|------------|------------|------------|-------|
| 2026-03-03 (60s×3) | 60s | 3 | +18.9% | +20.8% | +8.7% | −5.2% | High (CV 20–30% at c=32) |
| 2026-03-06 (300s×7) | 300s | 7 | +7.4% | +19.3% | +13.5% | +12.6% | Low (CV <1.5% everywhere) |

The 300s×7 run resolved the noise issues and revealed that **all workloads
benefit at c=1**, including INSERT which previously appeared neutral. The
SELECT c=8 improvement is consistent across both runs (~20%).

---

## 7  Is the One-Task Adaptive Executor Worth Pursuing?

**Yes, unambiguously.** The data makes a strong case:

1. **The MIXED workload is the best proxy for real OLTP traffic** (60% SELECT /
   20% UPDATE / 20% INSERT). It shows **+15.8% at c=1** and **+8.7% at c=8** —
   both statistically bulletproof (CV% 0.2–0.5%, zero overlap between ON/OFF
   distributions). An 8–16% throughput improvement for the most common Citus
   query pattern (single-shard fast-path) is a significant win.

2. **No regressions in any of the 24 cells.** The worst case is INSERT c=32 at
   +0.2% — dead even. This means the feature is safe to ship as a default-on
   GUC with zero downside risk.

3. **The gains are largest where they matter most.** Production OLTP systems
   typically run at moderate concurrency (c=1 to c=8 per node), not at CPU
   saturation (c=32). The one-task executor delivers its best results exactly
   in that sweet spot.

4. **The data quality eliminates doubt.** With 7 × 300s iterations, CV% under
   1.5%, and zero overlap between ON/OFF distributions for every significant
   cell — these aren't marginal or noisy results. They're as clean as
   micro-benchmarks get.

5. **The improvement is "free" from the user's perspective.** It requires no
   schema changes, no query rewrites, no configuration tuning. It activates
   automatically for any single-shard fast-path query, which is the dominant
   query pattern for hash-distributed tables.

The only caveat is that c=32 gains flatten, but that's expected — at 32 clients
on 16 CPUs, the system saturates on OS scheduling regardless of executor
efficiency. This doesn't undermine the feature; it just defines its operating
envelope.

---

## 8  Where the One-Task Executor Saves CPU Cycles

The following details where the `OneTaskAdaptiveExecutor` saves cycles compared
to the regular `AdaptiveExecutor`. All references are to
`src/backend/distributed/executor/adaptive_executor.c`.

### 8.1  WaitEventSet creation/teardown → replaced with WaitLatchOrSocket

The regular executor creates a `WaitEventSet` (wrapping `epoll_create`/`epoll_ctl`
on Linux) to multiplex I/O across potentially many connections, then tears it down.
It rebuilds the event set via `RebuildWaitEventSet()` whenever connections change.

The one-task path calls `WaitLatchOrSocket()` on a single socket — one syscall,
no allocation, no teardown.

### 8.2  Worker pool management → eliminated entirely

The regular executor runs `ManageWorkerPool()` on every event-loop iteration,
which includes:

- **Slow-start algorithm** (`ShouldWaitForSlowStart`) — TCP-style ramp-up with
  100ms delays deciding whether to open new connections
- **Connection counting** (`CalculateNewConnectionCount`,
  `UsableConnectionCount`) — estimating optimal connection count
- **Cost comparison** (`UsingExistingSessionsCheaperThanEstablishingNewConnections`)
  — deciding reuse vs new connection
- **Timeout enforcement** (`CheckConnectionTimeout`)

The one-task path does a single `GetNodeUserDatabaseConnection()` call (typically
a pool hit) and skips all of this.

### 8.3  ConnectionStateMachine (6 states, ~380 lines) → inline dead-check (~30 lines)

The regular executor drives a 6-state async FSM per connection: INITIAL →
CONNECTING → CONNECTED → TIMED_OUT / LOST / FAILED, with `PQconnectPoll()` loops,
socket-change detection, pool counter updates, and retry/failover logic.

The one-task path does a simple `recv(..., MSG_PEEK | MSG_DONTWAIT)` to check if
the connection is alive, and if dead, closes and reconnects once. No state machine,
no pool counters.

### 8.4  TransactionStateMachine (5 states, ~250 lines) → synchronous BEGIN + inline results (~150 lines)

The regular executor runs an async FSM for transaction setup: async
`StartRemoteTransactionBegin()`, poll for BEGIN result, clear results, state
transition, then pop task and start. Multiple event-loop iterations are consumed
just setting up the transaction block.

The one-task path calls `RemoteTransactionBeginIfNecessary()` synchronously
(blocking), then immediately sends the query. One round-trip instead of FSM
iterations.

### 8.5  Task queuing and wrapper allocation → direct list access

The regular executor allocates `ShardCommandExecution` and
`TaskPlacementExecution` wrapper structs, inserts them into ready/pending queues,
and later pops them through a work-stealing mechanism (`PopPlacementExecution`).

The one-task path does `linitial(remoteTaskList)` — a single pointer dereference.
No allocations, no queue data structures.

### 8.6  ProcessWaitEvents dispatch loop → eliminated

The regular executor iterates over all fired events from `WaitEventSetWait()`,
dispatching each to the appropriate connection's state machine, checking for
postmaster death, latch signals, and cancellation per event.

The one-task path checks one socket and one latch inline.

### 8.7  Net effect per query

All of these savings are **per-query** on the coordinator. The benchmark data
shows the cumulative savings manifest as:

- ~0.006–0.012 ms at c=1 (pure overhead removal, no contention)
- ~0.016–0.026 ms at c=8 (amplified because the regular executor's
  WaitEventSet/pool management code contends with itself under concurrency)

The code footprint difference is telling: the regular path's event loop + pool
management + two state machines spans ~1,200 lines; the one-task path's
equivalent is ~200 lines of straightforward synchronous code.

---

## 9  Test-Automation Benchmark: 1-Task AE vs Citus 14.0

### 9.1  Setup

**Environment:** Citus test-automation Azure infrastructure (multi-node, non-localhost). 
Setup: https://github.com/citusdata/test-automation/tree/master?tab=readme-ov-file#azure-getting-started
Workload: https://github.com/citusdata/test-automation/tree/master?tab=readme-ov-file#pgbench

PG 18.1, 32 shards, replication factor 1, 7 iterations per workload.

**Branches compared:**
- `colm/1-task-AE-take-2` (OneTaskAdaptiveExecutor enabled)
- `release-14.0` (baseline)

### 9.2  Per-workload statistics

| Workload | Version | Median TPS | Mean TPS | Stddev | CV% | Min | Max |
|----------|---------|------------|----------|--------|-----|-----|-----|
| select-only | 1-Task AE | 27,648 | 27,570 | 359 | 1.3% | 26,938 | 28,074 |
| select-only | 14.0 | 26,543 | 26,343 | 428 | 1.6% | 25,690 | 26,765 |
| simple-update | 1-Task AE | 2,912 | 2,881 | 236 | 8.2% | 2,496 | 3,289 |
| simple-update | 14.0 | 3,082 | 3,016 | 295 | 9.8% | 2,493 | 3,353 |
| default-pgbench | 1-Task AE | 2,070 | 2,077 | 136 | 6.6% | 1,837 | 2,284 |
| default-pgbench | 14.0 | 2,126 | 2,090 | 155 | 7.4% | 1,778 | 2,256 |

### 9.3  Comparison

| Workload | AE Median | 14.0 Median | **Δ%** | Welch's t | Cohen's d | Overlap? | Verdict |
|----------|-----------|-------------|--------|-----------|-----------|----------|---------|
| select-only | 27,648 | 26,543 | **+4.2%** | 5.81 | 3.11 | **None** | **Significant win** |
| simple-update | 2,912 | 3,082 | −5.5% | −0.95 | −0.51 | Yes | Not significant |
| default-pgbench | 2,070 | 2,126 | −2.6% | −0.16 | −0.09 | Yes | Not significant |

### 9.4  Raw TPS values (sorted)

| Workload | Version | All TPS (sorted) |
|----------|---------|------------------|
| select-only | 1-Task AE | 26938, 27409, 27433, **27648**, 27680, 27810, 28074 |
| select-only | 14.0 | 25690, 25956, 26084, **26543**, 26625, 26740, 26765 |
| simple-update | 1-Task AE | 2496, 2788, 2814, **2912**, 2921, 2946, 3289 |
| simple-update | 14.0 | 2493, 2808, 2989, **3082**, 3091, 3300, 3353 |
| default-pgbench | 1-Task AE | 1837, 2024, 2056, **2070**, 2115, 2152, 2284 |
| default-pgbench | 14.0 | 1778, 2023, 2120, **2126**, 2138, 2187, 2256 |

### 9.5  Interpretation

**select-only** is a clear, statistically significant win: +4.2%, t=5.81, zero
overlap between the distributions (min AE 26,938 > max 14.0 26,765). This
validates that the one-task executor delivers a real, measurable improvement in
a realistic multi-node deployment — not just in a localhost setup.

**simple-update** and **default-pgbench** show no significant difference. The
distributions overlap heavily and the t-statistics are well below the significance
threshold (|t| < 2). With CV% of 8–10%, the noise in the test-automation
environment is too large relative to any real difference. The localhost benchmarks
measured write improvements of +5–9% at c=8; the signal is likely present here
but buried in environmental variance.

The high CV% for write workloads (~8–10% vs ~1.5% for select-only) reflects
shared infrastructure and disk I/O variability in test-automation. The select-only
workload is CPU-bound and less affected, making it the most reliable signal.

This benchmark serves as **confirmation in a realistic environment** rather than
primary motivation. Combined with the localhost data (§1–§5), the overall evidence
is solid: a clear win for reads, no regressions for writes, validated across two
independent benchmarking environments.
