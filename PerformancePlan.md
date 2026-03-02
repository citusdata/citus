# Performance Test Plan: One-Task Adaptive Executor

## Goal

Measure TPS improvement (if any) from bypassing the full adaptive executor machinery for single-shard queries.

## Setup

**Schema** (run once on coordinator):

```sql
CREATE TABLE bench_kv (
    key int PRIMARY KEY,
    value text
);
SELECT create_distributed_table('bench_kv', 'key');

-- Seed with 1M rows
INSERT INTO bench_kv SELECT i, md5(i::text) FROM generate_series(1, 1000000) i;
```

## Workloads

Create 3 pgbench custom scripts:

**1. Point SELECT** (`select.sql`):

```sql
\set key random(1, 1000000)
SELECT value FROM bench_kv WHERE key = :key;
```

**2. Point UPDATE** (`update.sql`):

```sql
\set key random(1, 1000000)
UPDATE bench_kv SET value = md5(:key::text) WHERE key = :key;
```

**3. Point INSERT (upsert)** (`upsert.sql`):

```sql
\set key random(1, 2000000)
INSERT INTO bench_kv (key, value) VALUES (:key, md5(:key::text))
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
```

## Protocol

For each workload, run **3 iterations** at each concurrency level, alternating GUC on/off:

```bash
# Warm up connections first
pgbench -h localhost -p 9700 -U citus -c $C -j $C -T 5 -f $SCRIPT citus

# --- Fast path ON (default) ---
for i in 1 2 3; do
  psql -h localhost -p 9700 -U citus -c "ALTER SYSTEM SET citus.enable_single_task_fast_path = on;" citus
  psql -h localhost -p 9700 -U citus -c "SELECT pg_reload_conf();" citus
  pgbench -h localhost -p 9700 -U citus -c $C -j $C -T 30 -P 5 -f $SCRIPT citus
done

# --- Fast path OFF ---
for i in 1 2 3; do
  psql -h localhost -p 9700 -U citus -c "ALTER SYSTEM SET citus.enable_single_task_fast_path = off;" citus
  psql -h localhost -p 9700 -U citus -c "SELECT pg_reload_conf();" citus
  pgbench -h localhost -p 9700 -U citus -c $C -j $C -T 30 -P 5 -f $SCRIPT citus
done
```

## Concurrency Levels

- `C=1` (serial latency — most sensitive to per-query overhead)
- `C=4`
- `C=16`

## What to Record

| Workload | Concurrency | GUC | Run | TPS (excl. conn) | Avg Latency (ms) |
|----------|-------------|-----|-----|-------------------|-------------------|
|          |             |     |     |                   |                   |

## Success Criteria

- **Meaningful**: >5% TPS improvement at `C=1` for point SELECTs
- **Worth pursuing**: >3% across multiple workloads
- **Not worth it**: <3% or within noise (variance between runs)

## Key Notes

- Use `-T 30` (30 seconds) minimum to smooth out noise
- `C=1` is the most important — it isolates per-query overhead without connection contention
- The dev container has coordinator + workers on localhost, so network latency is ~zero; this means the executor overhead is a larger fraction of total latency than in production (good for measuring the delta)
- Adjust port `9700` to match your coordinator port (`SHOW port;`)
