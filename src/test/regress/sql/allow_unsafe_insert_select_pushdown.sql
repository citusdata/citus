--
-- ALLOW_UNSAFE_INSERT_SELECT_PUSHDOWN
--
-- Tests citus.allow_unsafe_insert_select_pushdown, which lets a colocated
-- INSERT .. SELECT push GROUP BY / window / DISTINCT batching and a batch UDF
-- down to the shards instead of pulling rows to the coordinator.
--
-- The distribution column of the target must still come from the source
-- distribution column unchanged: either as a plain Var, or as the provably
-- shard-local batch pass-through unnest(array_agg(dist_col)). Any other derived
-- distribution value (an arithmetic/function transform, or a transform wrapped
-- inside the array_agg) is rejected even with the GUC enabled, because it could
-- route a row to a different shard.
--
CREATE SCHEMA allow_unsafe_insert_select_pushdown;
SET search_path = allow_unsafe_insert_select_pushdown;
SET citus.next_shard_id TO 14000000;
SET citus.shard_count = 4;
SET citus.shard_replication_factor = 1;

CREATE TABLE dist(text_id int, text_col text);
CREATE TABLE res(text_id int, val int);
SELECT create_distributed_table('dist', 'text_id');
SELECT create_distributed_table('res', 'text_id');

INSERT INTO dist SELECT g, 't' || g FROM generate_series(1, 500) g;

-- a batched UDF: returns one value per input, mimicking a batched API call.
-- immutable + parallel safe, like a real batch UDF.
CREATE FUNCTION batch_transform(t text[]) RETURNS int[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$ SELECT array_agg(length(x)) FROM unnest(t) x $$;
SELECT create_distributed_function('batch_transform(text[])');

-- default off: batching is done after pulling rows to the coordinator
-- (explain_filter strips the PG18-only "Window:" line so the plan is
--  comparable across supported Postgres versions)
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT b, unnest(array_agg(text_id)) id,
            unnest(batch_transform(array_agg(text_col))) val
  FROM (SELECT text_id, text_col, (row_number() OVER () - 1) / 100 b FROM dist) q
  GROUP BY b
) s
$$, true);

SET citus.allow_unsafe_insert_select_pushdown TO on;

-- now the batching and UDF call run on the shards
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT b, unnest(array_agg(text_id)) id,
            unnest(batch_transform(array_agg(text_col))) val
  FROM (SELECT text_id, text_col, (row_number() OVER () - 1) / 100 b FROM dist) q
  GROUP BY b
) s
$$, true);

INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT b, unnest(array_agg(text_id)) id,
            unnest(batch_transform(array_agg(text_col))) val
  FROM (SELECT text_id, text_col, (row_number() OVER () - 1) / 100 b FROM dist) q
  GROUP BY b
) s;

-- every text_id should be matched to the right value
SELECT count(*), count(*) FILTER (WHERE val = length('t' || text_id)) AS ok
FROM res JOIN dist USING (text_id);

-- ---------------------------------------------------------------------
-- Positive per-branch coverage. Each construct below used to force a
-- coordinator merge (or was rejected outright). With the GUC enabled the whole
-- colocated INSERT .. SELECT is pushed to the shards because the distribution
-- column is either a plain partition-column Var or the unnest(array_agg(text_id))
-- batch pass-through. The pushed-down plan is a Custom Scan (Citus Adaptive)
-- whose task runs the INSERT on a shard, with no Distributed Subplan /
-- intermediate results. explain_filter keeps the plan comparable across
-- Postgres versions.
-- ---------------------------------------------------------------------

-- the batched benchmark shape: bucket rows into fixed-size batches with
-- row_number()/batch_size, array_agg each batch (id and text in the same
-- order), call the batch UDF once per batch, then unnest back to one row per id.
-- This is the query the GUC is meant to push down to the shards.
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT
    unnest(array_agg(text_id ORDER BY text_id)) id,
    unnest(batch_transform(array_agg(text_col ORDER BY text_id))) val
  FROM (
    SELECT text_id, text_col, (row_number() OVER () - 1) / 100 batch FROM dist
  ) q
  GROUP BY batch
) s
$$, true);

-- branch: GROUP BY on a non-distribution column, distribution column projected
-- as the unnest(array_agg(text_id)) batch pass-through
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT unnest(array_agg(text_id)), unnest(batch_transform(array_agg(text_col)))
FROM dist GROUP BY text_id % 10
$$, true);

-- branch: aggregates without GROUP BY
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT unnest(array_agg(text_id)), unnest(batch_transform(array_agg(text_col))) FROM dist
$$, true);

-- branch: HAVING without GROUP BY on the distribution column
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT unnest(array_agg(text_id)), unnest(batch_transform(array_agg(text_col)))
FROM dist HAVING count(*) > 0
$$, true);

-- branch: window function not partitioned on the distribution column, with the
-- distribution column projected as a plain partition-column Var
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT text_id, row_number() OVER (ORDER BY text_col) FROM dist
$$, true);

-- combination: GROUP BY + DISTINCT, distribution column as the batch pass-through
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT DISTINCT unnest(array_agg(text_id)), unnest(array_agg(length(text_col)))
FROM dist GROUP BY text_id % 10
$$, true);

-- combination: window + GROUP BY, relaxed constructs living in nested subqueries
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT id, v FROM (
  SELECT unnest(array_agg(text_id)) id, unnest(batch_transform(array_agg(text_col))) v
  FROM (SELECT text_id, text_col, row_number() OVER (ORDER BY text_col) rn FROM dist) q
  GROUP BY rn % 5
) s
$$, true);

-- ---------------------------------------------------------------------
-- Executed coverage for the relaxed positive branches: the plans above only
-- assert pushdown; run two of them for real to confirm the shard-local batching
-- keeps each row's value matched to its own distribution key.
-- ---------------------------------------------------------------------
SET citus.allow_unsafe_insert_select_pushdown TO on;

-- aggregate without GROUP BY: each shard aggregates all its rows into one group
-- and the unnest zip-back keeps text_id matched to length(text_col)
TRUNCATE res;
INSERT INTO res(text_id, val)
SELECT unnest(array_agg(text_id)), unnest(batch_transform(array_agg(text_col))) FROM dist;
SELECT count(*), count(*) FILTER (WHERE val = length('t' || text_id)) AS ok FROM res;

-- window not partitioned on the distribution column: the distribution column is
-- a plain Var, so every text_id is routed to its own shard exactly once
TRUNCATE res;
INSERT INTO res(text_id, val)
SELECT text_id, row_number() OVER (ORDER BY text_col) FROM dist;
SELECT count(*), count(DISTINCT text_id) AS distinct_ids,
       count(*) FILTER (WHERE text_id IS NULL) AS null_keys FROM res;

-- ---------------------------------------------------------------------
-- Negative coverage: pattern requirement. With the GUC enabled the batching
-- relaxations still fire, but the distribution column is a *transformed* value
-- rather than the source partition column, so the query is not pushed down and
-- falls back to a coordinator merge -- identical to the GUC-disabled plan.
-- ---------------------------------------------------------------------

-- distribution column derived by arithmetic on the partition column
SET citus.allow_unsafe_insert_select_pushdown TO off;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT text_id + 0, length(text_col) FROM dist
$$, true);
SET citus.allow_unsafe_insert_select_pushdown TO on;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT text_id + 0, length(text_col) FROM dist
$$, true);

-- distribution column derived from a non-distribution column (DISTINCT)
SET citus.allow_unsafe_insert_select_pushdown TO off;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT DISTINCT length(text_col), text_id % 7 FROM dist
$$, true);
SET citus.allow_unsafe_insert_select_pushdown TO on;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT DISTINCT length(text_col), text_id % 7 FROM dist
$$, true);

-- distribution column derived from a non-distribution column, one subquery down
SET citus.allow_unsafe_insert_select_pushdown TO off;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT k, k FROM (
  SELECT DISTINCT length(text_col) k FROM dist
) s
$$, true);
SET citus.allow_unsafe_insert_select_pushdown TO on;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT k, k FROM (
  SELECT DISTINCT length(text_col) k FROM dist
) s
$$, true);

-- the batched benchmark shape, but with the distribution column transformed
-- *inside* array_agg (unnest(array_agg(text_id + 1))): the batch pass-through no
-- longer carries the untransformed partition column, so the same shape that
-- pushes down above is now rejected and falls back to a coordinator merge
SET citus.allow_unsafe_insert_select_pushdown TO off;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT unnest(array_agg(text_id + 1)) id,
         unnest(batch_transform(array_agg(text_col))) val
  FROM (SELECT text_id, text_col, (row_number() OVER () - 1) / 100 b FROM dist) q
  GROUP BY b
) s
$$, true);
SET citus.allow_unsafe_insert_select_pushdown TO on;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT unnest(array_agg(text_id + 1)) id,
         unnest(batch_transform(array_agg(text_col))) val
  FROM (SELECT text_id, text_col, (row_number() OVER () - 1) / 100 b FROM dist) q
  GROUP BY b
) s
$$, true);

-- the batched benchmark shape, but with a transform wrapping array_agg for the
-- distribution column (unnest(batch_transform(array_agg(text_col)))): the unnest
-- argument must be a bare array_agg of the partition column, so this is rejected
-- too and falls back to a coordinator merge
SET citus.allow_unsafe_insert_select_pushdown TO off;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT unnest(batch_transform(array_agg(text_col))) id,
         unnest(array_agg(text_id)) val
  FROM (SELECT text_id, text_col, (row_number() OVER () - 1) / 100 b FROM dist) q
  GROUP BY b
) s
$$, true);
SET citus.allow_unsafe_insert_select_pushdown TO on;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT unnest(batch_transform(array_agg(text_col))) id,
         unnest(array_agg(text_id)) val
  FROM (SELECT text_id, text_col, (row_number() OVER () - 1) / 100 b FROM dist) q
  GROUP BY b
) s
$$, true);

-- ---------------------------------------------------------------------
-- Runtime behavior of the rejected transform shapes.
--
-- Both transform shapes rejected above keep the set-returning unnest() inside
-- a subquery, so they fall back to a coordinator merge and still execute
-- correctly (each id stays matched to its own value).
-- ---------------------------------------------------------------------
SET citus.allow_unsafe_insert_select_pushdown TO on;
TRUNCATE res;
INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT unnest(array_agg(text_id + 1)) id,
         unnest(batch_transform(array_agg(text_col))) val
  FROM (SELECT text_id, text_col, (row_number() OVER () - 1) / 100 b FROM dist) q
  GROUP BY b
) s;
-- id = text_id + 1 and val = length('t' || text_id), so val = length('t' || (id - 1))
SELECT count(*), count(*) FILTER (WHERE val = length('t' || (text_id - 1))) AS ok
FROM res;

-- ---------------------------------------------------------------------
-- Pre-existing limitation, NOT specific to this GUC: a set-returning function
-- left in the *top-level* target list of a non-pushed-down INSERT .. SELECT
-- (flat, or fed from a CTE) surfaces PostgreSQL's opaque "set-valued function
-- called in context that cannot accept a set" error. The Citus coordinator
-- INSERT .. SELECT path does not replicate PostgreSQL's split_pathtarget_at_srfs()
-- (ProjectSet insertion), so the unnest() is left where PostgreSQL's executor
-- cannot evaluate it. This reproduces with the GUC off, and on a plain
-- distributed SELECT too. Tracked in
-- https://github.com/citusdata/citus/issues/2265; pinned here so a future fix
-- (a clearer error, or actual support) is caught.
-- ---------------------------------------------------------------------

-- flat set-returning function in the top-level target list, GUC on
SET citus.allow_unsafe_insert_select_pushdown TO on;
INSERT INTO res(text_id, val)
SELECT unnest(array_agg(text_id + 1)), unnest(batch_transform(array_agg(text_col)))
FROM dist GROUP BY text_id % 10;

-- same shape with the GUC off: the error is pre-existing, not caused by the GUC
SET citus.allow_unsafe_insert_select_pushdown TO off;
INSERT INTO res(text_id, val)
SELECT unnest(array_agg(text_id + 1)), unnest(batch_transform(array_agg(text_col)))
FROM dist GROUP BY text_id % 10;

-- a CTE-wrapped batch source hits the same limitation
WITH b AS (SELECT text_id, text_col FROM dist)
INSERT INTO res(text_id, val)
SELECT unnest(array_agg(text_id)), unnest(batch_transform(array_agg(text_col)))
FROM b GROUP BY text_id % 10;

-- not specific to INSERT .. SELECT: the same set-returning function over an
-- aggregate errors in a plain distributed SELECT as well (#2265)
SELECT unnest(array_agg(text_id)) FROM dist;

-- ---------------------------------------------------------------------
-- Negative coverage: FILTER / DISTINCT on the batch pass-through array_agg.
-- The distribution-column pass-through unnest(array_agg(text_id)) is only
-- safe when the array_agg emits exactly one element per group row. A FILTER
-- or DISTINCT clause on that array_agg can emit fewer elements, so when a
-- sibling set-returning column is longer PostgreSQL's ProjectSet NULL-pads
-- the shorter distribution-column set -- silently producing NULL (mis-routed)
-- partition keys. Such an array_agg is therefore rejected even with the GUC
-- enabled and falls back to a coordinator merge, which re-routes each row and
-- enforces the not-NULL partition-column invariant. ORDER BY inside array_agg
-- only reorders (never drops) elements and stays pushed down (covered above).
-- ---------------------------------------------------------------------

-- FILTER on the distribution-column array_agg: not pushed down
SET citus.allow_unsafe_insert_select_pushdown TO off;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT unnest(array_agg(text_id) FILTER (WHERE text_id % 2 = 0)) id,
         unnest(batch_transform(array_agg(text_col))) val
  FROM (SELECT text_id, text_col, (row_number() OVER () - 1) / 100 b FROM dist) q
  GROUP BY b
) s
$$, true);
SET citus.allow_unsafe_insert_select_pushdown TO on;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT unnest(array_agg(text_id) FILTER (WHERE text_id % 2 = 0)) id,
         unnest(batch_transform(array_agg(text_col))) val
  FROM (SELECT text_id, text_col, (row_number() OVER () - 1) / 100 b FROM dist) q
  GROUP BY b
) s
$$, true);

-- DISTINCT on the distribution-column array_agg: not pushed down
SET citus.allow_unsafe_insert_select_pushdown TO off;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT unnest(array_agg(DISTINCT text_id)) id,
         unnest(batch_transform(array_agg(text_col))) val
  FROM (SELECT text_id, text_col, (row_number() OVER () - 1) / 100 b FROM dist) q
  GROUP BY b
) s
$$, true);
SET citus.allow_unsafe_insert_select_pushdown TO on;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT unnest(array_agg(DISTINCT text_id)) id,
         unnest(batch_transform(array_agg(text_col))) val
  FROM (SELECT text_id, text_col, (row_number() OVER () - 1) / 100 b FROM dist) q
  GROUP BY b
) s
$$, true);

-- the FILTER shape rejected above, run for real under the GUC: the coordinator
-- fallback re-routes each row and raises the not-NULL partition-column error
-- instead of silently inserting NULL (mis-routed) partition keys
SET citus.allow_unsafe_insert_select_pushdown TO on;
TRUNCATE res;
INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT unnest(array_agg(text_id) FILTER (WHERE text_id % 2 = 0)) id,
         unnest(batch_transform(array_agg(text_col))) val
  FROM (SELECT text_id, text_col, (row_number() OVER () - 1) / 100 b FROM dist) q
  GROUP BY b
) s;
SELECT count(*) AS rows_with_null_key FROM res WHERE text_id IS NULL;

-- ---------------------------------------------------------------------
-- Correctness of the pushed-down batches.
-- ---------------------------------------------------------------------

-- correctness for a GROUP BY batch: per-shard aggregation keeps each text_id
-- matched to its own value
TRUNCATE res;
INSERT INTO res(text_id, val)
SELECT unnest(array_agg(text_id)), unnest(batch_transform(array_agg(text_col)))
FROM dist GROUP BY text_id % 10;

SELECT count(*), count(*) FILTER (WHERE val = length('t' || text_id)) AS ok
FROM res JOIN dist USING (text_id);

-- correctness for the batched benchmark shape: every text_id keeps its own value
-- after batching, the UDF call, and the unnest zip-back
TRUNCATE res;
INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT
    unnest(array_agg(text_id ORDER BY text_id)) id,
    unnest(batch_transform(array_agg(text_col ORDER BY text_id))) val
  FROM (
    SELECT text_id, text_col, (row_number() OVER () - 1) / 100 batch FROM dist
  ) q
  GROUP BY batch
) s;

SELECT count(*), count(*) FILTER (WHERE val = length('t' || text_id)) AS ok
FROM res JOIN dist USING (text_id);

-- ---------------------------------------------------------------------
-- Runtime NULL-key drop: an over-emitting batch UDF returns more elements than
-- the distribution-column array_agg, so PostgreSQL's ProjectSet NULL-pads the
-- (shorter) distribution column. The pushed-down plan filters those rows out
-- (distribution column IS NOT NULL) so no NULL (mis-routed) partition key is
-- inserted, while every real text_id keeps its own value. Both shapes are
-- covered: the outer-subquery shape (distribution column is a plain Var,
-- filtered in place) and the flat shape (distribution column is the unnest
-- set-returning expression, filtered via a pass-through subquery wrapper).
-- ---------------------------------------------------------------------

-- a batch UDF that emits one extra element per batch (appends a trailing 0),
-- mimicking a batched API call whose output is longer than its input
CREATE FUNCTION batch_transform_over(t text[]) RETURNS int[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT (SELECT array_agg(length(x)) FROM unnest(t) x) || 0 $$;
SELECT create_distributed_function('batch_transform_over(text[])');

SET citus.allow_unsafe_insert_select_pushdown TO on;

-- outer-subquery shape: the distribution column is a plain Var, so the
-- IS NOT NULL filter is attached to the pushed-down SELECT directly
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT unnest(array_agg(text_id ORDER BY text_id)) id,
         unnest(batch_transform_over(array_agg(text_col ORDER BY text_id))) val
  FROM (SELECT text_id, text_col, (row_number() OVER () - 1) / 100 batch FROM dist) q
  GROUP BY batch
) s
$$, true);

TRUNCATE res;
INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT unnest(array_agg(text_id ORDER BY text_id)) id,
         unnest(batch_transform_over(array_agg(text_col ORDER BY text_id))) val
  FROM (SELECT text_id, text_col, (row_number() OVER () - 1) / 100 batch FROM dist) q
  GROUP BY batch
) s;
-- no NULL (mis-routed) key inserted; all 500 rows kept their value
SELECT count(*) FILTER (WHERE text_id IS NULL) AS rows_with_null_key,
       count(*) AS rows,
       count(*) FILTER (WHERE val = length('t' || text_id)) AS ok
FROM res;

-- flat shape: the distribution column is the bare unnest set-returning
-- expression, so the SELECT is wrapped in a pass-through subquery whose
-- IS NOT NULL filter drops the NULL-padded rows
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT unnest(array_agg(text_id ORDER BY text_id)),
       unnest(batch_transform_over(array_agg(text_col ORDER BY text_id)))
FROM dist GROUP BY text_id % 10
$$, true);

TRUNCATE res;
INSERT INTO res(text_id, val)
SELECT unnest(array_agg(text_id ORDER BY text_id)),
       unnest(batch_transform_over(array_agg(text_col ORDER BY text_id)))
FROM dist GROUP BY text_id % 10;
-- no NULL (mis-routed) key inserted; all 500 rows kept their value
SELECT count(*) FILTER (WHERE text_id IS NULL) AS rows_with_null_key,
       count(*) AS rows,
       count(*) FILTER (WHERE val = length('t' || text_id)) AS ok
FROM res;

-- ---------------------------------------------------------------------
-- Negative coverage: constructs the GUC never relaxes.
-- ---------------------------------------------------------------------

-- reference table target: the GUC does not apply. The plan is a coordinator
-- merge whether the GUC is disabled or enabled (identical), unlike the
-- distributed-target cases above which push down once the GUC is enabled.
CREATE TABLE ref(text_id int, val int);
SELECT create_reference_table('ref');

SET citus.allow_unsafe_insert_select_pushdown TO off;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO ref(text_id, val)
SELECT text_id % 10, count(*)::int FROM dist GROUP BY text_id % 10
$$, true);

SET citus.allow_unsafe_insert_select_pushdown TO on;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO ref(text_id, val)
SELECT text_id % 10, count(*)::int FROM dist GROUP BY text_id % 10
$$, true);

-- volatile functions: the GUC relaxes only grouping / partition-column matching,
-- never volatility. A volatile function in the SELECT is still not pushed to the
-- shards with the GUC enabled -- it falls back to a coordinator plan, identical
-- to the GUC-disabled case.
CREATE FUNCTION volatile_transform(t text) RETURNS int
LANGUAGE sql VOLATILE AS $$ SELECT length(t) $$;

SET citus.allow_unsafe_insert_select_pushdown TO off;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT text_id, volatile_transform(text_col) FROM dist
$$, true);

SET citus.allow_unsafe_insert_select_pushdown TO on;
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT text_id, volatile_transform(text_col) FROM dist
$$, true);

-- a LIMIT forces a coordinator merge even with the GUC enabled and an otherwise
-- pushdown-eligible plan (grouped on the distribution column); LIMIT/OFFSET are
-- never relaxed
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT text_id id, count(*)::int val FROM dist GROUP BY text_id LIMIT 100
) s
$$, true);

-- ... and likewise for OFFSET
SELECT public.explain_filter($$
EXPLAIN (COSTS OFF) INSERT INTO res(text_id, val)
SELECT id, val FROM (
  SELECT text_id id, count(*)::int val FROM dist GROUP BY text_id OFFSET 5
) s
$$, true);

-- ---------------------------------------------------------------------
-- Positive coverage: common INSERT .. SELECT wrappers keep routing correctly
-- with the GUC on -- RETURNING, ON CONFLICT DO UPDATE and a PREPARE/EXECUTE
-- generic plan.
-- ---------------------------------------------------------------------
SET citus.allow_unsafe_insert_select_pushdown TO on;

-- RETURNING the batched rows (consumed by a wrapping CTE so the output stays
-- deterministic): every returned row keeps its own value
TRUNCATE res;
WITH ins AS (
  INSERT INTO res(text_id, val)
  SELECT unnest(array_agg(text_id)), unnest(batch_transform(array_agg(text_col)))
  FROM dist GROUP BY text_id % 10
  RETURNING text_id, val
)
SELECT count(*), count(*) FILTER (WHERE val = length('t' || text_id)) AS ok FROM ins;

-- ON CONFLICT DO UPDATE on a primary-key target: the relaxed window shape (plain
-- distribution-column Var) routes every row to its own shard and updates it
CREATE TABLE res_pk(text_id int PRIMARY KEY, val int);
SELECT create_distributed_table('res_pk', 'text_id');
INSERT INTO res_pk SELECT g, -1 FROM generate_series(1, 500) g;
INSERT INTO res_pk(text_id, val)
SELECT text_id, (row_number() OVER (ORDER BY text_col))::int FROM dist
ON CONFLICT (text_id) DO UPDATE SET val = EXCLUDED.val;
SELECT count(*), count(DISTINCT text_id) AS distinct_ids,
       count(*) FILTER (WHERE val = -1) AS not_updated FROM res_pk;
DROP TABLE res_pk;

-- PREPARE / EXECUTE: the same batch shape keeps routing correctly under a
-- generic plan (>5 executions force the generic plan)
TRUNCATE res;
PREPARE batch_ins(int) AS
  INSERT INTO res(text_id, val)
  SELECT id, val FROM (
    SELECT unnest(array_agg(text_id)) id, unnest(batch_transform(array_agg(text_col))) val
    FROM dist WHERE text_id % 10 = $1 GROUP BY text_id % 10
  ) s;
EXECUTE batch_ins(0);
EXECUTE batch_ins(1);
EXECUTE batch_ins(2);
EXECUTE batch_ins(3);
EXECUTE batch_ins(4);
EXECUTE batch_ins(5);
EXECUTE batch_ins(6);
SELECT count(*), count(*) FILTER (WHERE val = length('t' || text_id)) AS ok FROM res;
DEALLOCATE batch_ins;

SET client_min_messages TO WARNING;
DROP SCHEMA allow_unsafe_insert_select_pushdown CASCADE;
