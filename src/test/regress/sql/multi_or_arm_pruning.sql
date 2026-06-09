--
-- MULTI_OR_ARM_PRUNING
--
-- Tests citus.enable_or_clause_arm_pruning: for a multi-shard SELECT whose
-- WHERE clause is a top-level OR where each arm constrains the distribution
-- column, each shard's task query should keep only the arms that can match on
-- that shard, instead of pushing the full N-arm OR to every shard.
--

CREATE SCHEMA or_arm_pruning;
SET search_path TO or_arm_pruning;

SET citus.next_shard_id TO 1630000;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

CREATE TABLE dt (k int, v int);
SELECT create_distributed_table('dt', 'k');
CREATE INDEX dt_k_v ON dt (k, v);
INSERT INTO dt SELECT k, v FROM generate_series(1, 8) k, generate_series(1, 50) v;
ANALYZE dt;

-- a colocated table for the join cases
CREATE TABLE dt2 (k int, y int);
SELECT create_distributed_table('dt2', 'k');
INSERT INTO dt2 SELECT k, y FROM generate_series(1, 8) k, generate_series(1, 50) y;
ANALYZE dt2;

-- ===================================================================
-- helpers (deterministic, port/shard-id independent)
-- ===================================================================

-- Returns the Filter lines of an EXPLAIN, with shard-id suffixes stripped and
-- sorted, so the output does not depend on worker ports, shard ids, or task
-- order. Forcing seq scans (see callers) keeps the plan node stable across PG
-- versions, so only the per-shard WHERE clause (the thing this feature changes)
-- shows up.
CREATE OR REPLACE FUNCTION shard_filters(cmd text)
RETURNS SETOF text LANGUAGE plpgsql AS $$
DECLARE line text;
BEGIN
  FOR line IN EXECUTE cmd LOOP
    IF position('Filter:' in line) > 0 THEN
      RETURN NEXT trim(both ' ' from regexp_replace(line, '_[0-9]+', '', 'g'));
    END IF;
  END LOOP;
  RETURN;
END;
$$;

-- Runs a query with the optimization off and on and reports whether the result
-- sets are identical (multiset equality). Independent of row order.
CREATE OR REPLACE FUNCTION parity(q text)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE off_only bigint; on_only bigint;
BEGIN
  SET LOCAL citus.enable_or_clause_arm_pruning TO off;
  EXECUTE 'CREATE TEMP TABLE _off AS ' || q;
  SET LOCAL citus.enable_or_clause_arm_pruning TO on;
  EXECUTE 'CREATE TEMP TABLE _on AS ' || q;
  EXECUTE 'SELECT count(*) FROM (TABLE _off EXCEPT ALL TABLE _on) z' INTO off_only;
  EXECUTE 'SELECT count(*) FROM (TABLE _on EXCEPT ALL TABLE _off) z' INTO on_only;
  DROP TABLE _off, _on;
  IF off_only = 0 AND on_only = 0 THEN
    RETURN 'parity ok';
  END IF;
  RETURN format('MISMATCH off_only=%s on_only=%s', off_only, on_only);
END;
$$;

-- ===================================================================
-- GUC defaults to on
-- ===================================================================
SHOW citus.enable_or_clause_arm_pruning;

-- ===================================================================
-- Per-shard WHERE clause: before vs after.
-- k = 1, 2, 3, 6 each hash to a different one of the 4 shards.
-- ===================================================================
SET enable_indexscan TO off;
SET enable_bitmapscan TO off;
SET citus.explain_all_tasks TO on;

-- BEFORE: every shard carries the full 4-arm OR
SET citus.enable_or_clause_arm_pruning TO off;
SELECT * FROM shard_filters($$
  EXPLAIN (COSTS OFF)
  SELECT * FROM dt
  WHERE (k=1 AND v=7) OR (k=2 AND v=7) OR (k=3 AND v=7) OR (k=6 AND v=7)
$$) ORDER BY 1;

-- AFTER: each shard keeps only the single arm whose k lives on that shard
SET citus.enable_or_clause_arm_pruning TO on;
SELECT * FROM shard_filters($$
  EXPLAIN (COSTS OFF)
  SELECT * FROM dt
  WHERE (k=1 AND v=7) OR (k=2 AND v=7) OR (k=3 AND v=7) OR (k=6 AND v=7)
$$) ORDER BY 1;

-- Range/inequality on the dist column does NOT prune on a hash-distributed
-- table (hashing is not order preserving), so a range arm is kept on every
-- shard. An equality arm sharing the OR is still pruned. The optimization is
-- on for both queries below.

-- pure range OR: nothing is pruned, every shard keeps the full OR
SELECT * FROM shard_filters($$
  EXPLAIN (COSTS OFF)
  SELECT * FROM dt WHERE (k <= 2 AND v=7) OR (k >= 7 AND v=7)
$$) ORDER BY 1;

-- mixed range + equality: the range arm is kept everywhere, while the equality
-- arm (k=5) is dropped on the shards that do not own k=5
SELECT * FROM shard_filters($$
  EXPLAIN (COSTS OFF)
  SELECT * FROM dt WHERE (k <= 4 AND v=7) OR (k=5 AND v=7)
$$) ORDER BY 1;

RESET enable_indexscan;
RESET enable_bitmapscan;

-- ===================================================================
-- Correctness: results are identical with the optimization off vs on
-- ===================================================================

-- basic: every arm pins the dist column
SELECT parity($$SELECT * FROM dt WHERE (k=1 AND v=7) OR (k=2 AND v=7) OR (k=3 AND v=7) OR (k=6 AND v=7)$$);

-- two arms collide on the same shard (both must be kept on that shard)
SELECT parity($$SELECT * FROM dt WHERE (k=1 AND v=7) OR (k=5 AND v=7)$$);

-- an arm with NO constraint on the dist column must be kept on all shards
SELECT parity($$SELECT * FROM dt WHERE (k=1 AND v=7) OR (v=7)$$);

-- mixed: top-level AND of a prunable OR with another predicate
SELECT parity($$SELECT * FROM dt WHERE v < 10 AND ((k=1 AND v=7) OR (k=2 AND v=7))$$);

-- nested OR inside an arm
SELECT parity($$SELECT * FROM dt WHERE (k=1 AND (v=7 OR v=8)) OR (k=2 AND v=7)$$);

-- non-equality (range) and IN constraints on the dist column
SELECT parity($$SELECT * FROM dt WHERE (k < 3 AND v=7) OR (k=5 AND v=7)$$);
SELECT parity($$SELECT * FROM dt WHERE (k IN (1,2) AND v=7) OR (k=5 AND v=7)$$);
-- pure range OR (no arm prunes on a hash table): exact no-op
SELECT parity($$SELECT * FROM dt WHERE (k <= 2 AND v=7) OR (k >= 7 AND v=7)$$);

-- colocated join, arms referencing both tables' dist keys
SELECT parity($$SELECT dt.v, dt2.y FROM dt JOIN dt2 USING (k) WHERE (dt.k=1 AND dt2.y=7) OR (dt.k=2 AND dt2.y=7)$$);

-- colocated join, cross-table arms
SELECT parity($$SELECT dt.v FROM dt JOIN dt2 USING (k) WHERE (dt.k=1 AND dt2.y=7) OR (dt2.k=2 AND dt.v=7)$$);

-- queries with nothing to prune are exact no-ops
SELECT parity($$SELECT * FROM dt WHERE v=7$$);
SELECT parity($$SELECT * FROM dt WHERE v=7 OR v=8$$);
SELECT parity($$SELECT * FROM dt WHERE (v=7 AND k=1) OR (v=8)$$);

-- join with a reference table: the reference fragment is not hash-distributed,
-- so it is skipped and all arms are kept; results must be unchanged.
CREATE TABLE ref (k int, w int);
SELECT create_reference_table('ref');
INSERT INTO ref SELECT g, g FROM generate_series(1, 8) g;
ANALYZE ref;
SELECT parity($$SELECT dt.v FROM dt JOIN ref ON dt.k = ref.k WHERE (dt.k=1 AND ref.w=1) OR (dt.k=2 AND ref.w=2)$$);

-- parameterized (prepared) query: a custom plan substitutes the params as
-- constants, so pruning still applies and must not change the result.
SET citus.enable_or_clause_arm_pruning TO off;
PREPARE or_prep(int, int) AS
  SELECT k, v FROM dt WHERE (k = $1 AND v = 7) OR (k = $2 AND v = 7);
CREATE TEMP TABLE prep_off AS EXECUTE or_prep(1, 2);
SET citus.enable_or_clause_arm_pruning TO on;
CREATE TEMP TABLE prep_on AS EXECUTE or_prep(1, 2);
SELECT count(*) AS prep_off_only FROM (TABLE prep_off EXCEPT ALL TABLE prep_on) z;
SELECT count(*) AS prep_on_only FROM (TABLE prep_on EXCEPT ALL TABLE prep_off) z;
DROP TABLE prep_off, prep_on;
DEALLOCATE or_prep;

-- ===================================================================
-- cleanup
-- ===================================================================
SET client_min_messages TO WARNING;
DROP SCHEMA or_arm_pruning CASCADE;
