--
-- PG18
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 18 AS server_version_ge_18
\gset

-- test invalid statistics
-- behavior is same among PG versions, error message differs
-- relevant PG18 commit: 3eea4dc2c7, 38883916e
CREATE STATISTICS tst ON a FROM (VALUES (x)) AS foo;

CREATE FUNCTION tftest(int) returns table(a int, b int) as $$
SELECT $1, $1+i FROM generate_series(1,5) g(i);
$$ LANGUAGE sql IMMUTABLE STRICT;
CREATE STATISTICS alt_stat2 ON a FROM tftest(1);
DROP FUNCTION tftest;

\if :server_version_ge_18
\else
\q
\endif

-- PG18-specific tests go here.
--

-- Purpose: Verify PG18 behavior that NOT NULL constraints are materialized
--          as pg_constraint rows with contype = 'n' on both coordinator and
--          worker shards. Also confirm our helper view (table_checks) does
--          NOT surface NOT NULL entries.
-- https://github.com/postgres/postgres/commit/14e87ffa5c543b5f30ead7413084c25f7735039f

CREATE SCHEMA pg18_nn;
SET search_path TO pg18_nn;

-- Local control table
DROP TABLE IF EXISTS nn_local CASCADE;
CREATE TABLE nn_local(
    a int NOT NULL,
    b int,
    c text NOT NULL
);

-- Distributed table
DROP TABLE IF EXISTS nn_dist CASCADE;
CREATE TABLE nn_dist(
    a int NOT NULL,
    b int,
    c text NOT NULL
);

SELECT create_distributed_table('nn_dist', 'a');

-- Coordinator: count NOT NULL constraint rows
SELECT 'local_n_count' AS label, contype, count(*)
FROM pg_constraint
WHERE conrelid = 'pg18_nn.nn_local'::regclass
GROUP BY contype
ORDER BY contype;

SELECT 'dist_n_count' AS label, contype, count(*)
FROM pg_constraint
WHERE conrelid = 'pg18_nn.nn_dist'::regclass
GROUP BY contype
ORDER BY contype;

-- Our helper view should exclude NOT NULL
SELECT 'table_checks_local_count' AS label, count(*)
FROM public.table_checks
WHERE relid = 'pg18_nn.nn_local'::regclass;

SELECT 'table_checks_dist_count' AS label, count(*)
FROM public.table_checks
WHERE relid = 'pg18_nn.nn_dist'::regclass;

-- Add a real CHECK to ensure table_checks still reports real checks
ALTER TABLE nn_dist ADD CONSTRAINT nn_dist_check CHECK (b IS DISTINCT FROM 42);

SELECT 'table_checks_dist_with_real_check' AS label, count(*)
FROM public.table_checks
WHERE relid = 'pg18_nn.nn_dist'::regclass;

-- === Worker checks ===
\c - - - :worker_1_port
SET client_min_messages TO WARNING;
SET search_path TO pg18_nn;

-- Pick one heap shard of nn_dist in our schema
SELECT format('%I.%I', n.nspname, c.relname) AS shard_regclass
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'pg18_nn'
  AND c.relname LIKE 'nn_dist_%'
  AND c.relkind = 'r'
ORDER BY c.relname
LIMIT 1
\gset

-- Expect: 2 NOT NULL rows (a,c) + 1 CHECK row on the shard
SELECT 'worker_shard_n_count' AS label, contype, count(*)
FROM pg_constraint
WHERE conrelid = :'shard_regclass'::regclass
GROUP BY contype
ORDER BY contype;

-- table_checks on shard should hide NOT NULL
SELECT 'table_checks_worker_shard_count' AS label, count(*)
FROM public.table_checks
WHERE relid = :'shard_regclass'::regclass;

-- Drop one NOT NULL on coordinator; verify propagation
\c - - - :master_port
SET search_path TO pg18_nn;

ALTER TABLE nn_dist ALTER COLUMN c DROP NOT NULL;

-- Re-check on worker: NOT NULL count should drop to 1
\c - - - :worker_1_port
SET search_path TO pg18_nn;

SELECT 'worker_shard_n_after_drop' AS label, contype, count(*)
FROM pg_constraint
WHERE conrelid = :'shard_regclass'::regclass
GROUP BY contype
ORDER BY contype;

-- And on coordinator
\c - - - :master_port
SET search_path TO pg18_nn;

SELECT 'dist_n_after_drop' AS label, contype, count(*)
FROM pg_constraint
WHERE conrelid = 'pg18_nn.nn_dist'::regclass
GROUP BY contype
ORDER BY contype;

-- Purpose: test self join elimination for distributed, citus local and local tables.
--
CREATE TABLE sje_d1 (id bigserial PRIMARY KEY, name text, created_at timestamptz DEFAULT now());
CREATE TABLE sje_d2 (id bigserial PRIMARY KEY, name text, created_at timestamptz DEFAULT now());
CREATE TABLE sje_local (id bigserial PRIMARY KEY, title text);

SELECT create_distributed_table('sje_d1', 'id');
SELECT create_distributed_table('sje_d2', 'id');

INSERT INTO sje_d1 SELECT i,  i::text, now() FROM generate_series(0,100)i;
INSERT INTO sje_d2 SELECT i,  i::text, now() FROM generate_series(0,100)i;
INSERT INTO sje_local SELECT i,  i::text FROM generate_series(0,100)i;

-- Self-join elimination is applied when distributed tables are involved
-- The query plan has only one join
EXPLAIN (costs off)
select count(1) from sje_d1 INNER
JOIN sje_d2 u1 USING (id) INNER
JOIN sje_d2 u2 USING (id) INNER
JOIN sje_d2 u3 USING (id) INNER
JOIN sje_d2 u4 USING (id) INNER
JOIN sje_d2 u5 USING (id) INNER
JOIN sje_d2 u6 USING (id);

select count(1) from sje_d1 INNER
JOIN sje_d2 u1 USING (id) INNER
JOIN sje_d2 u2 USING (id) INNER
JOIN sje_d2 u3 USING (id) INNER
JOIN sje_d2 u4 USING (id) INNER
JOIN sje_d2 u5 USING (id) INNER
JOIN sje_d2 u6 USING (id);

-- Self-join elimination applied to from list join
EXPLAIN (costs off)
SELECT count(1) from sje_d1 d1, sje_d2 u1, sje_d2 u2, sje_d2 u3
WHERE d1.id = u1.id and u1.id = u2.id and u3.id = d1.id;

SELECT count(1) from sje_d1 d1, sje_d2 u1, sje_d2 u2, sje_d2 u3
WHERE d1.id = u1.id and u1.id = u2.id and u3.id = d1.id;

-- Self-join elimination is not applied when a local table is involved
-- This is a limitation that will be resolved in citus 14
EXPLAIN (costs off)
select count(1) from sje_d1 INNER
JOIN sje_local u1 USING (id) INNER
JOIN sje_local u2 USING (id) INNER
JOIN sje_local u3 USING (id) INNER
JOIN sje_local u4 USING (id) INNER
JOIN sje_local u5 USING (id) INNER
JOIN sje_local u6 USING (id);

select count(1) from sje_d1 INNER
JOIN sje_local u1 USING (id) INNER
JOIN sje_local u2 USING (id) INNER
JOIN sje_local u3 USING (id) INNER
JOIN sje_local u4 USING (id) INNER
JOIN sje_local u5 USING (id) INNER
JOIN sje_local u6 USING (id);


-- to test USING vs ON equivalence
EXPLAIN (costs off)
SELECT count(1)
FROM sje_d1 d
JOIN sje_d2 u1 ON (d.id = u1.id)
JOIN sje_d2 u2 ON (u1.id = u2.id);

SELECT count(1)
FROM sje_d1 d
JOIN sje_d2 u1 ON (d.id = u1.id)
JOIN sje_d2 u2 ON (u1.id = u2.id);

-- Null-introducing join can have SJE
EXPLAIN (costs off)
SELECT count(*)
FROM sje_d1 d
LEFT JOIN sje_d2 u1 USING (id)
LEFT JOIN sje_d2 u2 USING (id);

SELECT count(*)
FROM sje_d1 d
LEFT JOIN sje_d2 u1 USING (id)
LEFT JOIN sje_d2 u2 USING (id);

-- prepared statement
PREPARE sje_p(int,int) AS
SELECT count(1)
FROM sje_d1 d
JOIN sje_d2 u1 USING (id)
JOIN sje_d2 u2 USING (id)
WHERE d.id BETWEEN $1 AND $2;

EXPLAIN (costs off)
EXECUTE sje_p(10,20);

EXECUTE sje_p(10,20);

-- cte
EXPLAIN (costs off)
WITH z AS (SELECT id FROM sje_d2 WHERE id % 2 = 0)
SELECT count(1)
FROM sje_d1 d
JOIN z USING (id)
JOIN sje_d2 u2 USING (id);

WITH z AS (SELECT id FROM sje_d2 WHERE id % 2 = 0)
SELECT count(1)
FROM sje_d1 d
JOIN z USING (id)
JOIN sje_d2 u2 USING (id);

-- cleanup with minimum verbosity
SET client_min_messages TO ERROR;
RESET search_path;
DROP SCHEMA pg18_nn CASCADE;
RESET client_min_messages;
