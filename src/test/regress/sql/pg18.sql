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
CREATE STATISTICS alt_stat2 ON a FROM tftest(1);

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

-- cleanup
RESET client_min_messages;
RESET search_path;
DROP SCHEMA pg18_nn CASCADE;
