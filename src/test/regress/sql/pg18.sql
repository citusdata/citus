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

SET citus.next_shard_id TO 4754000;
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

-- PG18 Feature: JSON functionality - JSON_TABLE has COLUMNS clause for
-- extracting multiple fields from JSON documents.
-- PG18 commit: https://github.com/postgres/postgres/commit/bb766cd

CREATE TABLE pg18_json_test (id serial PRIMARY KEY, data JSON);
INSERT INTO pg18_json_test (data) VALUES
('{ "user": {"name": "Alice", "age": 30, "city": "San Diego"} }'),
('{ "user": {"name": "Bob", "age": 25, "city": "Los Angeles"} }'),
('{ "user": {"name": "Charlie", "age": 35, "city": "Los Angeles"} }'),
('{ "user": {"name": "Diana", "age": 28, "city": "Seattle"} } '),
('{ "user": {"name": "Evan", "age": 40, "city": "Portland"} } '),
('{ "user": {"name": "Ethan", "age": 32, "city": "Seattle"} } '),
('{ "user": {"name": "Fiona", "age": 27, "city": "Seattle"} } '),
('{ "user": {"name": "George", "age": 29, "city": "San Francisco"} } '),
('{ "user": {"name": "Hannah", "age": 33, "city": "Seattle"} } '),
('{ "user": {"name": "Ian", "age": 26, "city": "Portland"} } '),
('{ "user": {"name": "Jane", "age": 38, "city": "San Francisco"} } ');

SELECT jt.name, jt.age  FROM pg18_json_test, JSON_TABLE(
  data,
  '$.user'
  COLUMNS (
    age INT PATH '$.age',
    name TEXT PATH '$.name'
  )
) AS jt
WHERE jt.age between 25 and 35
ORDER BY jt.age, jt.name;

SELECT jt.city, count(1)  FROM pg18_json_test, JSON_TABLE(
  data,
  '$.user'
  COLUMNS (
    city TEXT PATH '$.city'
  )
) AS jt
GROUP BY jt.city
ORDER BY count(1) DESC;

-- Make it distributed and repeat the queries
SELECT create_distributed_table('pg18_json_test', 'id');

SELECT jt.name, jt.age  FROM pg18_json_test, JSON_TABLE(
  data,
  '$.user'
  COLUMNS (
    age INT PATH '$.age',
    name TEXT PATH '$.name'
  )
) AS jt
WHERE jt.age between 25 and 35
ORDER BY jt.age, jt.name;

SELECT jt.city, count(1)  FROM pg18_json_test, JSON_TABLE(
  data,
  '$.user'
  COLUMNS (
    city TEXT PATH '$.city'
  )
) AS jt
GROUP BY jt.city
ORDER BY count(1) DESC;


-- PG18 Feature: WITHOUT OVERLAPS can appear in PRIMARY KEY and UNIQUE constraints.
-- PG18 commit: https://github.com/postgres/postgres/commit/fc0438b4e

CREATE TABLE temporal_rng (
  -- Since we can't depend on having btree_gist here,
  -- use an int4range instead of an int.
  -- (The rangetypes regression test uses the same trick.)
  id int4range,
  valid_at daterange,
  CONSTRAINT temporal_rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
SELECT create_distributed_table('temporal_rng', 'id');

-- okay:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[1,2)', daterange('2018-01-02', '2018-02-03'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[1,2)', daterange('2018-03-03', '2018-04-04'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[2,3)', daterange('2018-01-01', '2018-01-05'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[3,4)', daterange('2018-01-01', NULL));
-- should fail:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[1,2)', daterange('2018-01-01', '2018-01-05'));
-- NULLs are not allowed in the shard key:
INSERT INTO temporal_rng (id, valid_at) VALUES (NULL, daterange('2018-01-01', '2018-01-05'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[3,4)', NULL);
-- rejects empty:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[3,4)', 'empty');
SELECT * FROM temporal_rng ORDER BY id, valid_at;

-- Repeat with UNIQUE constraint
CREATE TABLE temporal_rng_uq (
  -- Since we can't depend on having btree_gist here,
  -- use an int4range instead of an int.
  id int4range,
  valid_at daterange,
  CONSTRAINT temporal_rng_uq_uk UNIQUE (id, valid_at WITHOUT OVERLAPS)
);
SELECT create_distributed_table('temporal_rng_uq', 'id');

-- okay:
INSERT INTO temporal_rng_uq (id, valid_at) VALUES ('[1,2)', daterange('2018-01-02', '2018-02-03'));
INSERT INTO temporal_rng_uq (id, valid_at) VALUES ('[1,2)', daterange('2018-03-03', '2018-04-04'));
INSERT INTO temporal_rng_uq (id, valid_at) VALUES ('[2,3)', daterange('2018-01-01', '2018-01-05'));
INSERT INTO temporal_rng_uq (id, valid_at) VALUES ('[3,4)', daterange('2018-01-01', NULL));
-- should fail:
INSERT INTO temporal_rng_uq (id, valid_at) VALUES ('[1,2)', daterange('2018-01-01', '2018-01-05'));
-- NULLs are not allowed in the shard key:
INSERT INTO temporal_rng_uq (id, valid_at) VALUES (NULL, daterange('2018-01-01', '2018-01-05'));
INSERT INTO temporal_rng_uq (id, valid_at) VALUES ('[3,4)', NULL);
-- rejects empty:
INSERT INTO temporal_rng_uq (id, valid_at) VALUES ('[3,4)', 'empty');
SELECT * FROM temporal_rng_uq ORDER BY id, valid_at;

DROP TABLE temporal_rng CASCADE;
DROP TABLE temporal_rng_uq CASCADE;

-- Repeat the tests with the PRIMARY KEY and UNIQUE constraints added
-- after the table is created and distributed. INSERTs produce the
-- same results as before.

CREATE TABLE temporal_rng (
  -- Since we can't depend on having btree_gist here,
  -- use an int4range instead of an int.
  -- (The rangetypes regression test uses the same trick.)
  id int4range,
  valid_at daterange
);
SELECT create_distributed_table('temporal_rng', 'id');

-- okay:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[1,2)', daterange('2018-01-02', '2018-02-03'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[1,2)', daterange('2018-03-03', '2018-04-04'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[2,3)', daterange('2018-01-01', '2018-01-05'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[3,4)', daterange('2018-01-01', NULL));

ALTER TABLE temporal_rng
  ADD CONSTRAINT temporal_rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS);

-- should fail:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[1,2)', daterange('2018-01-01', '2018-01-05'));
-- NULLs are not allowed in the shard key:
INSERT INTO temporal_rng (id, valid_at) VALUES (NULL, daterange('2018-01-01', '2018-01-05'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[3,4)', NULL);
-- rejects empty:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[3,4)', 'empty');
SELECT * FROM temporal_rng ORDER BY id, valid_at;

-- Repeat with UNIQUE constraint
CREATE TABLE temporal_rng_uq (
  -- Since we can't depend on having btree_gist here,
  -- use an int4range instead of an int.
  id int4range,
  valid_at daterange
);
SELECT create_distributed_table('temporal_rng_uq', 'id');

-- okay:
INSERT INTO temporal_rng_uq (id, valid_at) VALUES ('[1,2)', daterange('2018-01-02', '2018-02-03'));
INSERT INTO temporal_rng_uq (id, valid_at) VALUES ('[1,2)', daterange('2018-03-03', '2018-04-04'));
INSERT INTO temporal_rng_uq (id, valid_at) VALUES ('[2,3)', daterange('2018-01-01', '2018-01-05'));
INSERT INTO temporal_rng_uq (id, valid_at) VALUES ('[3,4)', daterange('2018-01-01', NULL));

ALTER TABLE temporal_rng_uq
  ADD CONSTRAINT temporal_rng_uq_uk UNIQUE (id, valid_at WITHOUT OVERLAPS);

-- should fail:
INSERT INTO temporal_rng_uq (id, valid_at) VALUES ('[1,2)', daterange('2018-01-01', '2018-01-05'));
-- NULLs are not allowed in the shard key:
INSERT INTO temporal_rng_uq (id, valid_at) VALUES (NULL, daterange('2018-01-01', '2018-01-05'));
INSERT INTO temporal_rng_uq (id, valid_at) VALUES ('[3,4)', NULL);
-- rejects empty:
INSERT INTO temporal_rng_uq (id, valid_at) VALUES ('[3,4)', 'empty');
SELECT * FROM temporal_rng_uq ORDER BY id, valid_at;

-- PG18 Feature: RETURNING old and new values in DML statements
-- PG18 commit: https://github.com/postgres/postgres/commit/80feb727c

CREATE TABLE users (id SERIAL PRIMARY KEY, email text, category int);
INSERT INTO users (email, category) SELECT 'xxx@foo.com', i % 10 from generate_series (1,100) t(i);

SELECT create_distributed_table('users','id');

UPDATE users SET email = 'colm@planet.com' WHERE id = 1
	RETURNING OLD.email AS previous_email,
		  NEW.email AS current_email;

SELECT * FROM users WHERE id = 1
ORDER BY id;

UPDATE users SET email = 'tim@arctic.net' WHERE id = 22
  RETURNING OLD.email AS previous_email,
      NEW.email AS current_email;

UPDATE users SET email = 'john@farm.ie' WHERE id = 33
  RETURNING OLD.email AS previous_email,
      NEW.email AS current_email;

SELECT * FROM users WHERE id = 22
ORDER BY id;

SELECT * FROM users
WHERE email not like 'xxx@%'
ORDER BY id;

-- NULL values creep into the email column..
INSERT INTO users (email, category) VALUES (null, 5)
  RETURNING OLD.email AS previous_email,
      NEW.email AS current_email;

UPDATE users SET email = NULL WHERE id = 79
  RETURNING OLD.email AS previous_email,
      NEW.email AS current_email;

-- Now add a NOT NULL constraint on email, but do
-- not apply it to existing rows yet.
ALTER TABLE users
ADD CONSTRAINT users_email_not_null
CHECK (email IS NOT NULL) NOT VALID;

UPDATE users SET email = NULL WHERE id = 50
  RETURNING OLD.email AS previous_email,
      NEW.email AS current_email;

-- Validation should fail due to existing NULLs
ALTER TABLE users VALIDATE CONSTRAINT users_email_not_null;

-- Fix NULL emails to a default value
UPDATE users SET email = 'xxx@foo.com' WHERE email IS NULL
  RETURNING OLD.email AS previous_email,
      NEW.email AS current_email;

-- Validation should now succeed
ALTER TABLE users VALIDATE CONSTRAINT users_email_not_null;

-- And prevent future NULLs
INSERT INTO users (email, category) VALUES (null, 10)
  RETURNING OLD.email AS previous_email,
      NEW.email AS current_email;

-- PG18 Feature: support for LIKE in CREATE FOREIGN TABLE
-- PG18 commit: https://github.com/postgres/postgres/commit/302cf1575
SET citus.use_citus_managed_tables TO ON;
CREATE EXTENSION postgres_fdw;

CREATE SERVER foreign_server
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host 'localhost', port :'master_port', dbname 'regression');

CREATE USER MAPPING FOR CURRENT_USER
        SERVER foreign_server
        OPTIONS (user 'postgres');

CREATE TABLE ctl_table(a int PRIMARY KEY,
  b varchar COMPRESSION pglz,
  c int GENERATED ALWAYS AS (a * 2) STORED,
  d bigint GENERATED ALWAYS AS IDENTITY,
  e int DEFAULT 1);

CREATE INDEX ctl_table_ab_key ON ctl_table(a, b);
COMMENT ON COLUMN ctl_table.b IS 'Column b';
CREATE STATISTICS ctl_table_stat ON a,b FROM ctl_table;

INSERT INTO ctl_table VALUES (1, 'first'), (2, 'second'), (3, 'third'), (4, 'fourth');

-- Test EXCLUDING ALL
CREATE FOREIGN TABLE ctl_ft1(LIKE ctl_table EXCLUDING ALL)
  SERVER foreign_server
        OPTIONS (schema_name 'pg18_nn', table_name 'ctl_table');
-- Test INCLUDING ALL
CREATE FOREIGN TABLE ctl_ft2(LIKE ctl_table INCLUDING ALL)
  SERVER foreign_server
        OPTIONS (schema_name 'pg18_nn', table_name 'ctl_table');

-- check that the foreign tables are citus local table
SELECT partmethod, repmodel FROM pg_dist_partition
WHERE logicalrelid IN ('ctl_ft1'::regclass, 'ctl_ft2'::regclass) ORDER BY logicalrelid;

-- we can query the foreign tables
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ctl_ft1 ORDER BY a;
SELECT * FROM ctl_ft1 ORDER BY a;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ctl_ft2 ORDER BY a;
SELECT * FROM ctl_ft2 ORDER BY a;

-- Clean up foreign table test
RESET citus.use_citus_managed_tables;
SELECT undistribute_table('ctl_ft1');
SELECT undistribute_table('ctl_ft2');

DROP SERVER foreign_server CASCADE;

-- PG18 Feature: PERIOD clause in foreign key constraint definitions.
-- PG18 commit: https://github.com/postgres/postgres/commit/89f908a6d

-- This test verifies that the PG18 tests apply to Citus tables

CREATE EXTENSION btree_gist; -- needed for range type indexing
CREATE TABLE temporal_test (
  id integer,
  valid_at daterange,
  CONSTRAINT temporal_test_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);

SET citus.shard_count TO 4;

SELECT create_reference_table( 'temporal_test');

INSERT INTO temporal_test VALUES
  (1, '[2000-01-01,2001-01-01)');
-- same key, doesn't overlap:
INSERT INTO temporal_test VALUES
  (1, '[2001-01-01,2002-01-01)');
-- overlaps but different key:
INSERT INTO temporal_test VALUES
  (2, '[2000-01-01,2001-01-01)');
-- should fail:
INSERT INTO temporal_test VALUES
  (1, '[2000-06-01,2001-01-01)');

-- Required for foreign key constraint on distributed table
SET citus.shard_replication_factor TO 1;

-- Create and distribute a table with temporal foreign key constraints
CREATE TABLE temporal_fk_rng2rng (
  id integer,
  valid_at daterange,
  parent_id integer,
  CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);

SELECT create_distributed_table( 'temporal_fk_rng2rng', 'id');

--
-- Add foreign key constraint with PERIOD clause
-- This is propagated to worker shards
ALTER TABLE temporal_fk_rng2rng
  ADD CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
    REFERENCES temporal_test (id, PERIOD valid_at);

INSERT INTO temporal_fk_rng2rng VALUES
  (1, '[2000-01-01,2001-01-01)', 1);
-- okay spanning two parent records:
INSERT INTO temporal_fk_rng2rng VALUES
  (2, '[2000-01-01,2002-01-01)', 1);
-- key is missing
INSERT INTO temporal_fk_rng2rng VALUES
  (3, '[2000-01-01,2001-01-01)', 3);
-- key exist but is outside range
INSERT INTO temporal_fk_rng2rng VALUES
  (4, '[2001-01-01,2002-01-01)', 2);
-- key exist but is partly outside range
INSERT INTO temporal_fk_rng2rng VALUES
  (5, '[2000-01-01,2002-01-01)', 2);

-- PG18 Feature: REJECT_LIMIT option for COPY errors
-- PG18 commit: https://github.com/postgres/postgres/commit/4ac2a9bec

-- Citus does not support COPY with ON_ERROR so just need to
-- ensure the appropriate error is returned.

CREATE TABLE check_ign_err (n int, m int[], k int);
SELECT create_distributed_table('check_ign_err', 'n');

COPY check_ign_err FROM STDIN WITH (on_error stop, reject_limit 5);
COPY check_ign_err FROM STDIN WITH (ON_ERROR ignore, REJECT_LIMIT 100);
COPY check_ign_err FROM STDIN WITH (on_error ignore, log_verbosity verbose, reject_limit 50);
COPY check_ign_err FROM STDIN WITH (reject_limt 77, log_verbosity verbose, on_error ignore);
-- PG requires on_error when reject_limit is specified
COPY check_ign_err FROM STDIN WITH (reject_limit 100);

-- PG18 Feature: COPY TABLE TO on a materialized view
-- PG18 commit: https://github.com/postgres/postgres/commit/534874fac

-- This does not work in Citus as a materialized view cannot be distributed.
-- So just verify that the appropriate error is raised.
CREATE MATERIALIZED VIEW copytest_mv AS
  SELECT i as id, md5(i::text) as hashval
  FROM generate_series(1,100) i;
-- Attempting to make it distributed should fail with appropriate error as
-- Citus does not yet support materialized views.
SELECT create_distributed_table('copytest_mv', 'id');
-- After that, any command on the materialized view is outside Citus support.

-- PG18: verify publish_generated_columns is preserved for distributed tables
-- https://github.com/postgres/postgres/commit/7054186c4
\c - - - :master_port
CREATE SCHEMA pg18_publication;
SET search_path TO pg18_publication;

-- table with a stored generated column
CREATE TABLE gen_pub_tab (
    id int primary key,
    a  int,
    b  int GENERATED ALWAYS AS (a * 10) STORED
);

-- make it distributed so CREATE PUBLICATION goes through Citus metadata/DDL path
SELECT create_distributed_table('gen_pub_tab', 'id', colocate_with := 'none');

-- publication using the new PG18 option: stored
CREATE PUBLICATION pub_gen_cols_stored
    FOR TABLE gen_pub_tab
    WITH (publish = 'insert, update', publish_generated_columns = stored);

-- second publication explicitly using "none" for completeness
CREATE PUBLICATION pub_gen_cols_none
    FOR TABLE gen_pub_tab
    WITH (publish = 'insert, update', publish_generated_columns = none);

-- On coordinator: pubgencols must be 's' and 'n' respectively
SELECT pubname, pubgencols
FROM pg_publication
WHERE pubname IN ('pub_gen_cols_stored', 'pub_gen_cols_none')
ORDER BY pubname;

-- On worker 1: both publications must exist and keep pubgencols in sync
\c - - - :worker_1_port
SET search_path TO pg18_publication;

SELECT pubname, pubgencols
FROM pg_publication
WHERE pubname IN ('pub_gen_cols_stored', 'pub_gen_cols_none')
ORDER BY pubname;

-- On worker 2: same check
\c - - - :worker_2_port
SET search_path TO pg18_publication;

SELECT pubname, pubgencols
FROM pg_publication
WHERE pubname IN ('pub_gen_cols_stored', 'pub_gen_cols_none')
ORDER BY pubname;

-- Now verify ALTER PUBLICATION .. SET (publish_generated_columns = none)
-- propagates to workers as well.

\c - - - :master_port
SET search_path TO pg18_publication;

ALTER PUBLICATION pub_gen_cols_stored
    SET (publish_generated_columns = none);

-- coordinator: both publications should now have pubgencols = 'n'
SELECT pubname, pubgencols
FROM pg_publication
WHERE pubname IN ('pub_gen_cols_stored', 'pub_gen_cols_none')
ORDER BY pubname;

-- worker 1: pubgencols must match coordinator
\c - - - :worker_1_port
SET search_path TO pg18_publication;

SELECT pubname, pubgencols
FROM pg_publication
WHERE pubname IN ('pub_gen_cols_stored', 'pub_gen_cols_none')
ORDER BY pubname;

-- worker 2: same check
\c - - - :worker_2_port
SET search_path TO pg18_publication;

SELECT pubname, pubgencols
FROM pg_publication
WHERE pubname IN ('pub_gen_cols_stored', 'pub_gen_cols_none')
ORDER BY pubname;

-- Column list precedence test: Citus must preserve both prattrs and pubgencols

\c - - - :master_port
SET search_path TO pg18_publication;

-- Case 1: column list explicitly includes the generated column, flag = none
CREATE PUBLICATION pub_gen_cols_list_includes_b
    FOR TABLE gen_pub_tab (id, a, b)
    WITH (publish_generated_columns = none);

-- Case 2: column list excludes the generated column, flag = stored
CREATE PUBLICATION pub_gen_cols_list_excludes_b
    FOR TABLE gen_pub_tab (id, a)
    WITH (publish_generated_columns = stored);

-- Helper: show pubname, pubgencols, and column list (prattrs) for gen_pub_tab
SELECT p.pubname,
       p.pubgencols,
       r.prattrs
FROM pg_publication p
JOIN pg_publication_rel r ON p.oid = r.prpubid
JOIN pg_class c ON c.oid = r.prrelid
WHERE p.pubname IN ('pub_gen_cols_list_includes_b',
                    'pub_gen_cols_list_excludes_b')
  AND c.relname = 'gen_pub_tab'
ORDER BY p.pubname;

-- worker 1: must see the same pubgencols + prattrs
\c - - - :worker_1_port
SET search_path TO pg18_publication;

SELECT p.pubname,
       p.pubgencols,
       r.prattrs
FROM pg_publication p
JOIN pg_publication_rel r ON p.oid = r.prpubid
JOIN pg_class c ON c.oid = r.prrelid
WHERE p.pubname IN ('pub_gen_cols_list_includes_b',
                    'pub_gen_cols_list_excludes_b')
  AND c.relname = 'gen_pub_tab'
ORDER BY p.pubname;

-- worker 2: same check
\c - - - :worker_2_port
SET search_path TO pg18_publication;

SELECT p.pubname,
       p.pubgencols,
       r.prattrs
FROM pg_publication p
JOIN pg_publication_rel r ON p.oid = r.prpubid
JOIN pg_class c ON c.oid = r.prrelid
WHERE p.pubname IN ('pub_gen_cols_list_includes_b',
                    'pub_gen_cols_list_excludes_b')
  AND c.relname = 'gen_pub_tab'
ORDER BY p.pubname;

-- back to coordinator for subsequent tests / cleanup
\c - - - :master_port
SET search_path TO pg18_publication;
DROP PUBLICATION pub_gen_cols_stored;
DROP PUBLICATION pub_gen_cols_none;
DROP PUBLICATION pub_gen_cols_list_includes_b;
DROP PUBLICATION pub_gen_cols_list_excludes_b;
DROP SCHEMA pg18_publication CASCADE;
SET search_path TO pg18_nn;
-- END: PG18: verify publish_generated_columns is preserved for distributed tables

-- PG18 Feature: FOREIGN KEY constraints can be specified as NOT ENFORCED
-- PG18 commit: https://github.com/postgres/postgres/commit/eec0040c4
CREATE TABLE customers(
   customer_id INT GENERATED ALWAYS AS IDENTITY,
   customer_name VARCHAR(255) NOT NULL,
   PRIMARY KEY(customer_id)
);

SELECT create_distributed_table('customers', 'customer_id');

CREATE TABLE contacts(
   contact_id INT GENERATED ALWAYS AS IDENTITY,
   customer_id INT,
   contact_name VARCHAR(255) NOT NULL,
   phone VARCHAR(15),
   email VARCHAR(100),
   CONSTRAINT fk_customer
      FOREIGN KEY(customer_id)
	  REFERENCES customers(customer_id)
	  ON DELETE CASCADE NOT ENFORCED
);

-- The foreign key constraint is propagated to worker nodes.
SELECT create_distributed_table('contacts', 'customer_id');

SELECT pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
WHERE conrelid = 'contacts'::regclass AND conname = 'fk_customer';

INSERT INTO customers(customer_name)
VALUES('BlueBird Inc'),
      ('Dolphin LLC');

INSERT INTO contacts(customer_id, contact_name, phone, email)
VALUES(1,'John Doe','(408)-111-1234','john.doe@example.com'),
      (1,'Jane Doe','(408)-111-1235','jane.doe@example.com'),
      (2,'David Wright','(408)-222-1234','david.wright@example.com');

DELETE FROM customers WHERE customer_name = 'Dolphin LLC';

-- After deleting 'Dolphin LLC' from customers, the corresponding contact
-- 'David Wright' is not deleted from contacts due to the NOT ENFORCED.
SELECT * FROM contacts ORDER BY contact_id;

-- Test that ALTER TABLE .. ADD CONSTRAINT .. FOREIGN KEY .. NOT ENFORCED
-- is propagated to worker nodes. First drop the foreign key:
ALTER TABLE contacts DROP CONSTRAINT fk_customer;

SELECT pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
WHERE conrelid = 'contacts'::regclass AND conname = 'fk_customer';

-- Now add the foreign key constraint back with NOT ENFORCED.
ALTER TABLE contacts ADD CONSTRAINT fk_customer
     FOREIGN KEY(customer_id)
     REFERENCES customers(customer_id)
     ON DELETE CASCADE NOT ENFORCED;

-- The foreign key is propagated to worker nodes.
SELECT pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
WHERE conrelid = 'contacts'::regclass AND conname = 'fk_customer';

DELETE FROM customers WHERE customer_name = 'BlueBird Inc';

-- The customers table is now empty but the contacts table still has
-- the contacts due to the NOT ENFORCED foreign key.
SELECT * FROM customers ORDER BY customer_id;
SELECT * FROM contacts ORDER BY contact_id;

-- ALTER TABLE .. ALTER CONSTRAINT is not supported in Citus,
-- so the following command should fail
ALTER TABLE contacts ALTER CONSTRAINT fk_customer ENFORCED;

-- PG18 Feature: ENFORCED / NOT ENFORCED check constraints
-- PG18 commit: https://github.com/postgres/postgres/commit/ca87c415e

-- In Citus, CHECK constraints are propagated on promoting a postgres table
-- to a citus table, on adding a new CHECK constraint to a citus table, and
-- on adding a node to a citus cluster. Postgres does not support altering a
-- check constraint's enforcement status, so Citus does not either.

CREATE TABLE NE_CHECK_TBL (x int, y int,
	CONSTRAINT CHECK_X CHECK (x > 3) NOT ENFORCED,
  CONSTRAINT CHECK_Y CHECK (y < 20) ENFORCED
);

SELECT create_distributed_table('ne_check_tbl', 'x');

-- CHECK_X is NOT ENFORCED, so these inserts should succeed
INSERT INTO NE_CHECK_TBL (x) VALUES (5), (4), (3), (2), (6), (1);
SELECT x FROM NE_CHECK_TBL ORDER BY x;

-- CHECK_Y is ENFORCED, so this insert should fail
INSERT INTO NE_CHECK_TBL (x, y) VALUES (1, 15), (2, 25), (3, 10), (4, 30);

-- Test adding new constraints with enforcement status
ALTER TABLE NE_CHECK_TBL
  ADD CONSTRAINT CHECK_Y2 CHECK (y > 10) NOT ENFORCED;

-- CHECK_Y2 is NOT ENFORCED, so these inserts should succeed
INSERT INTO NE_CHECK_TBL (x, y) VALUES (1, 8), (2, 9), (3, 10), (4, 11);
SELECT x, y FROM NE_CHECK_TBL ORDER BY x, y;

ALTER TABLE NE_CHECK_TBL
  ADD CONSTRAINT CHECK_X2 CHECK (x < 10) ENFORCED;

-- CHECK_X2 is ENFORCED, so these inserts should fail
INSERT INTO NE_CHECK_TBL (x) VALUES (5), (15), (8), (12);

-- cleanup with minimum verbosity
SET client_min_messages TO ERROR;
RESET search_path;
RESET citus.shard_count;
RESET citus.shard_replication_factor;
DROP SCHEMA pg18_nn CASCADE;
RESET client_min_messages;
