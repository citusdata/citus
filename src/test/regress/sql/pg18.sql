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

SET citus.shard_replication_factor TO 1;

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

SET citus.next_shard_id TO 4754044;
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

-- PG18 Feature: dropping of constraints ONLY on partitioned tables
-- PG18 commit: https://github.com/postgres/postgres/commit/4dea33ce7

-- Here we verify that dropping constraints ONLY on partitioned tables
-- works correctly in Citus. This is done by repeating the tests of the
-- PG commit (4dea33ce7) on a table that is a distributed table in Citus,
-- in addition to a Postgres partitioned table.

CREATE TABLE partitioned_table (
	a int,
	b char(3)
) PARTITION BY LIST (a);

SELECT create_distributed_table('partitioned_table', 'a');

-- check that violating rows are correctly reported
CREATE TABLE part_2 (LIKE partitioned_table);
INSERT INTO part_2 VALUES (3, 'aaa');
ALTER TABLE partitioned_table ATTACH PARTITION part_2 FOR VALUES IN (2);

-- should be ok after deleting the bad row
DELETE FROM part_2;
ALTER TABLE partitioned_table ATTACH PARTITION part_2 FOR VALUES IN (2);

-- PG18's "cannot add NOT NULL or check constraints to *only* the parent, when
-- partitions exist" applies to Citus distributed tables as well.

ALTER TABLE ONLY partitioned_table ALTER b SET NOT NULL;
ALTER TABLE ONLY partitioned_table ADD CONSTRAINT check_b CHECK (b <> 'zzz');

-- Dropping constraints from parent should be ok
ALTER TABLE partitioned_table ALTER b SET NOT NULL;
ALTER TABLE ONLY partitioned_table ALTER b DROP NOT NULL;
ALTER TABLE partitioned_table ADD CONSTRAINT check_b CHECK (b <> 'zzz');
ALTER TABLE ONLY partitioned_table DROP CONSTRAINT check_b;

-- ... and the partitions still have the NOT NULL constraint:
select relname, attname, attnotnull
from pg_class inner join pg_attribute on (oid=attrelid)
where relname = 'part_2' and attname = 'b' ;
-- ... and the check_b constraint:
select relname, conname, pg_get_expr(conbin, conrelid, true)
from pg_class inner join pg_constraint on (pg_class.oid=conrelid)
where relname = 'part_2' and conname = 'check_b' ;

-- PG18 Feature: partitioned tables can have NOT VALID foreign keys
-- PG18 commit: https://github.com/postgres/postgres/commit/b663b9436

-- As with dropping constraints only on the partitioned tables, for
-- NOT VALID foreign keys, we verify that foreign key declarations
-- that use NOT VALID work correctly in Citus by repeating the tests
-- of the PG commit (b663b9436) on a table that is a distributed
-- table in Citus, in addition to a Postgres partitioned table.

CREATE TABLE fk_notpartitioned_pk (a int, b int, PRIMARY KEY (a, b), c int);
CREATE TABLE fk_partitioned_fk (b int, a int) PARTITION BY RANGE (a, b);

SELECT create_reference_table('fk_notpartitioned_pk');
SELECT create_distributed_table('fk_partitioned_fk', 'a');

ALTER TABLE fk_partitioned_fk ADD FOREIGN KEY (a, b) REFERENCES fk_notpartitioned_pk NOT VALID;

-- Attaching a child table with the same valid foreign key constraint.
CREATE TABLE fk_partitioned_fk_1 (a int, b int);
ALTER TABLE fk_partitioned_fk_1 ADD FOREIGN KEY (a, b) REFERENCES fk_notpartitioned_pk;
ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_1 FOR VALUES FROM (0,0) TO (1000,1000);

-- Child constraint will remain valid.
SELECT conname, convalidated, conrelid::regclass FROM pg_constraint
WHERE conrelid::regclass::text like 'fk_partitioned_fk%' ORDER BY oid;

-- Validate the constraint
ALTER TABLE fk_partitioned_fk VALIDATE CONSTRAINT fk_partitioned_fk_a_b_fkey;

-- All constraints are now valid.
SELECT conname, convalidated, conrelid::regclass FROM pg_constraint
WHERE conrelid::regclass::text like 'fk_partitioned_fk%' ORDER BY oid;

-- Attaching a child with a NOT VALID constraint.
CREATE TABLE fk_partitioned_fk_2 (a int, b int);
INSERT INTO fk_partitioned_fk_2 VALUES(1000, 1000); -- doesn't exist in referenced table
ALTER TABLE fk_partitioned_fk_2 ADD FOREIGN KEY (a, b) REFERENCES fk_notpartitioned_pk NOT VALID;

-- It will fail because the attach operation implicitly validates the data.
ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_2 FOR VALUES FROM (1000,1000) TO (2000,2000);

-- Remove the invalid data and try again.
TRUNCATE fk_partitioned_fk_2;
ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_2 FOR VALUES FROM (1000,1000) TO (2000,2000);

-- The child constraint will also be valid.
SELECT conname, convalidated FROM pg_constraint WHERE conrelid = 'fk_partitioned_fk_2'::regclass;

-- Test case where the child constraint is invalid, the grandchild constraint
-- is valid, and the validation for the grandchild should be skipped when a
-- valid constraint is applied to the top parent.
CREATE TABLE fk_partitioned_fk_3 (a int, b int) PARTITION BY RANGE (a, b);
ALTER TABLE fk_partitioned_fk_3 ADD FOREIGN KEY (a, b) REFERENCES fk_notpartitioned_pk NOT VALID;
SELECT create_distributed_table('fk_partitioned_fk_3', 'a');

CREATE TABLE fk_partitioned_fk_3_1 (a int, b int);
ALTER TABLE fk_partitioned_fk_3_1 ADD FOREIGN KEY (a, b) REFERENCES fk_notpartitioned_pk;
SELECT create_distributed_table('fk_partitioned_fk_3_1', 'a');

ALTER TABLE fk_partitioned_fk_3 ATTACH PARTITION fk_partitioned_fk_3_1 FOR VALUES FROM (2000,2000) TO (3000,3000);

-- Fails because Citus does not support multi-level (grandchild) partitions
ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_3 FOR VALUES FROM (2000,2000) TO (3000,3000);

-- All constraints are now valid, except for fk_partitioned_fk_3
-- because the attach failed because of Citus not yet supporting
-- multi-level partitions.
SELECT conname, convalidated, conrelid::regclass FROM pg_constraint
WHERE conrelid::regclass::text like 'fk_partitioned_fk%' ORDER BY oid;

DROP TABLE fk_partitioned_fk, fk_notpartitioned_pk CASCADE;

-- NOT VALID foreign key on a non-partitioned table referencing a partitioned table
CREATE TABLE fk_partitioned_pk (a int, b int, PRIMARY KEY (a, b)) PARTITION BY RANGE (a, b);
SELECT create_distributed_table('fk_partitioned_pk', 'a');
CREATE TABLE fk_partitioned_pk_1 PARTITION OF fk_partitioned_pk FOR VALUES FROM (0,0) TO (1000,1000);

CREATE TABLE fk_notpartitioned_fk (b int, a int);
SELECT create_distributed_table('fk_notpartitioned_fk', 'a');
ALTER TABLE fk_notpartitioned_fk ADD FOREIGN KEY (a, b) REFERENCES fk_partitioned_pk NOT VALID;

-- Constraint will be invalid.
SELECT conname, convalidated FROM pg_constraint WHERE conrelid = 'fk_notpartitioned_fk'::regclass;

ALTER TABLE fk_notpartitioned_fk VALIDATE CONSTRAINT fk_notpartitioned_fk_a_b_fkey;

-- All constraints are now valid.
SELECT conname, convalidated FROM pg_constraint WHERE conrelid = 'fk_notpartitioned_fk'::regclass;

DROP TABLE fk_notpartitioned_fk, fk_partitioned_pk;

-- PG18 Feature: Generated Virtual Columns
-- PG18 commit: https://github.com/postgres/postgres/commit/83ea6c540

-- Verify that generated virtual columns are supported on distributed tables.
CREATE TABLE v_reading (
    celsius DECIMAL(5,2),
    farenheit DECIMAL(6, 2) GENERATED ALWAYS AS (celsius * 9/5 + 32) VIRTUAL,
    created_at TIMESTAMPTZ DEFAULT now(),
    device_id INT
);

-- Cannot distribute on a generated column (#4616) applies
-- to VIRTUAL columns.
SELECT create_distributed_table('v_reading', 'farenheit');

SELECT create_distributed_table('v_reading', 'device_id');

INSERT INTO v_reading (celsius, device_id) VALUES (0, 1), (100, 1), (37.5, 2), (25, 2), (-40, 3);

SELECT device_id, celsius, farenheit FROM v_reading ORDER BY device_id;

ALTER TABLE v_reading ADD COLUMN kelvin DECIMAL(6, 2) GENERATED ALWAYS AS (celsius +  273.15) VIRTUAL;
SELECT device_id, celsius, kelvin FROM v_reading ORDER BY device_id, celsius;

-- Show all columns that are generated
  SELECT s.relname, a.attname, a.attgenerated
  FROM pg_class s
  JOIN pg_attribute a ON a.attrelid=s.oid
  WHERE s.relname LIKE 'v_reading%' and attgenerated::int != 0
  ORDER BY 1,2;

-- Generated columns are virtual by default - repeat the test without VIRTUAL keyword
CREATE TABLE d_reading (
    celsius DECIMAL(5,2),
    farenheit DECIMAL(6, 2) GENERATED ALWAYS AS (celsius * 9/5 + 32),
    created_at TIMESTAMPTZ DEFAULT now(),
    device_id INT
);

SELECT create_distributed_table('d_reading', 'farenheit');

SELECT create_distributed_table('d_reading', 'device_id');

INSERT INTO d_reading (celsius, device_id) VALUES (0, 1), (100, 1), (37.5, 2), (25, 2), (-40, 3);

SELECT device_id, celsius, farenheit FROM d_reading ORDER BY device_id;

ALTER TABLE d_reading ADD COLUMN kelvin DECIMAL(6, 2) GENERATED ALWAYS AS (celsius +  273.15) VIRTUAL;
SELECT device_id, celsius, kelvin FROM d_reading ORDER BY device_id, celsius;

-- Show all columns that are generated
  SELECT s.relname, a.attname, a.attgenerated
  FROM pg_class s
  JOIN pg_attribute a ON a.attrelid=s.oid
  WHERE s.relname LIKE 'd_reading%' and attgenerated::int != 0
  ORDER BY 1,2;

-- COPY implementation needs to handle GENERATED ALWAYS AS (...) VIRTUAL columns.
\COPY d_reading FROM STDIN WITH DELIMITER ','
3.00,2025-11-24 09:46:17.390872+00,1
6.00,2025-11-24 09:46:17.390872+00,5
2.00,2025-11-24 09:46:17.390872+00,1
22.00,2025-11-24 09:46:17.390872+00,5
15.00,2025-11-24 09:46:17.390872+00,1
13.00,2025-11-24 09:46:17.390872+00,5
27.00,2025-11-24 09:46:17.390872+00,1
14.00,2025-11-24 09:46:17.390872+00,5
2.00,2025-11-24 09:46:17.390872+00,1
23.00,2025-11-24 09:46:17.390872+00,5
22.00,2025-11-24 09:46:17.390872+00,1
3.00,2025-11-24 09:46:17.390872+00,5
2.00,2025-11-24 09:46:17.390872+00,1
7.00,2025-11-24 09:46:17.390872+00,5
6.00,2025-11-24 09:46:17.390872+00,1
21.00,2025-11-24 09:46:17.390872+00,5
30.00,2025-11-24 09:46:17.390872+00,1
1.00,2025-11-24 09:46:17.390872+00,5
31.00,2025-11-24 09:46:17.390872+00,1
22.00,2025-11-24 09:46:17.390872+00,5
\.

SELECT device_id, count(device_id) as count, round(avg(celsius), 2) as avg, min(farenheit), max(farenheit)
FROM d_reading
GROUP BY device_id
ORDER BY count DESC;

-- Test GROUP BY on tables with generated virtual columns - this requires
-- special case handling in distributed planning. Test it out on some
-- some queries involving joins and set operations.

SELECT device_id, max(kelvin) as Kel
FROM v_reading
WHERE (device_id, celsius) NOT IN (SELECT device_id, max(celsius) FROM v_reading GROUP BY device_id)
GROUP BY device_id
ORDER BY device_id ASC;

SELECT device_id, round(AVG( (d_farenheit + v_farenheit) / 2), 2) as Avg_Far
FROM (SELECT *
      FROM (SELECT device_id, round(AVG(farenheit),2) as d_farenheit
            FROM d_reading
            GROUP BY device_id) AS subq
            RIGHT JOIN (SELECT device_id, MAX(farenheit) AS v_farenheit
                  FROM d_reading
                  GROUP BY device_id) AS subq2
            USING (device_id)
    ) AS finalq
GROUP BY device_id
ORDER BY device_id ASC;

SELECT device_id, MAX(farenheit) as farenheit
FROM
((SELECT device_id, round(AVG(farenheit),2) as farenheit
 FROM d_reading
 GROUP BY device_id)
UNION ALL (SELECT device_id, MAX(farenheit) AS farenheit
        FROM d_reading
        GROUP BY device_id) ) AS unioned
GROUP BY device_id
ORDER BY device_id ASC;

SELECT device_id, MAX(farenheit) as farenheit
FROM
((SELECT device_id, round(AVG(farenheit),2) as farenheit
 FROM d_reading
 GROUP BY device_id)
INTERSECT (SELECT device_id, MAX(farenheit) AS farenheit
        FROM d_reading
        GROUP BY device_id) ) AS intersected
GROUP BY device_id
ORDER BY device_id ASC;

SELECT device_id, MAX(farenheit) as farenheit
FROM
((SELECT device_id, round(AVG(farenheit),2) as farenheit
 FROM d_reading
 GROUP BY device_id)
EXCEPT (SELECT device_id, MAX(farenheit) AS farenheit
        FROM d_reading
        GROUP BY device_id) ) AS excepted
GROUP BY device_id
ORDER BY device_id ASC;

-- Ensure that UDFs such as alter_distributed_table, undistribute_table
-- and add_local_table_to_metadata work fine with VIRTUAL columns. For
-- this, PR #4616 changes are modified to handle VIRTUAL columns in
-- addition to STORED columns.

CREATE TABLE generated_stored_dist (
  col_1 int,
  "col\'_2" text,
  col_3 text generated always as (UPPER("col\'_2")) virtual
);

SELECT create_distributed_table ('generated_stored_dist', 'col_1');

INSERT INTO generated_stored_dist VALUES (1, 'text_1'), (2, 'text_2');
SELECT * FROM generated_stored_dist ORDER BY 1,2,3;

INSERT INTO generated_stored_dist VALUES (1, 'text_1'), (2, 'text_2');
SELECT alter_distributed_table('generated_stored_dist', shard_count := 5, cascade_to_colocated := false);
SELECT * FROM generated_stored_dist ORDER BY 1,2,3;

CREATE TABLE generated_stored_local (
  col_1 int,
  "col\'_2" text,
  col_3 text generated always as (UPPER("col\'_2")) stored
);

SELECT citus_add_local_table_to_metadata('generated_stored_local');

INSERT INTO generated_stored_local VALUES (1, 'text_1'), (2, 'text_2');
SELECT * FROM generated_stored_local ORDER BY 1,2,3;

SELECT create_distributed_table ('generated_stored_local', 'col_1');

INSERT INTO generated_stored_local VALUES (1, 'text_1'), (2, 'text_2');
SELECT * FROM generated_stored_local ORDER BY 1,2,3;

CREATE TABLE generated_stored_ref (
  col_1 int,
  col_2 int,
  col_3 int generated always as (col_1+col_2) virtual,
  col_4 int,
  col_5 int generated always as (col_4*2-col_1) virtual
);

SELECT create_reference_table ('generated_stored_ref');

INSERT INTO generated_stored_ref (col_1, col_4) VALUES (1,2), (11,12);
INSERT INTO generated_stored_ref (col_1, col_2, col_4) VALUES (100,101,102), (200,201,202);

SELECT * FROM generated_stored_ref ORDER BY 1,2,3,4,5;

BEGIN;
  SELECT undistribute_table('generated_stored_ref');
  INSERT INTO generated_stored_ref (col_1, col_4) VALUES (11,12), (21,22);
  INSERT INTO generated_stored_ref (col_1, col_2, col_4) VALUES (200,201,202), (300,301,302);
  SELECT * FROM generated_stored_ref ORDER BY 1,2,3,4,5;
ROLLBACK;

BEGIN;
  -- drop some of the columns not having "generated always as virtual" expressions
  SET client_min_messages TO WARNING;
  ALTER TABLE generated_stored_ref DROP COLUMN col_1 CASCADE;
  RESET client_min_messages;
  ALTER TABLE generated_stored_ref DROP COLUMN col_4;

  -- show that undistribute_table works fine
  SELECT undistribute_table('generated_stored_ref');
  INSERT INTO generated_stored_ref VALUES (5);
  SELECT * FROM generated_stored_REF ORDER BY 1;
ROLLBACK;

BEGIN;
  -- now drop all columns
  ALTER TABLE generated_stored_ref DROP COLUMN col_3;
  ALTER TABLE generated_stored_ref DROP COLUMN col_5;
  ALTER TABLE generated_stored_ref DROP COLUMN col_1;
  ALTER TABLE generated_stored_ref DROP COLUMN col_2;
  ALTER TABLE generated_stored_ref DROP COLUMN col_4;

  -- show that undistribute_table works fine
  SELECT undistribute_table('generated_stored_ref');

  SELECT * FROM generated_stored_ref;
ROLLBACK;

-- PG18 Feature: VACUUM/ANALYZE support ONLY to limit processing to the parent.
-- For Citus, ensure ONLY does not trigger shard propagation.
-- PG18 commit: https://github.com/postgres/postgres/commit/62ddf7ee9
CREATE SCHEMA pg18_vacuum_part;
SET search_path TO pg18_vacuum_part;

CREATE TABLE vac_analyze_only (a int);
SELECT create_distributed_table('vac_analyze_only', 'a');
INSERT INTO vac_analyze_only VALUES (1), (2), (3);

-- ANALYZE (no ONLY) should recurse into shard placements
ANALYZE vac_analyze_only;

\c - - - :worker_1_port
SET search_path TO pg18_vacuum_part;

SELECT coalesce(max(last_analyze), 'epoch'::timestamptz) AS analyze_before_only
FROM pg_stat_user_tables
WHERE schemaname = 'pg18_vacuum_part'
  AND relname LIKE 'vac_analyze_only_%'
\gset

\c - - - :master_port
SET search_path TO pg18_vacuum_part;

-- ANALYZE ONLY should not recurse into shard placements
ANALYZE ONLY vac_analyze_only;

\c - - - :worker_1_port
SET search_path TO pg18_vacuum_part;

SELECT max(last_analyze) = :'analyze_before_only'::timestamptz
       AS analyze_only_skipped
FROM pg_stat_user_tables
WHERE schemaname = 'pg18_vacuum_part'
  AND relname LIKE 'vac_analyze_only_%';

\c - - - :master_port
SET search_path TO pg18_vacuum_part;

-- VACUUM (no ONLY) should recurse into shard placements
VACUUM vac_analyze_only;

\c - - - :worker_1_port
SET search_path TO pg18_vacuum_part;

SELECT coalesce(max(last_vacuum), 'epoch'::timestamptz) AS vacuum_before_only
FROM pg_stat_user_tables
WHERE schemaname = 'pg18_vacuum_part'
  AND relname LIKE 'vac_analyze_only_%'
\gset

\c - - - :master_port
SET search_path TO pg18_vacuum_part;

-- VACUUM ONLY should not recurse into shard placements
VACUUM ONLY vac_analyze_only;

\c - - - :worker_1_port
SET search_path TO pg18_vacuum_part;

SELECT max(last_vacuum) = :'vacuum_before_only'::timestamptz
       AS vacuum_only_skipped
FROM pg_stat_user_tables
WHERE schemaname = 'pg18_vacuum_part'
  AND relname LIKE 'vac_analyze_only_%';

\c - - - :master_port
SET search_path TO pg18_vacuum_part;

DROP SCHEMA pg18_vacuum_part CASCADE;
SET search_path TO pg18_nn;

-- END PG18 Feature: VACUUM/ANALYZE support ONLY to limit processing to the parent

-- PG18 Feature: VACUUM/ANALYZE ONLY on a partitioned distributed table
-- Ensure Citus does not recurse into shard placements when ONLY is used
-- on the partitioned parent.
-- PG18 commit: https://github.com/postgres/postgres/commit/62ddf7ee9
CREATE SCHEMA pg18_vacuum_part_dist;
SET search_path TO pg18_vacuum_part_dist;

SET citus.shard_count = 2;
SET citus.shard_replication_factor = 1;

CREATE TABLE part_dist (id int, v int) PARTITION BY RANGE (id);
CREATE TABLE part_dist_1 PARTITION OF part_dist FOR VALUES FROM (1) TO (100);
CREATE TABLE part_dist_2 PARTITION OF part_dist FOR VALUES FROM (100) TO (200);

SELECT create_distributed_table('part_dist', 'id');

INSERT INTO part_dist
SELECT g, g FROM generate_series(1, 199) g;

-- ANALYZE (no ONLY) should recurse into partitions and shard placements
ANALYZE part_dist;

\c - - - :worker_1_port
SET search_path TO pg18_vacuum_part_dist;

SELECT coalesce(max(last_analyze), 'epoch'::timestamptz) AS analyze_before_only
FROM pg_stat_user_tables
WHERE schemaname = 'pg18_vacuum_part_dist'
  AND relname LIKE 'part_dist_%'
\gset

\c - - - :master_port
SET search_path TO pg18_vacuum_part_dist;

-- ANALYZE ONLY should not recurse into shard placements
ANALYZE ONLY part_dist;

\c - - - :worker_1_port
SET search_path TO pg18_vacuum_part_dist;

SELECT max(last_analyze) = :'analyze_before_only'::timestamptz
       AS analyze_only_partitioned_skipped
FROM pg_stat_user_tables
WHERE schemaname = 'pg18_vacuum_part_dist'
  AND relname LIKE 'part_dist_%';

\c - - - :master_port
SET search_path TO pg18_vacuum_part_dist;

-- VACUUM (no ONLY) should recurse into partitions and shard placements
VACUUM part_dist;

\c - - - :worker_1_port
SET search_path TO pg18_vacuum_part_dist;

SELECT coalesce(max(last_vacuum), 'epoch'::timestamptz) AS vacuum_before_only
FROM pg_stat_user_tables
WHERE schemaname = 'pg18_vacuum_part_dist'
  AND relname LIKE 'part_dist_%'
\gset

\c - - - :master_port
SET search_path TO pg18_vacuum_part_dist;

-- VACUUM ONLY parent: core warns and does no work; Citus must not
-- propagate to shard placements.
VACUUM ONLY part_dist;

\c - - - :worker_1_port
SET search_path TO pg18_vacuum_part_dist;

SELECT max(last_vacuum) = :'vacuum_before_only'::timestamptz
       AS vacuum_only_partitioned_skipped
FROM pg_stat_user_tables
WHERE schemaname = 'pg18_vacuum_part_dist'
  AND relname LIKE 'part_dist_%';

\c - - - :master_port
SET search_path TO pg18_vacuum_part_dist;

DROP SCHEMA pg18_vacuum_part_dist CASCADE;
SET search_path TO pg18_nn;

-- END PG18 Feature: VACUUM/ANALYZE ONLY on partitioned distributed table

-- PG18 Feature: text search with nondeterministic collations
-- PG18 commit: https://github.com/postgres/postgres/commit/329304c90

-- This test verifies that the PG18 tests apply to Citus tables; Citus
-- just passes through the collation info and text search queries to
-- worker shards.

CREATE COLLATION ignore_accents (provider = icu, locale = '@colStrength=primary;colCaseLevel=yes', deterministic = false);
-- nondeterministic collations
CREATE COLLATION ctest_det (provider = icu, locale = '', deterministic = true);
CREATE COLLATION ctest_nondet (provider = icu, locale = '', deterministic = false);

CREATE TABLE strtest1 (a int, b text);
SELECT create_distributed_table('strtest1', 'a');

INSERT INTO strtest1 VALUES (1, U&'zy\00E4bc');
INSERT INTO strtest1 VALUES (2, U&'zy\0061\0308bc');
INSERT INTO strtest1 VALUES (3, U&'ab\00E4cd');
INSERT INTO strtest1 VALUES (4, U&'ab\0061\0308cd');
INSERT INTO strtest1 VALUES (5, U&'ab\00E4cd');
INSERT INTO strtest1 VALUES (6, U&'ab\0061\0308cd');
INSERT INTO strtest1 VALUES (7, U&'ab\00E4cd');

SELECT * FROM strtest1 WHERE b = 'zyäbc' COLLATE ctest_det ORDER BY a;
SELECT * FROM strtest1 WHERE b = 'zyäbc' COLLATE ctest_nondet ORDER BY a;

SELECT strpos(b COLLATE ctest_det, 'bc') FROM strtest1 ORDER BY a;
SELECT strpos(b COLLATE ctest_nondet, 'bc') FROM strtest1 ORDER BY a;

SELECT replace(b COLLATE ctest_det, U&'\00E4b', 'X') FROM strtest1 ORDER BY a;
SELECT replace(b COLLATE ctest_nondet, U&'\00E4b', 'X') FROM strtest1 ORDER BY a;

SELECT a, split_part(b COLLATE ctest_det, U&'\00E4b', 2) FROM strtest1 ORDER BY a;
SELECT a, split_part(b COLLATE ctest_nondet, U&'\00E4b', 2) FROM strtest1 ORDER BY a;
SELECT a, split_part(b COLLATE ctest_det, U&'\00E4b', -1) FROM strtest1 ORDER BY a;
SELECT a, split_part(b COLLATE ctest_nondet, U&'\00E4b', -1) FROM strtest1 ORDER BY a;

SELECT a, string_to_array(b COLLATE ctest_det, U&'\00E4b') FROM strtest1 ORDER BY a;
SELECT a, string_to_array(b COLLATE ctest_nondet, U&'\00E4b') FROM strtest1 ORDER BY a;

SELECT * FROM strtest1 WHERE b LIKE 'zyäbc' COLLATE ctest_det ORDER BY a;
SELECT * FROM strtest1 WHERE b LIKE 'zyäbc' COLLATE ctest_nondet ORDER BY a;

CREATE TABLE strtest2 (a int, b text);
SELECT create_distributed_table('strtest2', 'a');
INSERT INTO strtest2 VALUES (1, 'cote'), (2, 'côte'), (3, 'coté'), (4, 'côté');

CREATE TABLE strtest2nfd (a int, b text);
SELECT create_distributed_table('strtest2nfd', 'a');
INSERT INTO strtest2nfd VALUES (1, 'cote'), (2, 'côte'), (3, 'coté'), (4, 'côté');

UPDATE strtest2nfd SET b = normalize(b, nfd);

-- This shows why replace should be greedy.  Otherwise, in the NFD
-- case, the match would stop before the decomposed accents, which
-- would leave the accents in the results.
SELECT a, b, replace(b COLLATE ignore_accents, 'co', 'ma') FROM strtest2 ORDER BY a, b;
SELECT a, b, replace(b COLLATE ignore_accents, 'co', 'ma') FROM strtest2nfd ORDER BY a, b;

-- PG18 Feature: LIKE support for non-deterministic collations
-- PG18 commit: https://github.com/postgres/postgres/commit/85b7efa1c

-- As with non-deterministic collation text search, we verify that
-- LIKE with non-deterministic collation is passed through by Citus
-- and expected results are returned by the queries.

INSERT INTO strtest1 VALUES (8, U&'abc');
INSERT INTO strtest1 VALUES (9, 'abc');

SELECT a, b FROM strtest1
WHERE b LIKE 'abc' COLLATE ctest_det
ORDER BY a;

SELECT a, b FROM strtest1
WHERE b LIKE 'a\bc' COLLATE ctest_det
ORDER BY a;

SELECT a, b FROM strtest1
WHERE b LIKE 'abc' COLLATE ctest_nondet
ORDER BY a;

SELECT a, b FROM strtest1
WHERE b LIKE 'a\bc' COLLATE ctest_nondet
ORDER BY a;

CREATE COLLATION case_insensitive (provider = icu, locale = '@colStrength=secondary', deterministic = false);

SELECT a, b FROM strtest1
WHERE b LIKE 'ABC' COLLATE case_insensitive
ORDER BY a;

SELECT a, b FROM strtest1
WHERE b LIKE 'ABC%' COLLATE case_insensitive
ORDER BY a;

INSERT INTO strtest1 VALUES (10, U&'\00E4bc');
INSERT INTO strtest1 VALUES (12, U&'\0061\0308bc');

SELECT * FROM strtest1
WHERE b LIKE 'äbc' COLLATE ctest_det
ORDER BY a;

SELECT * FROM strtest1
WHERE b LIKE 'äbc' COLLATE ctest_nondet
ORDER BY a;

-- Tests with ignore_accents collation. Taken from
-- PG18 regress tests and applied to a Citus table.

INSERT INTO strtest1 VALUES (10, U&'\0061\0308bc');
INSERT INTO strtest1 VALUES (11, U&'\00E4bc');
INSERT INTO strtest1 VALUES (12, U&'cb\0061\0308');
INSERT INTO strtest1 VALUES (13, U&'\0308bc');
INSERT INTO strtest1 VALUES (14, 'foox');

SELECT a, b FROM strtest1
WHERE b LIKE U&'\00E4_c' COLLATE ignore_accents ORDER BY a, b;
-- and in reverse:
SELECT a, b FROM strtest1
WHERE b LIKE U&'\0061\0308_c' COLLATE ignore_accents ORDER BY a, b;
-- inner % matches b:
SELECT a, b FROM strtest1
WHERE b LIKE U&'\00E4%c' COLLATE ignore_accents ORDER BY a, b;
-- inner %% matches b then zero:
SELECT a, b FROM strtest1
WHERE b LIKE U&'\00E4%%c' COLLATE ignore_accents ORDER BY a, b;
-- inner %% matches b then zero:
SELECT a, b FROM strtest1
WHERE b LIKE U&'c%%\00E4' COLLATE ignore_accents ORDER BY a, b;
-- trailing _ matches two codepoints that form one grapheme:
SELECT a, b FROM strtest1
WHERE b LIKE U&'cb_' COLLATE ignore_accents ORDER BY a, b;
-- trailing __ matches two codepoints that form one grapheme:
SELECT a, b FROM strtest1
WHERE b LIKE U&'cb__' COLLATE ignore_accents ORDER BY a, b;
-- leading % matches zero:
SELECT a, b FROM strtest1
WHERE b LIKE U&'%\00E4bc' COLLATE ignore_accents
ORDER BY a;

-- leading % matches zero (with later %):
SELECT a, b FROM strtest1
WHERE b LIKE U&'%\00E4%c' COLLATE ignore_accents ORDER BY a, b;
-- trailing % matches zero:
SELECT a, b FROM strtest1
WHERE b LIKE U&'\00E4bc%' COLLATE ignore_accents ORDER BY a, b;
-- trailing % matches zero (with previous %):
SELECT a, b FROM strtest1
WHERE b LIKE U&'\00E4%c%' COLLATE ignore_accents ORDER BY a, b;
-- _ versus two codepoints that form one grapheme:
SELECT a, b FROM strtest1
WHERE b LIKE U&'_bc' COLLATE ignore_accents ORDER BY a, b;
-- (actually this matches because)
SELECT a, b FROM strtest1
WHERE b = 'bc' COLLATE ignore_accents ORDER BY a, b;
-- __ matches two codepoints that form one grapheme:
SELECT a, b FROM strtest1
WHERE b LIKE U&'__bc' COLLATE ignore_accents ORDER BY a, b;
-- _ matches one codepoint that forms half a grapheme:
SELECT a, b FROM strtest1
WHERE b LIKE U&'_\0308bc' COLLATE ignore_accents ORDER BY a, b;
-- doesn't match because \00e4 doesn't match only \0308
SELECT a, b FROM strtest1
WHERE b LIKE U&'_\00e4bc' COLLATE ignore_accents ORDER BY a, b;
-- escape character at end of pattern
SELECT a, b FROM strtest1
WHERE b LIKE 'foo\' COLLATE ignore_accents ORDER BY a, b;

DROP TABLE strtest1;
DROP COLLATION ignore_accents;
DROP COLLATION ctest_det;
DROP COLLATION ctest_nondet;
DROP COLLATION case_insensitive;

-- PG18 Feature: GUC for CREATE DATABASE file copy method
-- PG18 commit: https://github.com/postgres/postgres/commit/f78ca6f3e

-- Citus supports the wal_log strategy only for CREATE DATABASE.
-- Here we show that the expected error (from PR #7249) occurs
-- when the file_copy strategy is attempted.

SET citus.enable_create_database_propagation=on;

SHOW file_copy_method;

-- Error output is expected here
CREATE DATABASE copied_db WITH strategy file_copy;

SET file_copy_method TO clone;

-- Also errors out, per #7249
CREATE DATABASE cloned_db WITH strategy file_copy;

RESET file_copy_method;

-- This is okay
CREATE DATABASE copied_db
    WITH strategy wal_log;

-- Show that file_copy works for ALTER DATABASE ... SET TABLESPACE

\set alter_db_tablespace :abs_srcdir '/tmp_check/ts3'
CREATE TABLESPACE alter_db_tablespace LOCATION :'alter_db_tablespace';

\c - - - :worker_1_port
\set alter_db_tablespace :abs_srcdir '/tmp_check/ts4'
CREATE TABLESPACE alter_db_tablespace LOCATION :'alter_db_tablespace';

\c - - - :worker_2_port
\set alter_db_tablespace :abs_srcdir '/tmp_check/ts5'
CREATE TABLESPACE alter_db_tablespace LOCATION :'alter_db_tablespace';

\c - - - :master_port

SET citus.enable_create_database_propagation TO on;
SET file_copy_method TO clone;
SET citus.log_remote_commands TO true;

SELECT datname, spcname
FROM pg_database d, pg_tablespace t
WHERE d.dattablespace = t.oid AND d.datname = 'copied_db';

ALTER DATABASE copied_db SET TABLESPACE alter_db_tablespace;

SELECT datname, spcname
FROM pg_database d, pg_tablespace t
WHERE d.dattablespace = t.oid AND d.datname = 'copied_db';

RESET file_copy_method;
RESET citus.log_remote_commands;

-- Enable alter_db_tablespace to be dropped
ALTER DATABASE copied_db SET TABLESPACE pg_default;

DROP DATABASE copied_db;

-- Done with DATABASE commands
RESET citus.enable_create_database_propagation;

SELECT result FROM run_command_on_all_nodes(
  $$
  DROP TABLESPACE "alter_db_tablespace"
  $$
);

-- cleanup with minimum verbosity
SET client_min_messages TO ERROR;
RESET search_path;
RESET citus.shard_count;
RESET citus.shard_replication_factor;
DROP SCHEMA pg18_nn CASCADE;
RESET client_min_messages;
