CREATE SCHEMA citus_local_dist_joins;
SET search_path TO citus_local_dist_joins;

SET client_min_messages to ERROR;

CREATE TABLE citus_local(key int, value text);
SELECT citus_add_local_table_to_metadata('citus_local');

CREATE TABLE postgres_table (key int, value text, value_2 jsonb);
CREATE TABLE reference_table (key int, value text, value_2 jsonb);
SELECT create_reference_table('reference_table');
CREATE TABLE distributed_table (key int, value text, value_2 jsonb);
SELECT create_distributed_table('distributed_table', 'key');

CREATE TABLE distributed_table_pkey (key int primary key, value text, value_2 jsonb);
SELECT create_distributed_table('distributed_table_pkey', 'key');

CREATE TABLE distributed_table_windex (key int primary key, value text, value_2 jsonb);
SELECT create_distributed_table('distributed_table_windex', 'key');
CREATE UNIQUE INDEX key_index ON distributed_table_windex (key);

CREATE TABLE distributed_partitioned_table(key int, value text) PARTITION BY RANGE (key);
CREATE TABLE distributed_partitioned_table_1 PARTITION OF distributed_partitioned_table FOR VALUES FROM (0) TO (50);
CREATE TABLE distributed_partitioned_table_2 PARTITION OF distributed_partitioned_table FOR VALUES FROM (50) TO (200);
SELECT create_distributed_table('distributed_partitioned_table', 'key');

CREATE TABLE local_partitioned_table(key int, value text) PARTITION BY RANGE (key);
CREATE TABLE local_partitioned_table_1 PARTITION OF local_partitioned_table FOR VALUES FROM (0) TO (50);
CREATE TABLE local_partitioned_table_2 PARTITION OF local_partitioned_table FOR VALUES FROM (50) TO (200);

CREATE TABLE distributed_table_composite (key int, value text, value_2 jsonb, primary key (key, value));
SELECT create_distributed_table('distributed_table_composite', 'key');

CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM postgres_table;
CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM distributed_table;

-- set log messages to debug1 so that we can see which tables are recursively planned.
SET client_min_messages TO DEBUG1;

INSERT INTO postgres_table SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO reference_table SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO distributed_table_windex SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO distributed_table SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO distributed_table_pkey SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO distributed_partitioned_table SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO distributed_table_composite SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO local_partitioned_table SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO citus_local SELECT i, i::varchar(256) FROM generate_series(1, 100) i;


-- a unique index on key so dist table should be recursively planned
SELECT count(*) FROM citus_local JOIN distributed_table_windex USING(key);
SELECT count(*) FROM citus_local JOIN distributed_table_windex USING(value);
SELECT count(*) FROM citus_local JOIN distributed_table_windex ON citus_local.key = distributed_table_windex.key;
SELECT count(*) FROM citus_local JOIN distributed_table_windex ON distributed_table_windex.key = 10;

-- no unique index, citus local table should be recursively planned
SELECT count(*) FROM citus_local JOIN distributed_table USING(key);
SELECT count(*) FROM citus_local JOIN distributed_table USING(value);
SELECT count(*) FROM citus_local JOIN distributed_table ON citus_local.key = distributed_table.key;
SELECT count(*) FROM citus_local JOIN distributed_table ON distributed_table.key = 10;

SELECT count(*) FROM citus_local JOIN distributed_table USING(key) JOIN postgres_table USING (key) JOIN reference_table USING(key);

SELECT count(*) FROM distributed_partitioned_table JOIN postgres_table USING(key) JOIN reference_table USING (key)
	JOIN citus_local USING(key) WHERE distributed_partitioned_table.key > 10 and distributed_partitioned_table.key = 10;

-- update
BEGIN;
SELECT COUNT(DISTINCT value) FROM citus_local;
UPDATE
	citus_local
SET
	value = 'test'
FROM
	distributed_table
WHERE
	distributed_table.key = citus_local.key;
SELECT COUNT(DISTINCT value) FROM citus_local;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table;
UPDATE
	distributed_table
SET
	value = 'test'
FROM
	citus_local
WHERE
	distributed_table.key = citus_local.key;
SELECT COUNT(DISTINCT value) FROM distributed_table;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table_pkey;
UPDATE
	distributed_table_pkey
SET
	value = 'test'
FROM
	citus_local
WHERE
	distributed_table_pkey.key = citus_local.key;
SELECT COUNT(DISTINCT value) FROM distributed_table_pkey;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table_windex;
UPDATE
	distributed_table_windex
SET
	value = 'test'
FROM
	citus_local
WHERE
	distributed_table_windex.key = citus_local.key;
SELECT COUNT(DISTINCT value) FROM distributed_table_windex;
ROLLBACK;

BEGIN;
UPDATE
	mv1
SET
	value = 'test'
FROM
	citus_local
WHERE
	mv1.key = citus_local.key;
ROLLBACK;

BEGIN;
UPDATE
	citus_local
SET
	value = 'test'
FROM
	mv1
WHERE
	mv1.key = citus_local.key;
ROLLBACK;

BEGIN;
UPDATE
	citus_local
SET
	value = 'test'
FROM
	mv2
WHERE
	mv2.key = citus_local.key;
ROLLBACK;

-- DELETE operations

BEGIN;
SELECT COUNT(DISTINCT value) FROM citus_local;
DELETE FROM
	citus_local
USING
	distributed_table
WHERE
	distributed_table.key = citus_local.key;
SELECT COUNT(DISTINCT value) FROM citus_local;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table;
DELETE FROM
	distributed_table
USING
	citus_local
WHERE
	distributed_table.key = citus_local.key;
SELECT COUNT(DISTINCT value) FROM distributed_table;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table_pkey;
DELETE FROM
	distributed_table_pkey
USING
	citus_local
WHERE
	distributed_table_pkey.key = citus_local.key;
SELECT COUNT(DISTINCT value) FROM distributed_table_pkey;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table_windex;
DELETE FROM
	distributed_table_windex
USING
	citus_local
WHERE
	distributed_table_windex.key = citus_local.key;
SELECT COUNT(DISTINCT value) FROM distributed_table_windex;
ROLLBACK;

DELETE FROM
	mv1
USING
	citus_local
WHERE
	mv1.key = citus_local.key;

DELETE FROM
	citus_local
USING
	mv1
WHERE
	mv1.key = citus_local.key;

DELETE FROM
	citus_local
USING
	mv2
WHERE
	mv2.key = citus_local.key;

SELECT count(*) FROM postgres_table JOIN (SELECT * FROM (SELECT * FROM distributed_table LIMIT 1) d1) d2 using (key) JOIN reference_table USING(key) JOIN citus_local USING (key) JOIN (SELECT * FROM citus_local) c1  USING (key) WHERE d2.key > 10 AND d2.key = 10;
SELECT count(*) FROM postgres_table JOIN (SELECT * FROM (SELECT * FROM distributed_table LIMIT 1) d1) d2 using (key) JOIN reference_table USING(key) JOIN citus_local USING (key) JOIN (SELECT * FROM citus_local) c1  USING (key) WHERE d2.key > 10 AND d2.key = 10;


SELECT
	COUNT(*)
FROM
	postgres_table p1
JOIN
	distributed_partitioned_table dp1
USING (key)
JOIN
	distributed_table d1
USING (key)
JOIN
	citus_local c1
USING (key)
JOIN
	postgres_table p2
USING (key)
JOIN
	reference_table r1
USING (key)
JOIN
	distributed_table d2
USING (key)
JOIN
	citus_local c2
USING (key);

-- prefer-distributed option causes recursive planner passes the query 2 times and errors out
-- planner recursively plans one of the distributed_table in its first pass. Then, at its second
-- pass, it also recursively plans other distributed_table as modification at first step caused it.
SET citus.local_table_join_policy TO 'prefer-distributed';

SELECT
	COUNT(*)
FROM
	postgres_table
JOIN
	distributed_table
USING
	(key)
JOIN
	(SELECT  key, NULL, NULL FROM distributed_table) foo
USING
	(key);

RESET citus.local_table_join_policy;


SET client_min_messages to ERROR;
DROP TABLE citus_local;
\set VERBOSITY terse
DROP SCHEMA citus_local_dist_joins CASCADE;
