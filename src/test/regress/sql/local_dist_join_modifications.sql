CREATE SCHEMA local_dist_join_modifications;
SET search_path TO local_dist_join_modifications;

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

SET citus.local_table_join_policy to 'auto';

-- we can support modification queries as well
BEGIN;
SELECT COUNT(DISTINCT value) FROM postgres_table;
UPDATE
	postgres_table
SET
	value = 'test'
FROM
	distributed_table
WHERE
	distributed_table.key = postgres_table.key;
SELECT COUNT(DISTINCT value) FROM postgres_table;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table;
UPDATE
	distributed_table
SET
	value = 'test'
FROM
	postgres_table
WHERE
	distributed_table.key = postgres_table.key;
SELECT COUNT(DISTINCT value) FROM distributed_table;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table_pkey;
UPDATE
	distributed_table_pkey
SET
	value = 'test'
FROM
	postgres_table
WHERE
	distributed_table_pkey.key = postgres_table.key;
SELECT COUNT(DISTINCT value) FROM distributed_table_pkey;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table_windex;
UPDATE
	distributed_table_windex
SET
	value = 'test'
FROM
	postgres_table
WHERE
	distributed_table_windex.key = postgres_table.key;
SELECT COUNT(DISTINCT value) FROM distributed_table_windex;
ROLLBACK;

BEGIN;
UPDATE
	mv1
SET
	value = 'test'
FROM
	postgres_table
WHERE
	mv1.key = postgres_table.key;
ROLLBACK;

BEGIN;
UPDATE
	postgres_table
SET
	value = 'test'
FROM
	mv1
WHERE
	mv1.key = postgres_table.key;
ROLLBACK;

BEGIN;
UPDATE
	postgres_table
SET
	value = 'test'
FROM
	mv2
WHERE
	mv2.key = postgres_table.key;
ROLLBACK;

-- in case of update/delete we always recursively plan
-- the tables other than target table no matter what the policy is

SET citus.local_table_join_policy TO 'prefer-local';

BEGIN;
SELECT COUNT(DISTINCT value) FROM postgres_table;
UPDATE
	postgres_table
SET
	value = 'test'
FROM
	distributed_table
WHERE
	distributed_table.key = postgres_table.key;
SELECT COUNT(DISTINCT value) FROM postgres_table;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table;
UPDATE
	distributed_table
SET
	value = 'test'
FROM
	postgres_table
WHERE
	distributed_table.key = postgres_table.key;
SELECT COUNT(DISTINCT value) FROM distributed_table;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table_pkey;
UPDATE
	distributed_table_pkey
SET
	value = 'test'
FROM
	postgres_table
WHERE
	distributed_table_pkey.key = postgres_table.key;
SELECT COUNT(DISTINCT value) FROM distributed_table_pkey;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table_windex;
UPDATE
	distributed_table_windex
SET
	value = 'test'
FROM
	postgres_table
WHERE
	distributed_table_windex.key = postgres_table.key;
SELECT COUNT(DISTINCT value) FROM distributed_table_windex;
ROLLBACK;

SET citus.local_table_join_policy TO 'prefer-distributed';

BEGIN;
SELECT COUNT(DISTINCT value) FROM postgres_table;
UPDATE
	postgres_table
SET
	value = 'test'
FROM
	distributed_table
WHERE
	distributed_table.key = postgres_table.key;
SELECT COUNT(DISTINCT value) FROM postgres_table;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table;
UPDATE
	distributed_table
SET
	value = 'test'
FROM
	postgres_table
WHERE
	distributed_table.key = postgres_table.key;
SELECT COUNT(DISTINCT value) FROM distributed_table;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table_pkey;
UPDATE
	distributed_table_pkey
SET
	value = 'test'
FROM
	postgres_table
WHERE
	distributed_table_pkey.key = postgres_table.key;
SELECT COUNT(DISTINCT value) FROM distributed_table_pkey;
ROLLBACK;


BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table_windex;
UPDATE
	distributed_table_windex
SET
	value = 'test'
FROM
	postgres_table
WHERE
	distributed_table_windex.key = postgres_table.key;
SELECT COUNT(DISTINCT value) FROM distributed_table_windex;
ROLLBACK;

SET citus.local_table_join_policy TO 'auto';

-- modifications with multiple tables
BEGIN;
UPDATE
	distributed_table
SET
	value = 'test'
FROM
	postgres_table p1, postgres_table p2
WHERE
	distributed_table.key = p1.key AND p1.key = p2.key;
ROLLBACK;

BEGIN;
UPDATE
	postgres_table
SET
	value = 'test'
FROM
	(SELECT * FROM distributed_table) d1
WHERE
	d1.key = postgres_table.key;
ROLLBACK;

BEGIN;
UPDATE
	postgres_table
SET
	value = 'test'
FROM
	(SELECT * FROM distributed_table LIMIT 1) d1
WHERE
	d1.key = postgres_table.key;
ROLLBACK;

BEGIN;
UPDATE
	distributed_table
SET
	value = 'test'
FROM
	postgres_table p1, distributed_table d2
WHERE
	distributed_table.key = p1.key AND p1.key = d2.key;
ROLLBACK;

-- pretty inefficient plan as it requires
-- recursive planninng of 2 distributed tables
BEGIN;
UPDATE
	postgres_table
SET
	value = 'test'
FROM
	distributed_table d1, distributed_table d2
WHERE
	postgres_table.key = d1.key AND d1.key = d2.key;
ROLLBACK;
-- DELETE operations

BEGIN;
SELECT COUNT(DISTINCT value) FROM postgres_table;
DELETE FROM
	postgres_table
USING
	distributed_table
WHERE
	distributed_table.key = postgres_table.key;
SELECT COUNT(DISTINCT value) FROM postgres_table;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table;
DELETE FROM
	distributed_table
USING
	postgres_table
WHERE
	distributed_table.key = postgres_table.key;
SELECT COUNT(DISTINCT value) FROM distributed_table;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table_pkey;
DELETE FROM
	distributed_table_pkey
USING
	postgres_table
WHERE
	distributed_table_pkey.key = postgres_table.key;
SELECT COUNT(DISTINCT value) FROM distributed_table_pkey;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT value) FROM distributed_table_windex;
DELETE FROM
	distributed_table_windex
USING
	postgres_table
WHERE
	distributed_table_windex.key = postgres_table.key;
SELECT COUNT(DISTINCT value) FROM distributed_table_windex;
ROLLBACK;

DELETE FROM
	mv1
USING
	postgres_table
WHERE
	mv1.key = postgres_table.key;

DELETE FROM
	postgres_table
USING
	mv1
WHERE
	mv1.key = postgres_table.key;

DELETE FROM
	postgres_table
USING
	mv2
WHERE
	mv2.key = postgres_table.key;


SET client_min_messages to ERROR;
DROP SCHEMA local_dist_join_modifications CASCADE;
