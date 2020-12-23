CREATE SCHEMA local_table_join;
SET search_path TO local_table_join;

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

INSERT INTO postgres_table SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO reference_table SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO distributed_table_windex SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO distributed_table SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO distributed_table_pkey SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO distributed_partitioned_table SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO distributed_table_composite SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO local_partitioned_table SELECT i, i::varchar(256) FROM generate_series(1, 100) i;

CREATE FUNCTION fake_fdw_handler()
RETURNS fdw_handler
AS 'citus'
LANGUAGE C STRICT;
CREATE FOREIGN DATA WRAPPER fake_fdw_1 HANDLER fake_fdw_handler;
CREATE SERVER fake_fdw_server_1 FOREIGN DATA WRAPPER fake_fdw_1;

CREATE FOREIGN TABLE foreign_table (
  key int,
  value text
) SERVER fake_fdw_server_1 OPTIONS (encoding 'utf-8', compression 'true');

CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM postgres_table;
CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM distributed_table;


SET client_min_messages TO DEBUG1;


-- the user doesn't allow local / distributed table joinn
SET citus.local_table_join_policy TO 'never';
SELECT count(*) FROM postgres_table JOIN distributed_table USING(key);
SELECT count(*) FROM postgres_table JOIN reference_table USING(key);

-- the user prefers local table recursively planned
SET citus.local_table_join_policy TO 'prefer-local';
SELECT count(*) FROM postgres_table JOIN distributed_table USING(key);
SELECT count(*) FROM postgres_table JOIN reference_table USING(key);


-- the user prefers distributed table recursively planned
SET citus.local_table_join_policy TO 'prefer-distributed';
SELECT count(*) FROM postgres_table JOIN distributed_table USING(key);
SELECT count(*) FROM postgres_table JOIN reference_table USING(key);

-- auto tests

-- switch back to the default policy, which is auto
SET citus.local_table_join_policy to 'auto';

-- on the auto mode, the local tables should be recursively planned
-- unless a unique index exists in a column for distributed table
SELECT count(*) FROM distributed_table JOIN postgres_table USING(key);
SELECT count(*) FROM reference_table JOIN postgres_table USING(key);
SELECT count(*) FROM distributed_table JOIN postgres_table USING(key) JOIN reference_table USING (key);

-- partititoned local tables should work as well
SELECT count(*) FROM distributed_table JOIN local_partitioned_table USING(key);
SELECT count(*) FROM reference_table JOIN local_partitioned_table USING(key);
SELECT count(*) FROM distributed_table JOIN local_partitioned_table USING(key) JOIN reference_table USING (key);

-- materialized views should work too
SELECT count(*) FROM distributed_table JOIN mv1 USING(key);
SELECT count(*) FROM (SELECT * FROM distributed_table) d1 JOIN mv1 USING(key);
SELECT count(*) FROM reference_table JOIN mv1 USING(key);
SELECT count(*) FROM distributed_table JOIN mv1 USING(key) JOIN reference_table USING (key);
SELECT count(*) FROM distributed_table JOIN mv2 USING(key);
SELECT count(*) FROM (SELECT * FROM distributed_table) d1 JOIN mv2 USING(key);
SELECT count(*) FROM reference_table JOIN mv2 USING(key);
SELECT count(*) FROM distributed_table JOIN mv2 USING(key) JOIN reference_table USING (key);

-- foreign tables should work too
SELECT count(*) FROM foreign_table JOIN distributed_table USING(key);

-- partitioned tables should work as well
SELECT count(*) FROM distributed_partitioned_table JOIN postgres_table USING(key);
SELECT count(*) FROM distributed_partitioned_table JOIN postgres_table USING(key) WHERE distributed_partitioned_table.key = 10;
SELECT count(*) FROM distributed_partitioned_table JOIN postgres_table USING(key) JOIN reference_table USING (key);

SELECT count(*) FROM distributed_partitioned_table JOIN local_partitioned_table USING(key);
SELECT count(*) FROM distributed_partitioned_table JOIN local_partitioned_table USING(key) WHERE distributed_partitioned_table.key = 10;
SELECT count(*) FROM distributed_partitioned_table JOIN local_partitioned_table USING(key) JOIN reference_table USING (key);

-- similar tests in transaction block should work fine

BEGIN;
-- materialized views should work too
SELECT count(*) FROM distributed_table JOIN mv1 USING(key);
SELECT count(*) FROM (SELECT * FROM distributed_table) d1 JOIN mv1 USING(key);
SELECT count(*) FROM reference_table JOIN mv1 USING(key);
SELECT count(*) FROM distributed_table JOIN mv1 USING(key) JOIN reference_table USING (key);
SELECT count(*) FROM distributed_table JOIN mv2 USING(key);
SELECT count(*) FROM (SELECT * FROM distributed_table) d1 JOIN mv2 USING(key);
SELECT count(*) FROM reference_table JOIN mv2 USING(key);
SELECT count(*) FROM distributed_table JOIN mv2 USING(key) JOIN reference_table USING (key);

-- foreign tables should work too
SELECT count(*) FROM foreign_table JOIN distributed_table USING(key);

-- partitioned tables should work as well
SELECT count(*) FROM distributed_partitioned_table JOIN postgres_table USING(key);
SELECT count(*) FROM distributed_partitioned_table JOIN postgres_table USING(key) WHERE distributed_partitioned_table.key = 10;
SELECT count(*) FROM distributed_partitioned_table JOIN postgres_table USING(key) JOIN reference_table USING (key);

SELECT count(*) FROM distributed_partitioned_table JOIN local_partitioned_table USING(key);
SELECT count(*) FROM distributed_partitioned_table JOIN local_partitioned_table USING(key) WHERE distributed_partitioned_table.key = 10;
SELECT count(*) FROM distributed_partitioned_table JOIN local_partitioned_table USING(key) JOIN reference_table USING (key);
ROLLBACK;

-- the conversions should be independent from the order of table entries in the query
SELECT COUNT(*) FROM postgres_table join distributed_table_pkey using(key) join local_partitioned_table using(key) join distributed_table using(key) where distributed_table_pkey.key = 5;
SELECT COUNT(*) FROM postgres_table join local_partitioned_table using(key) join distributed_table_pkey using(key) join distributed_table using(key) where distributed_table_pkey.key = 5;
SELECT COUNT(*) FROM postgres_table join distributed_table using(key) join local_partitioned_table using(key) join distributed_table_pkey using(key) where distributed_table_pkey.key = 5;
SELECT COUNT(*) FROM distributed_table_pkey join distributed_table using(key) join postgres_table using(key) join local_partitioned_table using(key) where distributed_table_pkey.key = 5;

SELECT count(*) FROM (SELECT *, random() FROM distributed_table) as d1  JOIN postgres_table ON (postgres_table.key = d1.key AND d1.key < postgres_table.key) WHERE d1.key = 1 AND false;
SELECT count(*) FROM (SELECT *, random() FROM distributed_table_pkey) as d1  JOIN postgres_table ON (postgres_table.key = d1.key AND d1.key < postgres_table.key) WHERE d1.key = 1 AND false;
SELECT count(*) FROM (SELECT *, random() FROM distributed_partitioned_table) as d1  JOIN postgres_table ON (postgres_table.key = d1.key AND d1.key < postgres_table.key) WHERE d1.key = 1 AND false;
SELECT count(*) FROM (SELECT *, random() FROM distributed_partitioned_table) as d1  JOIN postgres_table ON (postgres_table.key::int = d1.key::int AND d1.key < postgres_table.key) WHERE d1.key::int = 1 AND false;

-- different column names
SELECT a FROM postgres_table foo (a,b,c) JOIN distributed_table ON (distributed_table.key = foo.a) ORDER BY 1 LIMIT 1;


-- We will plan postgres table as the index is on key,value not just key
SELECT count(*) FROM distributed_table_composite JOIN postgres_table USING(key) WHERE distributed_table_composite.key = 10;
SELECT count(*) FROM distributed_table_composite JOIN postgres_table USING(key) WHERE distributed_table_composite.key = 10 OR distributed_table_composite.key = 20;
SELECT count(*) FROM distributed_table_composite JOIN postgres_table USING(key) WHERE distributed_table_composite.key > 10 AND distributed_table_composite.value = 'text';
SELECT count(*) FROM distributed_table_composite JOIN postgres_table USING(key) WHERE distributed_table_composite.key = 10 AND distributed_table_composite.value = 'text';
SELECT count(*) FROM distributed_table_composite JOIN postgres_table USING(key)
	WHERE (distributed_table_composite.key > 10 OR distributed_table_composite.key = 20)
	AND (distributed_table_composite.value = 'text' OR distributed_table_composite.value = 'text');
SELECT count(*) FROM distributed_table_composite JOIN postgres_table USING(key)
	WHERE (distributed_table_composite.key > 10 OR distributed_table_composite.value = 'text')
	AND (distributed_table_composite.value = 'text' OR distributed_table_composite.key = 30);
SELECT count(*) FROM distributed_table_composite JOIN postgres_table USING(key)
	WHERE (distributed_table_composite.key > 10 AND distributed_table_composite.value = 'text')
	OR (distributed_table_composite.value = 'text' AND distributed_table_composite.key = 30);
SELECT count(*) FROM distributed_table_composite JOIN postgres_table USING(key)
	WHERE (distributed_table_composite.key > 10 AND distributed_table_composite.key = 20)
	OR (distributed_table_composite.value = 'text' AND distributed_table_composite.value = 'text');

-- Known bug: #4269
SELECT count(*) FROM distributed_table_composite foo(a,b,c) JOIN postgres_table ON(foo.a > 1)
	WHERE foo.a IN (SELECT COUNT(*) FROM local_partitioned_table) AND (foo.a = 10 OR foo.b ='text');

-- a unique index on key so dist table should be recursively planned
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey USING(key);
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey USING(value);
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON postgres_table.key = distributed_table_pkey.key;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10;
-- it should favor distributed table only if it has equality on the unique column
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key > 10;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key < 10;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 AND distributed_table_pkey.key > 10 ;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 AND distributed_table_pkey.key > 10 ;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 AND distributed_table_pkey.key > 10 AND postgres_table.key = 5;

SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 OR distributed_table_pkey.key > 10;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 OR distributed_table_pkey.key = 20;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 OR distributed_table_pkey.key = 20 OR distributed_table_pkey.key = 30;
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 OR distributed_table_pkey.key = (
	SELECT count(*) FROM distributed_table_pkey
);
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 OR (distributed_table_pkey.key = 5 and distributed_table_pkey.key > 15);
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 OR (distributed_table_pkey.key > 10 and distributed_table_pkey.key > 15);
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 OR (distributed_table_pkey.key > 10 and distributed_table_pkey.value = 'notext');
SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON distributed_table_pkey.key = 10 OR (distributed_table_pkey.key = 10 and distributed_table_pkey.value = 'notext');

SELECT count(*) FROM postgres_table JOIN distributed_table_pkey ON postgres_table.key = 10;


select count(*) FROM postgres_table JOIN (SELECT a.key,random() FROM distributed_table a JOIN distributed_table b USING(key)) as foo USING(key);
select count(*) FROM (SELECT a.key, random() FROM distributed_table a JOIN distributed_table b USING(key)) as foo JOIN postgres_table  USING(key);

SELECT count(*) FROM postgres_table JOIN (SELECT * FROM distributed_table) d1 USING(key);
-- since this is already router plannable, we don't recursively plan the postgres table
SELECT count(*) FROM postgres_table JOIN (SELECT * FROM distributed_table LIMIT 1) d1 USING(key);

-- a unique index on key so dist table should be recursively planned
SELECT count(*) FROM postgres_table JOIN distributed_table_windex USING(key);
SELECT count(*) FROM postgres_table JOIN distributed_table_windex USING(value);
SELECT count(*) FROM postgres_table JOIN distributed_table_windex ON postgres_table.key = distributed_table_windex.key;
SELECT count(*) FROM postgres_table JOIN distributed_table_windex ON distributed_table_windex.key = 10;

-- no unique index on value so local table should be recursively planned.
SELECT count(*) FROM distributed_table JOIN postgres_table USING(key) WHERE distributed_table.value = 'test';

SELECT count(*) FROM distributed_table JOIN postgres_table USING(key) WHERE distributed_table.key = 1;


-- if both local and distributed tables have a filter, we prefer local unless distributed table has unique indexes on any equality filter
SELECT count(*) FROM distributed_table JOIN postgres_table USING(key) WHERE distributed_table.value = 'test' AND postgres_table.value = 'test';
SELECT count(*) FROM distributed_table JOIN postgres_table USING(key) WHERE distributed_table.value = 'test' OR postgres_table.value = 'test';


-- multiple local/distributed tables
-- only local tables are recursively planned
SELECT count(*) FROM distributed_table d1 JOIN postgres_table p1 USING(key) JOIN distributed_table d2 USING(key) JOIN postgres_table p2 USING(key);


SELECT
	count(*)
FROM
	distributed_table d1 JOIN postgres_table p1 USING(key) JOIN distributed_table d2 USING(key) JOIN postgres_table p2 USING(key)
WHERE
	d1.value = '1';

-- if the filter is on the JOIN key, we can recursively plan the local
-- tables as filters are pushed down to the local tables
SELECT
	count(*)
FROM
	distributed_table d1 JOIN postgres_table p1 USING(key) JOIN distributed_table d2 USING(key) JOIN postgres_table p2 USING(key)
WHERE
	d1.key = 1;

CREATE view loc_view AS SELECT * FROM postgres_table WHERE key > 0;
UPDATE loc_view SET key = (SELECT COUNT(*) FROM distributed_table);

SELECT count(*)
FROM
	(SELECT * FROM (SELECT * FROM distributed_table) d1) d2
JOIN postgres_table
USING(key);

-- will error as we don't support complex joins
SELECT COUNT(*) FROM postgres_table, distributed_table d1, distributed_table d2 WHERE d1.value = d2.value;

-- This will error because router planner will think that since reference tables have a single
-- shard, it contains only a single task for modify. However, updating a reference tables
-- will require multiple tasks. So requires some rewrite in router planner.
UPDATE reference_table SET key = 1 FROM postgres_table WHERE postgres_table.key = 10;
UPDATE reference_table SET key = 1 FROM (SELECT * FROM postgres_table) l WHERE l.key = 10;


SELECT count(*) FROM postgres_table JOIN distributed_table USING(key) WHERE FALSE;

SELECT count(*) FROM (SELECT * FROM distributed_table JOIN postgres_table USING(key) WHERE false) foo JOIN local_partitioned_table USING(key);

WITH dist_cte AS (SELECT * FROM distributed_table_pkey WHERE key = 5)
SELECT COUNT(*) FROM dist_cte JOIN postgres_table USING(key) WHERE dist_cte.key = 5;

SELECT COUNT(*) FROM postgres_table JOIN distributed_table_pkey USING(key)
	WHERE (distributed_table_pkey.key IN (SELECT COUNT(*) AS count FROM postgres_table JOIN distributed_table USING(key)) );

-- PREPARED statements
PREPARE local_dist_table_join_select(int) AS SELECT COUNT(*) FROM distributed_table_pkey JOIN postgres_table USING(key) WHERE distributed_table_pkey.key = $1;

EXECUTE local_dist_table_join_select(10);
EXECUTE local_dist_table_join_select(10);
EXECUTE local_dist_table_join_select(10);
EXECUTE local_dist_table_join_select(10);
EXECUTE local_dist_table_join_select(10);
EXECUTE local_dist_table_join_select(10);

PREPARE local_dist_table_join_update(int) AS UPDATE postgres_table SET key = 5 FROM distributed_table_pkey  WHERE distributed_table_pkey.key = $1;

EXECUTE local_dist_table_join_update(20);
EXECUTE local_dist_table_join_update(20);
EXECUTE local_dist_table_join_update(20);
EXECUTE local_dist_table_join_update(20);
EXECUTE local_dist_table_join_update(20);
EXECUTE local_dist_table_join_update(20);

PREPARE local_dist_table_join_subquery(int) AS SELECT COUNT(*) FROM postgres_table JOIN (SELECT * FROM distributed_table_pkey JOIN local_partitioned_table USING(key) WHERE distributed_table_pkey.key = $1) foo USING(key);

EXECUTE local_dist_table_join_subquery(5);
EXECUTE local_dist_table_join_subquery(5);
EXECUTE local_dist_table_join_subquery(5);
EXECUTE local_dist_table_join_subquery(5);
EXECUTE local_dist_table_join_subquery(5);
EXECUTE local_dist_table_join_subquery(5);

PREPARE local_dist_table_join_filters(int) AS SELECT COUNT(*) FROM local_partitioned_table JOIN distributed_table_composite USING(key)
	WHERE(
		distributed_table_composite.key = $1 OR
		distributed_table_composite.key = 20 OR
		(distributed_table_composite.key = 10 AND distributed_table_composite.key > 0) OR
		distributed_table_composite.value = 'text'
		);

EXECUTE local_dist_table_join_filters(20);
EXECUTE local_dist_table_join_filters(20);
EXECUTE local_dist_table_join_filters(20);
EXECUTE local_dist_table_join_filters(20);
EXECUTE local_dist_table_join_filters(20);
EXECUTE local_dist_table_join_filters(20);

CREATE TABLE local (key1 int, key2 int, key3 int);
INSERT INTO local VALUES (1,2,3);
ALTER TABLE local DROP column key2;
-- make sure dropped columns work
SELECT COUNT(*) FROM local JOIN distributed_table ON(key1 = key);
SELECT * FROM local JOIN distributed_table ON(key1 = key) ORDER BY 1 LIMIT 1;
SELECT * FROM (SELECT local.key1, local.key3 FROM local JOIN distributed_table
 ON(local.key1 = distributed_table.key) GROUP BY local.key1, local.key3) a ORDER BY 1,2;

SELECT * FROM (SELECT local.key3 FROM local JOIN distributed_table
 ON(local.key1 = distributed_table.key) GROUP BY local.key3) a ORDER BY 1;

SELECT a.key3 FROM (SELECT local.key3 FROM local JOIN distributed_table
 ON(local.key1 = distributed_table.key) GROUP BY local.key3) a ORDER BY 1;

-- drop all the remaining columns
ALTER TABLE local DROP column key3;
ALTER TABLE local DROP column key1;
SELECT COUNT(*) FROM distributed_table JOIN local ON distributed_table.value = 'text';


RESET client_min_messages;
\set VERBOSITY terse
DROP SCHEMA local_table_join CASCADE;
