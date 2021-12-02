-- Test queries on a distributed table with shards on the coordinator

CREATE SCHEMA coordinator_shouldhaveshards;
SET search_path TO coordinator_shouldhaveshards;
SET citus.next_shard_id TO 1503000;
SET citus.next_placement_id TO 1503000;

-- idempotently add node to allow this test to run without add_coordinator
SET client_min_messages TO WARNING;
SELECT 1 FROM master_add_node('localhost', :master_port, groupid => 0);
RESET client_min_messages;

SELECT 1 FROM master_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

SET citus.shard_replication_factor TO 1;

CREATE TABLE test (x int, y int);
SELECT create_distributed_table('test','x', colocate_with := 'none');

SELECT count(*) FROM pg_dist_shard JOIN pg_dist_placement USING (shardid)
WHERE logicalrelid = 'test'::regclass AND groupid = 0;

--- enable logging to see which tasks are executed locally
SET client_min_messages TO LOG;
SET citus.log_local_commands TO ON;

-- INSERT..SELECT with COPY under the covers
INSERT INTO test SELECT s,s FROM generate_series(2,100) s;

-- router queries execute locally
INSERT INTO test VALUES (1, 1);
SELECT y FROM test WHERE x = 1;

-- multi-shard queries connect to localhost
SELECT count(*) FROM test;
WITH a AS (SELECT * FROM test) SELECT count(*) FROM test;

-- multi-shard queries in transaction blocks execute locally
BEGIN;
SELECT y FROM test WHERE x = 1;
SELECT count(*) FROM test;
END;

BEGIN;
SELECT y FROM test WHERE x = 1;
SELECT count(*) FROM test;
END;

-- INSERT..SELECT with re-partitioning after local execution
BEGIN;
INSERT INTO test VALUES (0,1000);
CREATE TABLE repart_test (x int primary key, y int);
SELECT create_distributed_table('repart_test','x', colocate_with := 'none');

INSERT INTO repart_test (x, y) SELECT y, x FROM test;

SELECT y FROM repart_test WHERE x = 1000;
INSERT INTO repart_test (x, y) SELECT y, x FROM test ON CONFLICT (x) DO UPDATE SET y = -1;
SELECT y FROM repart_test WHERE x = 1000;
ROLLBACK;

-- INSERT..SELECT with re-partitioning in EXPLAIN ANALYZE after local execution
BEGIN;
INSERT INTO test VALUES (0,1000);
EXPLAIN (COSTS FALSE, ANALYZE TRUE, TIMING FALSE, SUMMARY FALSE) INSERT INTO test (x, y) SELECT y, x FROM test;
ROLLBACK;

-- DDL connects to locahost
ALTER TABLE test ADD COLUMN z int;

-- DDL after local execution
BEGIN;
SELECT y FROM test WHERE x = 1;
ALTER TABLE test DROP COLUMN z;
ROLLBACK;

BEGIN;
ALTER TABLE test DROP COLUMN z;
SELECT y FROM test WHERE x = 1;
END;

SET citus.shard_count TO 6;
SET citus.log_remote_commands TO OFF;


BEGIN;
SET citus.log_local_commands TO ON;
CREATE TABLE dist_table (a int);
INSERT INTO dist_table SELECT * FROM generate_series(1, 100);
-- trigger local execution
SELECT y FROM test WHERE x = 1;
-- this should be run locally
SELECT create_distributed_table('dist_table', 'a', colocate_with := 'none');
SELECT count(*) FROM dist_table;
ROLLBACK;

CREATE TABLE dist_table (a int);

INSERT INTO dist_table SELECT * FROM generate_series(1, 100);

BEGIN;
SET citus.log_local_commands TO ON;
-- trigger local execution
SELECT y FROM test WHERE x = 1;
-- this should be run locally
SELECT create_distributed_table('dist_table', 'a', colocate_with := 'none');
SELECT count(*) FROM dist_table;
ROLLBACK;

-- repartition queries should work fine
SET citus.enable_repartition_joins TO ON;
SELECT count(*) FROM test t1, test t2 WHERE t1.x = t2.y;

BEGIN;
SET citus.enable_repartition_joins TO ON;
SELECT count(*) FROM test t1, test t2 WHERE t1.x = t2.y;
END;

BEGIN;
SET citus.enable_repartition_joins TO ON;
-- trigger local execution
SELECT y FROM test WHERE x = 1;
SELECT count(*) FROM test t1, test t2 WHERE t1.x = t2.y;
ROLLBACK;

CREATE TABLE ref (a int, b int);
SELECT create_reference_table('ref');

CREATE TABLE local (x int, y int);

BEGIN;
SELECT count(*) FROM test;
SELECT * FROM ref JOIN local ON (a = x);
TRUNCATE ref;
ROLLBACK;


BEGIN;
SELECT count(*) FROM test;
TRUNCATE ref;
SELECT * FROM ref JOIN local ON (a = x);
ROLLBACK;

BEGIN;
SELECT count(*) FROM test;
INSERT INTO ref VALUES (1,2);
INSERT INTO local VALUES (1,2);
SELECT * FROM ref JOIN local ON (a = x);
ROLLBACK;


BEGIN;
SELECT count(*) FROM test;
-- we wont see the modifying cte in this query because we will use local execution and
-- in postgres we wouldn't see this modifying cte, so it is consistent with postgres.
WITH a AS (SELECT count(*) FROM test), b AS (INSERT INTO local VALUES (3,2) RETURNING *), c AS (INSERT INTO ref VALUES (3,2) RETURNING *), d AS (SELECT count(*) FROM ref JOIN local ON (a = x)) SELECT * FROM a, b, c, d ORDER BY x,y,a,b;
TRUNCATE ref;
SELECT * FROM ref JOIN local ON (a = x);
-- we wont see the modifying cte in this query because we will use local execution and
-- in postgres we wouldn't see this modifying cte, so it is consistent with postgres.
WITH a AS (SELECT count(*) FROM test), b AS (INSERT INTO local VALUES (3,2) RETURNING *), c AS (INSERT INTO ref VALUES (3,2) RETURNING *), d AS (SELECT count(*) FROM ref JOIN local ON (a = x)) SELECT * FROM a, b, c, d ORDER BY x,y,a,b;
ROLLBACK;


BEGIN;
-- we wont see the modifying cte in this query because we will use local execution and
-- in postgres we wouldn't see this modifying cte, so it is consistent with postgres.
WITH a AS (SELECT count(*) FROM test), b AS (INSERT INTO local VALUES (3,2) RETURNING *), c AS (INSERT INTO ref VALUES (3,2) RETURNING *), d AS (SELECT count(*) FROM ref JOIN local ON (a = x)) SELECT * FROM a, b, c, d ORDER BY x,y,a,b;
ROLLBACK;

BEGIN;
-- we wont see the modifying cte in this query because we will use local execution and
-- in postgres we wouldn't see this modifying cte, so it is consistent with postgres.
WITH a AS (SELECT count(*) FROM test), b AS (INSERT INTO local VALUES (3,2) RETURNING *), c AS (INSERT INTO ref SELECT *,* FROM generate_series(1,10) RETURNING *), d AS (SELECT count(*) FROM ref JOIN local ON (a = x)) SELECT * FROM a, b, c, d ORDER BY x,y,a,b;
ROLLBACK;

-- same local table reference table tests, but outside a transaction block
INSERT INTO ref VALUES (1,2);
INSERT INTO local VALUES (1,2);
SELECT * FROM ref JOIN local ON (a = x);

-- we wont see the modifying cte in this query because we will use local execution and
-- in postgres we wouldn't see this modifying cte, so it is consistent with postgres.
WITH a AS (SELECT count(*) FROM test), b AS (INSERT INTO local VALUES (3,2) RETURNING *), c AS (INSERT INTO ref VALUES (3,2) RETURNING *), d AS (SELECT count(*) FROM ref JOIN local ON (a = x)) SELECT * FROM a, b, c, d ORDER BY x,y,a,b;

-- joins between local tables and distributed tables are disallowed
CREATE TABLE dist_table(a int);
SELECT create_distributed_table('dist_table', 'a');
INSERT INTO dist_table VALUES(1);

SELECT * FROM local JOIN dist_table ON (a = x) ORDER BY 1,2,3;
SELECT * FROM local JOIN dist_table ON (a = x) WHERE a = 1 ORDER BY 1,2,3;

-- intermediate results are allowed
WITH cte_1 AS (SELECT * FROM dist_table ORDER BY 1 LIMIT 1)
SELECT * FROM ref JOIN local ON (a = x) JOIN cte_1 ON (local.x = cte_1.a);

-- full router query with CTE and local
WITH cte_1 AS (SELECT * FROM ref LIMIT 1)
SELECT * FROM ref JOIN local ON (a = x) JOIN cte_1 ON (local.x = cte_1.a);

DROP TABLE dist_table;

-- issue #3801
SET citus.shard_replication_factor TO 2;
CREATE TABLE dist_table(a int);
SELECT create_distributed_table('dist_table', 'a');
BEGIN;
-- this will use perPlacementQueryStrings, make sure it works correctly with
-- copying task
INSERT INTO dist_table SELECT a + 1 FROM dist_table;
ROLLBACK;
SET citus.shard_replication_factor TO 1;

BEGIN;
SET citus.shard_replication_factor TO 2;
CREATE TABLE dist_table1(a int);
-- this will use queryStringList, make sure it works correctly with
-- copying task
SELECT create_distributed_table('dist_table1', 'a');
ROLLBACK;

CREATE table ref_table(x int PRIMARY KEY, y int);
-- this will be replicated to the coordinator because of add_coordinator test
SELECT create_reference_table('ref_table');

TRUNCATE TABLE test;

BEGIN;
INSERT INTO test SELECT *, * FROM generate_series(1, 100);
INSERT INTO ref_table SELECT *, * FROM generate_series(1, 100);
SELECT COUNT(*) FROM test JOIN ref_table USING(x);
ROLLBACK;

-- writing to local file and remote intermediate files
-- at the same time
INSERT INTO ref_table SELECT *, * FROM generate_series(1, 100);

WITH cte_1 AS (
INSERT INTO ref_table SELECT * FROM ref_table LIMIT 10000 ON CONFLICT (x) DO UPDATE SET y = EXCLUDED.y + 1 RETURNING *)
SELECT count(*) FROM cte_1;

-- issue #4237: preventing empty placement creation on coordinator
CREATE TABLE test_append_table(a int);
SELECT create_distributed_table('test_append_table', 'a', 'append');
-- this will fail since it will try to create an empty placement in the
-- coordinator as well
SET citus.shard_replication_factor TO 3;
SELECT master_create_empty_shard('test_append_table');
-- this will create an empty shard with replicas in the two worker nodes
SET citus.shard_replication_factor TO 2;
SELECT 1 FROM master_create_empty_shard('test_append_table');
-- verify groupid is not 0 for each placement
SELECT COUNT(*) FROM pg_dist_placement p, pg_dist_shard s WHERE p.shardid = s.shardid AND s.logicalrelid = 'test_append_table'::regclass AND p.groupid > 0;
SET citus.shard_replication_factor TO 1;

-- test partitioned index creation with long name
CREATE TABLE test_index_creation1
(
    tenant_id integer NOT NULL,
    timeperiod timestamp without time zone NOT NULL,
    field1 integer NOT NULL,
    inserted_utc timestamp without time zone NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, timeperiod)
) PARTITION BY RANGE (timeperiod);

CREATE TABLE test_index_creation1_p2020_09_26
PARTITION OF test_index_creation1 FOR VALUES FROM ('2020-09-26 00:00:00') TO ('2020-09-27 00:00:00');
CREATE TABLE test_index_creation1_p2020_09_27
PARTITION OF test_index_creation1 FOR VALUES FROM ('2020-09-27 00:00:00') TO ('2020-09-28 00:00:00');

select create_distributed_table('test_index_creation1', 'tenant_id');

-- should be able to create indexes with INCLUDE/WHERE
CREATE INDEX ix_test_index_creation5 ON test_index_creation1
	USING btree(tenant_id, timeperiod)
	INCLUDE (field1) WHERE (tenant_id = 100);

-- test if indexes are created
SELECT 1 AS created WHERE EXISTS(SELECT * FROM pg_indexes WHERE indexname LIKE '%test_index_creation%');


-- test alter_distributed_table UDF
SET citus.shard_count TO 4;
CREATE TABLE adt_table (a INT, b INT);
CREATE TABLE adt_col (a INT UNIQUE, b INT);
CREATE TABLE adt_ref (a INT REFERENCES adt_col(a));

SELECT create_distributed_table('adt_table', 'a', colocate_with:='none');
SELECT create_distributed_table('adt_col', 'a', colocate_with:='adt_table');
SELECT create_distributed_table('adt_ref', 'a', colocate_with:='adt_table');

INSERT INTO adt_table VALUES (1, 2), (3, 4), (5, 6);
INSERT INTO adt_col VALUES (3, 4), (5, 6), (7, 8);
INSERT INTO adt_ref VALUES (3), (5);

SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables WHERE table_name::text LIKE 'adt%';
SELECT STRING_AGG(table_name::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables WHERE table_name::text LIKE 'adt%' GROUP BY colocation_id ORDER BY 1;
SELECT conrelid::regclass::text AS "Referencing Table", pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
    WHERE (conrelid::regclass::text = 'adt_col' OR confrelid::regclass::text = 'adt_col') ORDER BY 1;

SET client_min_messages TO WARNING;
SELECT alter_distributed_table('adt_table', shard_count:=6, cascade_to_colocated:=true);
SET client_min_messages TO DEFAULT;

SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables WHERE table_name::text LIKE 'adt%';
SELECT STRING_AGG(table_name::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables WHERE table_name::text LIKE 'adt%' GROUP BY colocation_id ORDER BY 1;
SELECT conrelid::regclass::text AS "Referencing Table", pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
    WHERE (conrelid::regclass::text = 'adt_col' OR confrelid::regclass::text = 'adt_col') ORDER BY 1;

SELECT alter_distributed_table('adt_table', distribution_column:='b', colocate_with:='none');

SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables WHERE table_name::text LIKE 'adt%';
SELECT STRING_AGG(table_name::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables WHERE table_name::text LIKE 'adt%' GROUP BY colocation_id ORDER BY 1;
SELECT conrelid::regclass::text AS "Referencing Table", pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
    WHERE (conrelid::regclass::text = 'adt_col' OR confrelid::regclass::text = 'adt_col') ORDER BY 1;

SELECT * FROM adt_table ORDER BY 1;
SELECT * FROM adt_col ORDER BY 1;
SELECT * FROM adt_ref ORDER BY 1;

SET client_min_messages TO WARNING;
BEGIN;
INSERT INTO adt_table SELECT x, x+1 FROM generate_series(1, 1000) x;
SELECT alter_distributed_table('adt_table', distribution_column:='a');
SELECT COUNT(*) FROM adt_table;
END;

SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables WHERE table_name::text = 'adt_table';
SET client_min_messages TO DEFAULT;


-- issue 4508 table_1 and table_2 are used to test
-- some edge cases around intermediate result pruning
CREATE TABLE table_1 (key int, value text);
SELECT create_distributed_table('table_1', 'key');

CREATE TABLE table_2 (key int, value text);
SELECT create_distributed_table('table_2', 'key');

INSERT INTO table_1    VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4');
INSERT INTO table_2    VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4'), (5, '5'), (6, '6');

SET citus.log_intermediate_results TO ON;
SET client_min_messages to debug1;
WITH a AS (SELECT * FROM table_1 ORDER BY 1,2 DESC LIMIT 1)
SELECT count(*),
key
FROM a JOIN table_2 USING (key)
GROUP BY key
HAVING (max(table_2.value) >= (SELECT value FROM a));

WITH a AS (SELECT * FROM table_1 ORDER BY 1,2 DESC LIMIT 1)
INSERT INTO table_1 SELECT count(*),
key
FROM a JOIN table_2 USING (key)
GROUP BY key
HAVING (max(table_2.value) >= (SELECT value FROM a));

WITH stats AS (
  SELECT count(key) m FROM table_1
),
inserts AS (
  INSERT INTO table_2
  SELECT key, count(*)
  FROM table_1
  WHERE key >= (SELECT m FROM stats)
  GROUP BY key
  HAVING count(*) <= (SELECT m FROM stats)
  LIMIT 1
  RETURNING *
) SELECT count(*) FROM inserts;

-- a helper function which return true if the coordinated
-- trannsaction uses 2PC
CREATE OR REPLACE FUNCTION coordinated_transaction_should_use_2PC()
RETURNS BOOL LANGUAGE C STRICT VOLATILE AS 'citus',
$$coordinated_transaction_should_use_2PC$$;

-- a local SELECT followed by remote SELECTs
-- does not trigger 2PC
BEGIN;
  SELECT y FROM test WHERE x = 1;
  WITH cte_1 AS (SELECT y FROM test WHERE x = 1 LIMIT 5) SELECT count(*) FROM test;
  SELECT count(*) FROM test;
  WITH cte_1 as (SELECT * FROM test LIMIT 5) SELECT count(*) FROM test;
  SELECT coordinated_transaction_should_use_2PC();
COMMIT;

-- remote SELECTs followed by local SELECTs
-- does not trigger 2PC
BEGIN;
  SELECT count(*) FROM test;
  WITH cte_1 as (SELECT * FROM test LIMIT 5) SELECT count(*) FROM test;
  SELECT y FROM test WHERE x = 1;
  WITH cte_1 AS (SELECT y FROM test WHERE x = 1 LIMIT 5) SELECT count(*) FROM test;
  SELECT coordinated_transaction_should_use_2PC();
COMMIT;

-- a local SELECT followed by a remote Modify
-- triggers 2PC
BEGIN;
  SELECT y FROM test WHERE x = 1;
  UPDATE test SET y = y +1;
  SELECT coordinated_transaction_should_use_2PC();
COMMIT;

-- a local modify followed by a remote SELECT
-- triggers 2PC
BEGIN;
  INSERT INTO test VALUES (1,1);
  SELECT count(*) FROM test;
  SELECT coordinated_transaction_should_use_2PC();
COMMIT;

-- a local modify followed by a remote MODIFY
-- triggers 2PC
BEGIN;
  INSERT INTO test VALUES (1,1);
  UPDATE test SET y = y +1;
  SELECT coordinated_transaction_should_use_2PC();
COMMIT;

-- a local modify followed by a remote single shard MODIFY
-- triggers 2PC
BEGIN;
  INSERT INTO test VALUES (1,1);
  INSERT INTO test VALUES (3,3);
  SELECT coordinated_transaction_should_use_2PC();
COMMIT;

-- a remote single shard modify followed by a local single
-- shard MODIFY triggers 2PC
BEGIN;
  INSERT INTO test VALUES (3,3);
  INSERT INTO test VALUES (1,1);
  SELECT coordinated_transaction_should_use_2PC();
COMMIT;

-- a remote single shard select followed by a local single
-- shard MODIFY triggers 2PC. But, the transaction manager
-- is smart enough to skip sending 2PC as the remote
-- command is read only
BEGIN;
  SELECT count(*) FROM test WHERE x = 3;
  INSERT INTO test VALUES (1,1);
  SELECT coordinated_transaction_should_use_2PC();
  SET LOCAL citus.log_remote_commands TO ON;
COMMIT;

-- a local single shard select followed by a remote single
-- shard modify does not trigger 2PC
BEGIN;
  SELECT count(*) FROM test WHERE x = 1;
  INSERT INTO test VALUES (3,3);
  SELECT coordinated_transaction_should_use_2PC();
  SET LOCAL citus.log_remote_commands TO ON;
COMMIT;

RESET client_min_messages;
\set VERBOSITY terse
DROP TABLE ref_table;

DELETE FROM test;
DROP TABLE test;
DROP TABLE dist_table;
DROP TABLE ref;
DROP TABLE test_append_table;

DROP SCHEMA coordinator_shouldhaveshards CASCADE;

SELECT 1 FROM master_set_node_property('localhost', :master_port, 'shouldhaveshards', false);
