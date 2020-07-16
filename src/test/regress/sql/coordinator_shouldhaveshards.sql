-- Test queries on a distributed table with shards on the coordinator

CREATE SCHEMA coordinator_shouldhaveshards;
SET search_path TO coordinator_shouldhaveshards;
SET citus.next_shard_id TO 1503000;

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

set citus.enable_cte_inlining to off;

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
WITH cte_1 AS (SELECT * FROM dist_table LIMIT 1)
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

RESET citus.enable_cte_inlining;
CREATE table ref_table(x int, y int);
-- this will be replicated to the coordinator because of add_coordinator test
SELECT create_reference_table('ref_table');

TRUNCATE TABLE test;

BEGIN;
INSERT INTO test SELECT *, * FROM generate_series(1, 100);
INSERT INTO ref_table SELECT *, * FROM generate_series(1, 100);
SELECT COUNT(*) FROM test JOIN ref_table USING(x);
ROLLBACK;

\set VERBOSITY terse
DROP TABLE ref_table;

DELETE FROM test;
DROP TABLE test;
DROP TABLE dist_table;
DROP TABLE ref;

DROP SCHEMA coordinator_shouldhaveshards CASCADE;

SELECT 1 FROM master_set_node_property('localhost', :master_port, 'shouldhaveshards', false);
