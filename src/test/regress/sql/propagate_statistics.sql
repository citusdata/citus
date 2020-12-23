CREATE SCHEMA "statistics'Test";

SET search_path TO "statistics'Test";
SET citus.next_shard_id TO 980000;
SET client_min_messages TO WARNING;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

-- test create statistics propagation
CREATE TABLE test_stats (
    a   int,
    b   int
);

SELECT create_distributed_table('test_stats', 'a');

CREATE STATISTICS s1 (dependencies) ON a, b FROM test_stats;

-- test for distributing an already existing statistics
CREATE TABLE "test'stats2" (
    a   int,
    b   int
);

CREATE STATISTICS s2 (dependencies) ON a, b FROM "test'stats2";

SELECT create_distributed_table('test''stats2', 'a');

-- test when stats is on a different schema
CREATE SCHEMA sc1;
CREATE TABLE tbl (a int, "B" text);
SELECT create_distributed_table ('tbl', 'a');

CREATE STATISTICS sc1.st1 ON a, "B" FROM tbl;

-- test distributing table with already created stats on a new schema
CREATE TABLE test_stats3 (
    a int,
    b int
);
CREATE SCHEMA sc2;
CREATE STATISTICS sc2."neW'Stat" ON a,b FROM test_stats3;
SELECT create_distributed_table ('test_stats3','a');

-- test dropping statistics
CREATE TABLE test_stats4 (
    a int,
    b int
);
SELECT create_reference_table ('test_stats4');

CREATE STATISTICS s3 ON a,b FROM test_stats3;
CREATE STATISTICS sc2.s4 ON a,b FROM test_stats3;
CREATE STATISTICS s5 ON a,b FROM test_stats4;
-- s6 doesn't exist
DROP STATISTICS IF EXISTS s3, sc2.s4, s6;
DROP STATISTICS s5,s6;
DROP STATISTICS IF EXISTS s5,s5,s6,s6;

-- test renaming statistics
CREATE STATISTICS s6 ON a,b FROM test_stats4;
DROP STATISTICS s7;
ALTER STATISTICS s6 RENAME TO s7;

-- test altering stats schema
CREATE SCHEMA test_alter_schema;
ALTER STATISTICS s7 SET SCHEMA test_alter_schema;
DROP STATISTICS test_alter_schema.s7;

\c - - - :worker_1_port
SELECT stxname
FROM pg_statistic_ext
WHERE stxnamespace IN (
	SELECT oid
	FROM pg_namespace
	WHERE nspname IN ('public', 'statistics''Test', 'sc1', 'sc2')
)
ORDER BY stxname ASC;

SELECT count(DISTINCT stxnamespace)
FROM pg_statistic_ext
WHERE stxnamespace IN (
	SELECT oid
	FROM pg_namespace
	WHERE nspname IN ('public', 'statistics''Test', 'sc1', 'sc2')
);

\c - - - :master_port
SET client_min_messages TO WARNING;
DROP SCHEMA "statistics'Test" CASCADE;
DROP SCHEMA test_alter_schema CASCADE;
DROP SCHEMA sc1 CASCADE;
DROP SCHEMA sc2 CASCADE;
