CREATE SCHEMA sqlancer_failures;
SET search_path TO sqlancer_failures;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 92862400;

CREATE TABLE t0 (c0 int, c1 MONEY);
SELECT create_distributed_table('t0', 'c0');
UPDATE t0 SET c1 = ((0.43107963)::MONEY) WHERE ((upper('-14295774') COLLATE "de_CH
.utf8") SIMILAR TO '');
UPDATE t0 SET c1 = 1 WHERE '' COLLATE "C" = '';

CREATE TABLE t1 (c0 text);
SELECT create_distributed_table('t1', 'c0');
INSERT INTO t1 VALUES ('' COLLATE "C");

CREATE TABLE t2 (c0 text, c1 bool, c2 timestamptz default now());
SELECT create_distributed_table('t2', 'c0');
INSERT INTO t2 VALUES ('key', '' COLLATE "C" = '');

CREATE TABLE t3 (c0 text, c1 text, c2 timestamptz default now());
SELECT create_distributed_table('t3', 'c0');
INSERT INTO t3 VALUES ('key', '' COLLATE "C");

CREATE TABLE t4(c0 real, c1 boolean);
SELECT create_distributed_table('t4', 'c1');
INSERT INTO t4 VALUES (1.0, 2 BETWEEN 1 AND 3);
-- NOTE: For some reason shard pruning doesn't happen correctly here. It does
-- work for non boolean const expressions. See explain plans for t5 below that
-- show that. The query still works though. So doesn't seem important enough to
-- fix, since boolean partition columns should not happen much/at all for
-- actual users.
EXPLAIN (COSTS FALSE) SELECT FROM t4 WHERE c1 = 2 BETWEEN 1 AND 3;
EXPLAIN (COSTS FALSE) SELECT FROM t4 WHERE c1 = true;

CREATE TABLE t5(c0 int);
SELECT create_distributed_table('t5', 'c0');
INSERT INTO t5 VALUES (CASE WHEN 2 BETWEEN 1 AND 3 THEN 2 ELSE 1 END);
EXPLAIN (COSTS FALSE) SELECT FROM t5 WHERE c0 = 2;
EXPLAIN (COSTS FALSE) SELECT FROM t5 WHERE c0 = CASE WHEN 2 BETWEEN 1 AND 3 THEN 2 ELSE 1 END;

CREATE TABLE IF NOT EXISTS t6(c0 TEXT  CHECK (TRUE), c1 money ) WITH (autovacuum_vacuum_threshold=1180014707, autovacuum_freeze_table_age=13771154, autovacuum_vacuum_cost_delay=23, autovacuum_analyze_threshold=1935153914, autovacuum_freeze_min_age=721733768, autovacuum_enabled=0, autovacuum_vacuum_cost_limit=9983);
CREATE UNLOGGED TABLE IF NOT EXISTS t7(LIKE t6);
CREATE TABLE t8(LIKE t6 INCLUDING INDEXES);
CREATE UNLOGGED TABLE t9(LIKE t6 EXCLUDING STATISTICS);
CREATE TABLE t10(LIKE t7);

SELECT create_distributed_table('t6', 'c0');
ALTER TABLE t6 ALTER COLUMN c0 SET NOT NULL;
SELECT create_reference_table('t7');
SELECT create_distributed_table('t8', 'c0');
ALTER TABLE t8 ALTER COLUMN c0 SET NOT NULL;
SELECT create_distributed_table('t9', 'c0');
ALTER TABLE t9 ALTER COLUMN c0 SET NOT NULL;
SELECT create_reference_table('t10');

SELECT count(*) FROM (
SELECT ALL t7.c1, t7.c0, t8.c1, t10.c1, t8.c0 FROM t7 CROSS JOIN t10 FULL OUTER JOIN t8 ON (((((((('[832125354,1134163512)'::int4range)*('(0,2106623281)'::int4range)))-('(-600267905,509840582]'::int4range)))*('(-365203965,1662828182)'::int4range)))&<((((((('(-1286467417,697584012]'::int4range)*('[-1691485781,1341103963)'::int4range)))*((('(-1768368435,1719707648)'::int4range)*('(139536997,1275813540]'::int4range)))))*((((('[-2103910157,-1961746758)'::int4range)*('[-834534078,533073939)'::int4range)))*((('[-1030552151,552856781]'::int4range)*('[-1109419376,1205173697]'::int4range))))))))
) AS foo;

SET client_min_messages TO WARNING;
DROP SCHEMA sqlancer_failures CASCADE;
