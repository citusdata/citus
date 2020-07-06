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

SET client_min_messages TO WARNING;
DROP SCHEMA sqlancer_failures CASCADE;
