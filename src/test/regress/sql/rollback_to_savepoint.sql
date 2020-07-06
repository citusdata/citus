-- Regression test for this issue:
-- https://github.com/citusdata/citus/issues/3622
SET citus.shard_count = 4;
SET citus.next_shard_id TO 1954000;
CREATE SCHEMA rollback_to_savepoint;
SET search_path TO rollback_to_savepoint;

CREATE TABLE t(a int);
SELECT create_distributed_table('t', 'a');

-- This timeout is chosen such that the INSERT with
-- generate_series(1, 100000000) is cancelled at the right time to trigger the
-- bug
SET statement_timeout = '2s';
BEGIN;
INSERT INTO t VALUES (4);
SAVEPOINT s1;
INSERT INTO t SELECT i FROM generate_series(1, 10000000) i;
ROLLBACK TO SAVEPOINT s1;
INSERT INTO t SELECT i FROM generate_series(1, 100) i;
ROLLBACK;

DROP SCHEMA rollback_to_savepoint CASCADE;
