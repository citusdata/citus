--- Test for verifying that column references (var nodes) in targets that cannot be pushed down
--- do not cause issues for the postgres planner, in particular postgres versions 16+, where the
--- varnullingrels field of a VAR node may contain relids of join relations that can make the var
--- NULL; in a rewritten distributed query without a join such relids do not have a meaning.
--- Issue #7705: [SEGFAULT] Querying distributed tables with window partition causes segmentation fault
--- https://github.com/citusdata/citus/issues/7705

CREATE SCHEMA issue_7705;
SET search_path to 'issue_7705';
SET citus.next_shard_id TO 30070000;
SET citus.shard_replication_factor TO 1;
SET citus.enable_local_execution TO ON;

CREATE TABLE t1 (id INT PRIMARY KEY);
INSERT INTO t1 VALUES (1), (2);

CREATE TABLE t2 (id INT, account_id INT, a2 INT, PRIMARY KEY(id, account_id));
INSERT INTO t2 VALUES (3, 1, 10), (4, 2, 20), (5, 1, NULL);

SELECT create_distributed_table('t1', 'id');
SELECT create_distributed_table('t2', 'account_id');

-- Test the issue seen in #7705; a target expression with
-- a window function that cannot be pushed down because the
-- partion by is not on the distribution column also includes
-- a column from the inner side of a left outer join, which
-- produces a non-empty varnullingrels set in PG 16 (and higher)
SELECT  t1.id, MAX(t2.a2) OVER (PARTITION BY t2.id)
FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.account_id;
EXPLAIN (VERBOSE, COSTS OFF, TIMING OFF)
SELECT  t1.id, MAX(t2.a2) OVER (PARTITION BY t2.id)
FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.account_id;

SELECT  t1.id, MAX(t2.a2) OVER (PARTITION BY t2.id)
FROM t2 RIGHT OUTER JOIN t1 ON t1.id = t2.account_id;
EXPLAIN (VERBOSE, COSTS OFF, TIMING OFF)
SELECT  t1.id, MAX(t2.a2) OVER (PARTITION BY t2.id)
FROM t2 RIGHT OUTER JOIN t1 ON t1.id = t2.account_id;

SELECT  DISTINCT t1.id, MAX(t2.a2) OVER (PARTITION BY t2.id)
FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.account_id;
EXPLAIN (VERBOSE, COSTS OFF, TIMING OFF)
SELECT DISTINCT t1.id, MAX(t2.a2) OVER (PARTITION BY t2.id)
FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.account_id;

CREATE SEQUENCE test_seq START 101;
CREATE OR REPLACE FUNCTION TEST_F(int) returns INT language sql stable as $$ select $1 + 42; $$ ;

-- Issue #7705 also occurs if a target expression includes a column
-- of a distributed table that is on the inner side of a left outer
-- join and a call to nextval(), because nextval() cannot be pushed
-- down, and must be run on the coordinator
SELECT t1.id, TEST_F(t2.a2 + nextval('test_seq') :: int)
FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.account_id
ORDER BY t1.id;
EXPLAIN (VERBOSE, COSTS OFF, TIMING OFF)
SELECT t1.id, TEST_F(t2.a2 + nextval('test_seq') :: int)
FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.account_id
ORDER BY t1.id;

SELECT t1.id, CASE nextval('test_seq') % 2 = 0 WHEN true THEN t2.a2 ELSE 1 END
FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.account_id
ORDER BY t1.id;
EXPLAIN (VERBOSE, COSTS OFF, TIMING OFF)
SELECT t1.id, CASE nextval('test_seq') %2 = 0 WHEN true THEN t2.a2 ELSE 1 END
FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.account_id
ORDER BY t1.id;

--- cleanup
\set VERBOSITY TERSE
DROP SCHEMA issue_7705 CASCADE;
RESET all;
