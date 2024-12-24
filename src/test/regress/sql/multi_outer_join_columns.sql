--- Test for verifying that column references (var nodes) in targets that cannot be pushed down
--- do not cause issues for the postgres planner, in particular postgres versions 16+, where the
--- varnullingrels field of a VAR node may contain relids of join relations that can make the var
--- NULL; in a rewritten distributed query without a join such relids do not have a meaning.

-- This test has an alternative goldfile because of the following feature in Postgres 16:
-- https://github.com/postgres/postgres/commit/1349d2790bf48a4de072931c722f39337e72055e
--

SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 16 AS server_version_ge_16;

CREATE SCHEMA outer_join_columns_testing;
SET search_path to 'outer_join_columns_testing';
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

-- Issue #7787: count distinct of a column from the inner side of a
-- left outer join will have a non-empty varnullingrels in the query
-- tree returned by Postgres 16+, so ensure this is not reflected in
-- the worker subquery constructed by Citus; it has just one relation,
-- for the pushed down subquery.
SELECT COUNT(DISTINCT a2)
FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.account_id;
EXPLAIN (VERBOSE, COSTS OFF, TIMING OFF)
SELECT COUNT(DISTINCT a2)
FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.account_id;

-- Issue #7787 also occurs with a HAVING clause
SELECT 1
FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.account_id
HAVING COUNT(DISTINCT a2) > 1;
EXPLAIN (VERBOSE, COSTS OFF, TIMING OFF)
SELECT 1
FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.account_id
HAVING COUNT(DISTINCT a2) > 1;

-- Check right outer join
SELECT COUNT(DISTINCT a2)
FROM t2 RIGHT OUTER JOIN t1 ON t2.account_id = t1.id;
EXPLAIN (VERBOSE, COSTS OFF, TIMING OFF)
SELECT COUNT(DISTINCT a2)
FROM t2 RIGHT OUTER JOIN t1 ON t2.account_id = t1.id;

-- Check both count distinct and having clause
SELECT COUNT(DISTINCT a2)
FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.account_id
HAVING COUNT(DISTINCT t2.id) > 1;
EXPLAIN (VERBOSE, COSTS OFF, TIMING OFF)
SELECT COUNT(DISTINCT a2)
FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.account_id
HAVING COUNT(DISTINCT t2.id) > 1;

--- cleanup
\set VERBOSITY TERSE
DROP SCHEMA outer_join_columns_testing CASCADE;
RESET all;
