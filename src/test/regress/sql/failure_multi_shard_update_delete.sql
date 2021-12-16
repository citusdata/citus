--
-- failure_multi_shard_update_delete
--

CREATE SCHEMA IF NOT EXISTS multi_shard;
SET SEARCH_PATH = multi_shard;
SET citus.shard_count TO 4;
SET citus.next_shard_id TO 201000;
SET citus.shard_replication_factor TO 1;
SET citus.max_adaptive_executor_pool_size TO 1;

-- do not cache any connections
SET citus.max_cached_conns_per_worker TO 0;

SELECT citus.mitmproxy('conn.allow()');

CREATE TABLE t1(a int PRIMARY KEY, b int, c int);
CREATE TABLE r1(a int, b int PRIMARY KEY);
CREATE TABLE t2(a int REFERENCES t1(a) ON DELETE CASCADE, b int REFERENCES r1(b) ON DELETE CASCADE, c int);

SELECT create_distributed_table('t1', 'a');
SELECT create_reference_table('r1');
SELECT create_distributed_table('t2', 'a');

-- insert some data
INSERT INTO r1 VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO t1 VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
INSERT INTO t2 VALUES (1, 1, 1), (1, 2, 1), (2, 1, 2), (2, 2, 4), (3, 1, 3), (3, 2, 3), (3, 3, 3);

SELECT pg_backend_pid() as pid \gset
SELECT count(*) FROM t2;

-- DELETION TESTS
-- delete using a filter on non-partition column filter
-- test both kill and cancellation

SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM").kill()');
-- issue a multi shard delete
DELETE FROM t2 WHERE b = 2;

-- verify nothing is deleted
SELECT count(*) FROM t2;

-- kill just one connection
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM multi_shard.t2_201005").kill()');
DELETE FROM t2 WHERE b = 2;

-- verify nothing is deleted
SELECT count(*) FROM t2;

-- cancellation
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM").cancel(' || :pid || ')');
-- issue a multi shard delete
DELETE FROM t2 WHERE b = 2;

-- verify nothing is deleted
SELECT count(*) FROM t2;

-- cancel just one connection
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM multi_shard.t2_201005").cancel(' || :pid || ')');
DELETE FROM t2 WHERE b = 2;

-- verify nothing is deleted
SELECT count(*) FROM t2;

-- UPDATE TESTS
-- update non-partition column based on a filter on another non-partition column
-- DELETION TESTS
-- delete using a filter on non-partition column filter
-- test both kill and cancellation


SELECT count(*) FILTER (WHERE b = 2) AS b2, count(*) FILTER (WHERE c = 4) AS c4 FROM t2;

SELECT citus.mitmproxy('conn.onQuery(query="^UPDATE").kill()');
-- issue a multi shard update
UPDATE t2 SET c = 4 WHERE b = 2;

-- verify nothing is updated
SELECT count(*) FILTER (WHERE b = 2) AS b2, count(*) FILTER (WHERE c = 4) AS c4 FROM t2;

-- kill just one connection
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE multi_shard.t2_201005").kill()');
UPDATE t2 SET c = 4 WHERE b = 2;

-- verify nothing is updated
SELECT count(*) FILTER (WHERE b = 2) AS b2, count(*) FILTER (WHERE c = 4) AS c4 FROM t2;

-- cancellation
SELECT citus.mitmproxy('conn.onQuery(query="^UPDATE").cancel(' || :pid || ')');
-- issue a multi shard update
UPDATE t2 SET c = 4 WHERE b = 2;

-- verify nothing is updated
SELECT count(*) FILTER (WHERE b = 2) AS b2, count(*) FILTER (WHERE c = 4) AS c4 FROM t2;

-- cancel just one connection
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE multi_shard.t2_201005").cancel(' || :pid || ')');
UPDATE t2 SET c = 4 WHERE b = 2;

-- verify nothing is updated
SELECT count(*) FILTER (WHERE b = 2) AS b2, count(*) FILTER (WHERE c = 4) AS c4 FROM t2;

-- DELETION TESTS
-- delete using a filter on non-partition column filter
-- test both kill and cancellation

SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM").kill()');
-- issue a multi shard delete
DELETE FROM t2 WHERE b = 2;

-- verify nothing is deleted
SELECT count(*) FROM t2;

-- kill just one connection
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM multi_shard.t2_201005").kill()');
DELETE FROM t2 WHERE b = 2;

-- verify nothing is deleted
SELECT count(*) FROM t2;

-- cancellation
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM").cancel(' || :pid || ')');
-- issue a multi shard delete
DELETE FROM t2 WHERE b = 2;

-- verify nothing is deleted
SELECT count(*) FROM t2;

-- cancel just one connection
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM multi_shard.t2_201005").cancel(' || :pid || ')');
DELETE FROM t2 WHERE b = 2;

-- verify nothing is deleted
SELECT count(*) FROM t2;

-- UPDATE TESTS
-- update non-partition column based on a filter on another non-partition column
-- DELETION TESTS
-- delete using a filter on non-partition column filter
-- test both kill and cancellation


SELECT count(*) FILTER (WHERE b = 2) AS b2, count(*) FILTER (WHERE c = 4) AS c4 FROM t2;

SELECT citus.mitmproxy('conn.onQuery(query="^UPDATE").kill()');
-- issue a multi shard update
UPDATE t2 SET c = 4 WHERE b = 2;

-- verify nothing is updated
SELECT count(*) FILTER (WHERE b = 2) AS b2, count(*) FILTER (WHERE c = 4) AS c4 FROM t2;

-- kill just one connection
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE multi_shard.t2_201005").kill()');
UPDATE t2 SET c = 4 WHERE b = 2;

-- verify nothing is updated
SELECT count(*) FILTER (WHERE b = 2) AS b2, count(*) FILTER (WHERE c = 4) AS c4 FROM t2;

-- cancellation
SELECT citus.mitmproxy('conn.onQuery(query="^UPDATE").cancel(' || :pid || ')');
-- issue a multi shard update
UPDATE t2 SET c = 4 WHERE b = 2;

-- verify nothing is updated
SELECT count(*) FILTER (WHERE b = 2) AS b2, count(*) FILTER (WHERE c = 4) AS c4 FROM t2;

-- cancel just one connection
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE multi_shard.t2_201005").cancel(' || :pid || ')');
UPDATE t2 SET c = 4 WHERE b = 2;

-- verify nothing is updated
SELECT count(*) FILTER (WHERE b = 2) AS b2, count(*) FILTER (WHERE c = 4) AS c4 FROM t2;


--
-- fail when cascading deletes from foreign key
-- unfortunately cascading deletes from foreign keys
-- are done inside the worker only and do not
-- generate any network output
-- therefore we can't just fail cascade part
-- following tests are added for completeness purposes
-- it is safe to remove them without reducing any
-- test coverage
SELECT citus.mitmproxy('conn.allow()');
-- check counts before delete
SELECT count(*) FILTER (WHERE b = 2) AS b2 FROM t2;

SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM").kill()');
DELETE FROM r1 WHERE a = 2;

-- verify nothing is deleted
SELECT count(*) FILTER (WHERE b = 2) AS b2 FROM t2;

SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM").kill()');

DELETE FROM t2 WHERE b = 2;

-- verify nothing is deleted
SELECT count(*) FILTER (WHERE b = 2) AS b2 FROM t2;

-- test update with subquery pull
SELECT citus.mitmproxy('conn.allow()');
CREATE TABLE t3 AS SELECT * FROM t2;
SELECT create_distributed_table('t3', 'a');
SELECT * FROM t3 ORDER BY 1, 2, 3;

SELECT citus.mitmproxy('conn.onQuery(query="^COPY").kill()');

UPDATE t3 SET c = q.c FROM (
	SELECT b, max(c) as c FROM t2  GROUP BY b) q
WHERE t3.b = q.b
RETURNING *;

--- verify nothing is updated
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM t3 ORDER BY 1, 2, 3;

-- kill update part
SELECT citus.mitmproxy('conn.onQuery(query="^UPDATE multi_shard.t3_201009").kill()');
UPDATE t3 SET c = q.c FROM (
	SELECT b, max(c) as c FROM t2  GROUP BY b) q
WHERE t3.b = q.b
RETURNING *;

--- verify nothing is updated
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM t3 ORDER BY 1, 2, 3;

-- test with replication_factor = 2
-- table can not have foreign reference with this setting so
-- use a different set of table

SET citus.shard_replication_factor to 2;
SELECT citus.mitmproxy('conn.allow()');
DROP TABLE t3;
CREATE TABLE t3 AS SELECT * FROM t2;
SELECT create_distributed_table('t3', 'a');
SELECT count(*) FILTER (WHERE b = 1) b1, count(*) FILTER (WHERE b = 2) AS b2 FROM t3;
-- prevent update of one replica of one shard
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE multi_shard.t3_201013").kill()');

UPDATE t3 SET b = 2 WHERE b = 1;

-- verify nothing is updated
SELECT count(*) FILTER (WHERE b = 1) b1, count(*) FILTER (WHERE b = 2) AS b2 FROM t3;

-- fail only one update verify transaction is rolled back correctly
BEGIN;
SELECT count(*) FILTER (WHERE b = 1) b1, count(*) FILTER (WHERE b = 2) AS b2 FROM t2;
SELECT count(*) FILTER (WHERE b = 1) b1, count(*) FILTER (WHERE b = 2) AS b2 FROM t3;

UPDATE t2 SET b = 2 WHERE b = 1;
-- verify update is performed on t2
SELECT count(*) FILTER (WHERE b = 1) b1, count(*) FILTER (WHERE b = 2) AS b2 FROM t2;
-- following will fail
UPDATE t3 SET b = 2 WHERE b = 1;
END;

-- verify everything is rolled back
SELECT count(*) FILTER (WHERE b = 1) b1, count(*) FILTER (WHERE b = 2) AS b2 FROM t2;
SELECT count(*) FILTER (WHERE b = 1) b1, count(*) FILTER (WHERE b = 2) AS b2 FROM t3;

UPDATE t3 SET b = 1 WHERE b = 2 RETURNING *;

-- verify nothing is updated
SELECT count(*) FILTER (WHERE b = 1) b1, count(*) FILTER (WHERE b = 2) AS b2 FROM t3;


SELECT count(*) FILTER (WHERE b = 1) b1, count(*) FILTER (WHERE b = 2) AS b2 FROM t3;

UPDATE t3 SET b = 2 WHERE b = 1;

-- verify nothing is updated
SELECT count(*) FILTER (WHERE b = 1) b1, count(*) FILTER (WHERE b = 2) AS b2 FROM t3;

-- fail only one update verify transaction is rolled back correctly
BEGIN;
SELECT count(*) FILTER (WHERE b = 1) b1, count(*) FILTER (WHERE b = 2) AS b2 FROM t2;
SELECT count(*) FILTER (WHERE b = 1) b1, count(*) FILTER (WHERE b = 2) AS b2 FROM t3;

UPDATE t2 SET b = 2 WHERE b = 1;
-- verify update is performed on t2
SELECT count(*) FILTER (WHERE b = 1) b1, count(*) FILTER (WHERE b = 2) AS b2 FROM t2;
-- following will fail
UPDATE t3 SET b = 2 WHERE b = 1;
END;

-- verify everything is rolled back
SELECT count(*) FILTER (WHERE b = 1) b1, count(*) FILTER (WHERE b = 2) AS b2 FROM t2;
SELECT count(*) FILTER (WHERE b = 1) b1, count(*) FILTER (WHERE b = 2) AS b2 FROM t3;

SELECT citus.mitmproxy('conn.allow()');
RESET SEARCH_PATH;
DROP SCHEMA multi_shard CASCADE;
