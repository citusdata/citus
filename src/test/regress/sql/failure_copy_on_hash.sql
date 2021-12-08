--
-- Failure tests for COPY to hash distributed tables
--

CREATE SCHEMA copy_distributed_table;
SET search_path TO 'copy_distributed_table';
SET citus.next_shard_id TO 1710000;

SELECT citus.mitmproxy('conn.allow()');

-- With one placement COPY should error out and placement should stay healthy.
SET citus.shard_replication_factor TO 1;
SET citus.shard_count to 4;
SET citus.max_cached_conns_per_worker to 0;

CREATE TABLE test_table(id int, value_1 int);
SELECT create_distributed_table('test_table','id');

CREATE VIEW unhealthy_shard_count AS
	SELECT count(*)
	FROM pg_dist_shard_placement pdsp
	JOIN
	pg_dist_shard pds
	ON pdsp.shardid=pds.shardid
	WHERE logicalrelid='copy_distributed_table.test_table'::regclass AND shardstate != 1;

-- Just kill the connection after sending the first query to the worker.
SELECT citus.mitmproxy('conn.kill()');
\COPY test_table FROM stdin delimiter ',';
1,2
3,4
6,7
8,9
\.

SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- Now, kill the connection while copying the data
SELECT citus.mitmproxy('conn.onCopyData().kill()');
\COPY test_table FROM stdin delimiter ',';
1,2
3,4
6,7
8,9
\.

SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- Similar to the above one, but now cancel the connection
-- instead of killing it.
SELECT citus.mitmproxy('conn.onCopyData().cancel(' ||  pg_backend_pid() || ')');
\COPY test_table FROM stdin delimiter ',';
1,2
3,4
\.

SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- kill the connection after worker sends command complete message
SELECT citus.mitmproxy('conn.onCommandComplete(command="COPY 1").kill()');
\COPY test_table FROM stdin delimiter ',';
1,2
3,4
6,7
8,9
\.

SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- similar to above one, but cancel the connection on command complete
SELECT citus.mitmproxy('conn.onCommandComplete(command="COPY 1").cancel(' || pg_backend_pid() || ')');
\COPY test_table FROM stdin delimiter ',';
1,2
3,4
6,7
8,9
\.

SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- kill the connection on PREPARE TRANSACTION
SELECT citus.mitmproxy('conn.onQuery(query="PREPARE TRANSACTION").kill()');
\COPY test_table FROM stdin delimiter ',';
1,2
3,4
\.

SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- we don't want to see the prepared transaction numbers in the warnings
SET client_min_messages TO ERROR;

-- kill on command complete on COMMIT PREPARE, command should succeed
SELECT citus.mitmproxy('conn.onCommandComplete(command="COMMIT PREPARED").kill()');
\COPY test_table FROM stdin delimiter ',';
1,2
3,4
6,7
8,9
\.

SET client_min_messages TO NOTICE;

SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

TRUNCATE TABLE test_table;

-- kill on ROLLBACK, command could be rollbacked
SELECT citus.mitmproxy('conn.onQuery(query="ROLLBACK").kill()');
BEGIN;
\COPY test_table FROM stdin delimiter ',';
1,2
3,4
\.
ROLLBACK;

SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;
DROP TABLE test_table CASCADE;

-- With two placement, should we error out or mark untouched shard placements as inactive?
SET citus.shard_replication_factor TO 2;

CREATE TABLE test_table_2(id int, value_1 int);
SELECT create_distributed_table('test_table_2','id');

SELECT citus.mitmproxy('conn.kill()');

\COPY test_table_2 FROM stdin delimiter ',';

SELECT citus.mitmproxy('conn.allow()');
SELECT pds.logicalrelid, pdsd.shardid, pdsd.shardstate
	FROM pg_dist_shard_placement as pdsd
	INNER JOIN pg_dist_shard as pds
	ON pdsd.shardid = pds.shardid
	WHERE pds.logicalrelid = 'test_table_2'::regclass
	ORDER BY shardid, nodeport;

-- Create test_table_2 again to have healthy one
DROP TABLE test_table_2;
CREATE TABLE test_table_2(id int, value_1 int);
SELECT create_distributed_table('test_table_2','id');

-- Kill the connection when we try to start the COPY
-- The query should abort
SELECT citus.mitmproxy('conn.onQuery(query="FROM STDIN WITH").killall()');

\COPY test_table_2 FROM stdin delimiter ',';
1,2
3,4
6,7
8,9
\.

SELECT citus.mitmproxy('conn.allow()');
SELECT pds.logicalrelid, pdsd.shardid, pdsd.shardstate
	FROM pg_dist_shard_placement as pdsd
	INNER JOIN pg_dist_shard as pds
	ON pdsd.shardid = pds.shardid
	WHERE pds.logicalrelid = 'test_table_2'::regclass
	ORDER BY shardid, nodeport;

-- Create test_table_2 again to have healthy one
DROP TABLE test_table_2;
CREATE TABLE test_table_2(id int, value_1 int);
SELECT create_distributed_table('test_table_2','id');

-- When kill on copying data, it will be rollbacked and placements won't be labaled as invalid.
-- Note that now we sent data to shard 210007, yet it is not marked as invalid.
-- You can check the issue about this behaviour: https://github.com/citusdata/citus/issues/1933
SELECT citus.mitmproxy('conn.onCopyData().kill()');
\COPY test_table_2 FROM stdin delimiter ',';
1,2
3,4
6,7
8,9
9,10
11,12
13,14
\.

SELECT citus.mitmproxy('conn.allow()');
SELECT pds.logicalrelid, pdsd.shardid, pdsd.shardstate
	FROM pg_dist_shard_placement as pdsd
	INNER JOIN pg_dist_shard as pds
	ON pdsd.shardid = pds.shardid
	WHERE pds.logicalrelid = 'test_table_2'::regclass
	ORDER BY shardid, nodeport;

DROP SCHEMA copy_distributed_table CASCADE;
SET search_path TO default;
