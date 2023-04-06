--
-- failure_multi_row_insert
--

CREATE SCHEMA IF NOT EXISTS failure_multi_row_insert;
SET SEARCH_PATH TO failure_multi_row_insert;

-- this test is dependent on the shard count, so do not change
-- whitout changing the test
SET citus.shard_count TO 4;
SET citus.next_shard_id TO 301000;
SET citus.shard_replication_factor TO 1;
SELECT pg_backend_pid() as pid \gset

SELECT citus.mitmproxy('conn.allow()');


CREATE TABLE distributed_table(key int, value int);
CREATE TABLE reference_table(value int);

SELECT create_distributed_table('distributed_table', 'key');
SELECT create_reference_table('reference_table');

-- we'll test failure cases of the following cases:
-- (a) multi-row INSERT that hits the same shard with the same value
-- (b) multi-row INSERT that hits the same shard with different values
-- (c) multi-row INSERT that hits multiple shards in a single worker
-- (d) multi-row INSERT that hits multiple shards in multiple workers
-- (e) multi-row INSERT to a reference table


--  Failure and cancellation on multi-row INSERT that hits the same shard with the same value
SELECT citus.mitmproxy('conn.onQuery(query="INSERT").kill()');
INSERT INTO distributed_table VALUES (1,1), (1,2), (1,3);

-- this test is broken, see https://github.com/citusdata/citus/issues/2460
-- SELECT citus.mitmproxy('conn.onQuery(query="^INSERT").cancel(' || :pid || ')');
-- INSERT INTO distributed_table VALUES (1,4), (1,5), (1,6);

--  Failure and cancellation on multi-row INSERT that hits the same shard with different values
SELECT citus.mitmproxy('conn.onQuery(query="^INSERT").kill()');
INSERT INTO distributed_table VALUES (1,7), (5,8);

-- this test is broken, see https://github.com/citusdata/citus/issues/2460
-- SELECT citus.mitmproxy('conn.onQuery(query="^INSERT").cancel(' || :pid || ')');
-- INSERT INTO distributed_table VALUES (1,9), (5,10);

--  Failure and cancellation multi-row INSERT that hits multiple shards in a single worker
SELECT citus.mitmproxy('conn.onQuery(query="^INSERT").kill()');
INSERT INTO distributed_table VALUES (1,11), (6,12);

SELECT citus.mitmproxy('conn.onQuery(query="^INSERT").cancel(' || :pid || ')');
INSERT INTO distributed_table VALUES (1,13), (6,14);

--  Failure and cancellation multi-row INSERT that hits multiple shards in a single worker, happening on the second query
SELECT citus.mitmproxy('conn.onQuery(query="^INSERT").after(1).kill()');
INSERT INTO distributed_table VALUES (1,15), (6,16);

SELECT citus.mitmproxy('conn.onQuery(query="^INSERT").after(1).cancel(' || :pid || ')');
INSERT INTO distributed_table VALUES (1,17), (6,18);

--  Failure and cancellation multi-row INSERT that hits multiple shards in multiple workers
SELECT citus.mitmproxy('conn.onQuery(query="^INSERT").kill()');
INSERT INTO distributed_table VALUES (2,19),(1,20);

SELECT citus.mitmproxy('conn.onQuery(query="^INSERT").cancel(' || :pid || ')');
INSERT INTO distributed_table VALUES (2,21), (1,22);

-- one test for the reference tables for completeness
SELECT citus.mitmproxy('conn.onQuery(query="^INSERT").cancel(' || :pid || ')');
INSERT INTO reference_table VALUES (1), (2), (3), (4);

SELECT citus.mitmproxy('conn.onQuery(query="^INSERT").cancel(' || :pid || ')');
INSERT INTO distributed_table VALUES (1,1), (2,2), (3,3), (4,2), (5,2), (6,2), (7,2);

-- cancel the second insert over the same connection
SELECT citus.mitmproxy('conn.onQuery(query="^INSERT").after(1).cancel(' || :pid || ')');
INSERT INTO distributed_table VALUES (1,1), (2,2), (3,3), (4,2), (5,2), (6,2), (7,2);

-- we've either failed or cancelled all queries, so should be empty
SELECT * FROM distributed_table;
SELECT * FROM reference_table;

SELECT citus.mitmproxy('conn.allow()');
RESET SEARCH_PATH;
DROP SCHEMA failure_multi_row_insert CASCADE;
