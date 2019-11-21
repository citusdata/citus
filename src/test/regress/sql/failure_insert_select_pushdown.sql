--
-- failure_insert_select_pushdown
--
-- performs failure/cancellation test for insert/select pushed down to shards.
--
SELECT citus.mitmproxy('conn.allow()');

CREATE SCHEMA insert_select_pushdown;
SET SEARCH_PATH=insert_select_pushdown;
SET citus.shard_count to 2;
SET citus.shard_replication_factor to 1;
SELECT pg_backend_pid() as pid \gset

CREATE TABLE events_table(user_id int, event_id int, event_type int);
CREATE TABLE events_summary(user_id int, event_id int, event_count int);
SELECT create_distributed_table('events_table', 'user_id');
SELECT create_distributed_table('events_summary', 'user_id');

INSERT INTO events_table VALUES (1, 1, 3 ), (1, 2, 1), (1, 3, 2), (2, 4, 3), (3, 5, 1), (4, 7, 1), (4, 1, 9), (4, 3, 2);


SELECT count(*) FROM events_summary;

-- insert/select from one distributed table to another

-- kill worker query
SELECT citus.mitmproxy('conn.onQuery(query="^INSERT INTO insert_select_pushdown").kill()');
INSERT INTO events_summary SELECT user_id, event_id, count(*) FROM events_table GROUP BY 1,2;

--verify nothing is modified
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM events_summary;

-- cancel worker query
SELECT citus.mitmproxy('conn.onQuery(query="^INSERT INTO insert_select_pushdown").cancel(' || :pid || ')');
INSERT INTO events_summary SELECT user_id, event_id, count(*) FROM events_table GROUP BY 1,2;

--verify nothing is modified
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM events_summary;

-- test self insert/select
SELECT count(*) FROM events_table;

-- kill worker query
SELECT citus.mitmproxy('conn.onQuery(query="^INSERT INTO insert_select_pushdown").kill()');
INSERT INTO events_table SELECT * FROM events_table;

--verify nothing is modified
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM events_table;

-- cancel worker query
SELECT citus.mitmproxy('conn.onQuery(query="^INSERT INTO insert_select_pushdown").cancel(' || :pid || ')');
INSERT INTO events_table SELECT * FROM events_table;

--verify nothing is modified
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM events_table;


RESET SEARCH_PATH;
SELECT citus.mitmproxy('conn.allow()');
DROP SCHEMA insert_select_pushdown CASCADE;
