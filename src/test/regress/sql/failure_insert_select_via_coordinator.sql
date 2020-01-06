--
-- failure_insert_select_via_coordinator
--
-- performs failure/cancellation test for insert/select executed by coordinator.
-- test for insert using CTEs are done in failure_cte_subquery, not repeating them here
--

CREATE SCHEMA coordinator_insert_select;
SET SEARCH_PATH=coordinator_insert_select;
SET citus.shard_count to 2;
SET citus.shard_replication_factor to 1;
SELECT pg_backend_pid() as pid \gset

CREATE TABLE events_table(user_id int, event_id int, event_type int);
CREATE TABLE events_summary(event_id int, event_type int, event_count int);
CREATE TABLE events_reference(event_type int, event_count int);
CREATE TABLE events_reference_distributed(event_type int, event_count int);
SELECT create_distributed_table('events_table', 'user_id');
SELECT create_distributed_table('events_summary', 'event_id');
SELECT create_reference_table('events_reference');
SELECT create_distributed_table('events_reference_distributed', 'event_type');

INSERT INTO events_table VALUES (1, 1, 3 ), (1, 2, 1), (1, 3, 2), (2, 4, 3), (3, 5, 1), (4, 7, 1), (4, 1, 9), (4, 3, 2);

SELECT count(*) FROM events_summary;

-- insert/select from one distributed table to another

-- kill coordinator pull query
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").kill()');
INSERT INTO events_summary SELECT event_id, event_type, count(*) FROM events_table GROUP BY 1,2;

-- kill data push
SELECT citus.mitmproxy('conn.onQuery(query="^COPY coordinator_insert_select").kill()');
INSERT INTO events_summary SELECT event_id, event_type, count(*) FROM events_table GROUP BY 1,2;

-- cancel coordinator pull query
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || :pid || ')');
INSERT INTO events_summary SELECT event_id, event_type, count(*) FROM events_table GROUP BY 1,2;

-- cancel data push
SELECT citus.mitmproxy('conn.onQuery(query="^COPY coordinator_insert_select").cancel(' || :pid || ')');
INSERT INTO events_summary SELECT event_id, event_type, count(*) FROM events_table GROUP BY 1,2;

--verify nothing is modified
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM events_summary;

-- insert into reference table from a distributed table
-- kill coordinator pull query
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").kill()');
INSERT INTO events_reference SELECT event_type, count(*) FROM events_table GROUP BY 1;

-- kill data push
SELECT citus.mitmproxy('conn.onQuery(query="^COPY coordinator_insert_select").kill()');
INSERT INTO events_reference SELECT event_type, count(*) FROM events_table GROUP BY 1;

-- cancel coordinator pull query
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || :pid || ')');
INSERT INTO events_reference SELECT event_type, count(*) FROM events_table GROUP BY 1;

-- cancel data push
SELECT citus.mitmproxy('conn.onQuery(query="^COPY coordinator_insert_select").cancel(' || :pid || ')');
INSERT INTO events_reference SELECT event_type, count(*) FROM events_table GROUP BY 1;

--verify nothing is modified
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM events_reference;

-- insert/select from reference table to distributed

-- fill up reference table first
INSERT INTO events_reference SELECT event_type, count(*) FROM events_table GROUP BY 1;

-- kill coordinator pull query
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").kill()');
INSERT INTO events_reference_distributed SELECT * FROM events_reference;

-- kill data push
SELECT citus.mitmproxy('conn.onQuery(query="^COPY coordinator_insert_select").kill()');
INSERT INTO events_reference_distributed SELECT * FROM events_reference;

-- cancel coordinator pull query
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || :pid || ')');
INSERT INTO events_reference_distributed SELECT * FROM events_reference;

-- cancel data push
SELECT citus.mitmproxy('conn.onQuery(query="^COPY coordinator_insert_select").cancel(' || :pid || ')');
INSERT INTO events_reference_distributed SELECT * FROM events_reference;

--verify nothing is modified
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM events_reference_distributed;

RESET SEARCH_PATH;
SELECT citus.mitmproxy('conn.allow()');
DROP SCHEMA coordinator_insert_select CASCADE;
