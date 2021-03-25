
CREATE SCHEMA cte_failure;
SET SEARCH_PATH=cte_failure;
SET citus.shard_count to 2;
SET citus.shard_replication_factor to 1;
SET citus.next_shard_id TO 16000000;

-- CTE inlining should not happen because
-- the tests rely on intermediate results
-- That's why we use MATERIALIZED CTEs in the test file
-- prevent using locally executing the intermediate results
SET citus.task_assignment_policy TO "round-robin";

SELECT pg_backend_pid() as pid \gset

CREATE TABLE users_table (user_id int, user_name text);
CREATE TABLE events_table(user_id int, event_id int, event_type int);
SELECT create_distributed_table('users_table', 'user_id');
SELECT create_distributed_table('events_table', 'user_id');
CREATE TABLE users_table_local AS SELECT * FROM users_table;

-- kill at the first copy (push)
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").kill()');

WITH cte AS MATERIALIZED (
	WITH local_cte AS MATERIALIZED (
		SELECT * FROM users_table_local
	),
	dist_cte AS MATERIALIZED (
		SELECT user_id FROM events_table
	)
	SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
)
SELECT
	count(*)
FROM
	cte,
	  (SELECT
    	DISTINCT users_table.user_id
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo
	  WHERE foo.user_id = cte.user_id;

-- kill at the second copy (pull)
SELECT citus.mitmproxy('conn.onQuery(query="SELECT user_id FROM cte_failure.events_table_16000002").kill()');

WITH cte AS MATERIALIZED (
	WITH local_cte AS MATERIALIZED (
		SELECT * FROM users_table_local
	),
	dist_cte AS MATERIALIZED (
		SELECT user_id FROM events_table
	)
	SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
)
SELECT
	count(*)
FROM
	cte,
	  (SELECT
    	DISTINCT users_table.user_id
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo
	  WHERE foo.user_id = cte.user_id;

-- kill at the third copy (pull)
SELECT citus.mitmproxy('conn.onQuery(query="SELECT DISTINCT users_table.user").kill()');

WITH cte AS MATERIALIZED (
	WITH local_cte AS MATERIALIZED (
		SELECT * FROM users_table_local
	),
	dist_cte AS MATERIALIZED (
		SELECT user_id FROM events_table
	)
	SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
)
SELECT
	count(*)
FROM
	cte,
	  (SELECT
    	DISTINCT users_table.user_id
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo
	  WHERE foo.user_id = cte.user_id;

-- cancel at the first copy (push)
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || :pid || ')');

WITH cte AS MATERIALIZED (
	WITH local_cte AS MATERIALIZED (
		SELECT * FROM users_table_local
	),
	dist_cte AS MATERIALIZED (
		SELECT user_id FROM events_table
	)
	SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
)
SELECT
	count(*)
FROM
	cte,
	  (SELECT
    	DISTINCT users_table.user_id
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo
	  WHERE foo.user_id = cte.user_id;

-- cancel at the second copy (pull)
SELECT citus.mitmproxy('conn.onQuery(query="SELECT user_id FROM").cancel(' || :pid || ')');

WITH cte AS MATERIALIZED (
	WITH local_cte AS MATERIALIZED (
		SELECT * FROM users_table_local
	),
	dist_cte AS MATERIALIZED (
		SELECT user_id FROM events_table
	)
	SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
)
SELECT
	count(*)
FROM
	cte,
	  (SELECT
    	DISTINCT users_table.user_id
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo
	  WHERE foo.user_id = cte.user_id;

-- cancel at the third copy (pull)
SELECT citus.mitmproxy('conn.onQuery(query="SELECT DISTINCT users_table.user").cancel(' || :pid || ')');

WITH cte AS MATERIALIZED (
	WITH local_cte AS MATERIALIZED (
		SELECT * FROM users_table_local
	),
	dist_cte AS MATERIALIZED (
		SELECT user_id FROM events_table
	)
	SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
)
SELECT
	count(*)
FROM
	cte,
	  (SELECT
    	DISTINCT users_table.user_id
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo
	  WHERE foo.user_id = cte.user_id;

-- distributed update tests
SELECT citus.mitmproxy('conn.allow()');

-- insert some rows
INSERT INTO users_table VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E');
INSERT INTO events_table VALUES (1,1,1), (1,2,1), (1,3,1), (2,1, 4), (3, 4,1), (5, 1, 2), (5, 2, 1), (5, 2,2);

SELECT * FROM users_table ORDER BY 1, 2;
-- following will delete and insert the same rows
WITH cte_delete AS MATERIALIZED (DELETE FROM users_table WHERE user_name in ('A', 'D') RETURNING *)
INSERT INTO users_table SELECT * FROM cte_delete;
-- verify contents are the same
SELECT * FROM users_table ORDER BY 1, 2;

-- kill connection during deletion
SELECT citus.mitmproxy('conn.onQuery(query="^DELETE FROM").kill()');
WITH cte_delete AS MATERIALIZED (DELETE FROM users_table WHERE user_name in ('A', 'D') RETURNING *)
INSERT INTO users_table SELECT * FROM cte_delete;

-- verify contents are the same
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM users_table ORDER BY 1, 2;

-- kill connection during insert
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").kill()');
WITH cte_delete AS MATERIALIZED (DELETE FROM users_table WHERE user_name in ('A', 'D') RETURNING *)
INSERT INTO users_table SELECT * FROM cte_delete;

-- verify contents are the same
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM users_table ORDER BY 1, 2;

-- cancel during deletion
SELECT citus.mitmproxy('conn.onQuery(query="^DELETE FROM").cancel(' || :pid || ')');
WITH cte_delete AS MATERIALIZED (DELETE FROM users_table WHERE user_name in ('A', 'D') RETURNING *)
INSERT INTO users_table SELECT * FROM cte_delete;

-- verify contents are the same
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM users_table ORDER BY 1, 2;

-- cancel during insert
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || :pid || ')');
WITH cte_delete AS MATERIALIZED (DELETE FROM users_table WHERE user_name in ('A', 'D') RETURNING *)
INSERT INTO users_table SELECT * FROM cte_delete;

-- verify contents are the same
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM users_table ORDER BY 1, 2;

-- test sequential delete/insert
SELECT citus.mitmproxy('conn.onQuery(query="^DELETE FROM").kill()');
BEGIN;
SET LOCAL citus.multi_shard_modify_mode = 'sequential';
WITH cte_delete AS MATERIALIZED (DELETE FROM users_table WHERE user_name in ('A', 'D') RETURNING *)
INSERT INTO users_table SELECT * FROM cte_delete;
END;

RESET SEARCH_PATH;
SELECT citus.mitmproxy('conn.allow()');
DROP SCHEMA cte_failure CASCADE;
