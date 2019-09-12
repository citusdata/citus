CREATE SCHEMA local_shard_execution;
SET search_path TO local_shard_execution;

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';
SET citus.next_shard_id TO 1470000;

CREATE TABLE reference_table (key int PRIMARY KEY);
SELECT create_reference_table('reference_table');

CREATE TABLE distributed_table (key int PRIMARY KEY , value text, age bigint CHECK (age > 10), FOREIGN KEY (key) REFERENCES reference_table(key) ON DELETE CASCADE);
SELECT create_distributed_table('distributed_table','key');

CREATE TABLE second_distributed_table (key int PRIMARY KEY , value text, FOREIGN KEY (key) REFERENCES distributed_table(key) ON DELETE CASCADE);
SELECT create_distributed_table('second_distributed_table','key');

-- ingest some data to enable some tests with data
INSERT INTO reference_table VALUES (1);
INSERT INTO distributed_table VALUES (1, '1', 20);
INSERT INTO second_distributed_table VALUES (1, '1');

-- a simple test for 
CREATE TABLE collections_list (
	key int,
	ts timestamptz,
	collection_id integer,
	value numeric,
	PRIMARY KEY(key, collection_id)
) PARTITION BY LIST (collection_id );

SELECT create_distributed_table('collections_list', 'key');

CREATE TABLE collections_list_0 
	PARTITION OF collections_list (key, ts, collection_id, value)
	FOR VALUES IN ( 0 );

-- connection worker and get ready for the tests
\c - - - :worker_1_port
SET search_path TO local_shard_execution;

-- returns true of the distribution key filter
-- on the distributed tables (e.g., WHERE key = 1), we'll hit a shard
-- placement which is local to this not
CREATE OR REPLACE FUNCTION shard_of_distribution_column_is_local(dist_key int) RETURNS bool AS $$
	
		DECLARE shard_is_local BOOLEAN := FALSE;

		BEGIN
		  
		  	WITH  local_shard_ids 			  AS (SELECT get_shard_id_for_distribution_column('local_shard_execution.distributed_table', dist_key)),
				  all_local_shard_ids_on_node AS (SELECT shardid FROM pg_dist_placement WHERE groupid IN (SELECT groupid FROM pg_dist_local_group))
		SELECT 
			true INTO shard_is_local
		FROM 
			local_shard_ids 
		WHERE 
			get_shard_id_for_distribution_column IN (SELECT * FROM all_local_shard_ids_on_node); 

		IF shard_is_local IS NULL THEN
			shard_is_local = FALSE;
		END IF;

		RETURN shard_is_local;
        END;
$$ LANGUAGE plpgsql;

-- pick some example values that reside on the shards locally and remote

-- distribution key values of 1,6, 500 and 701 are LOCAL to shards, 
-- we'll use these values in the tests
SELECT shard_of_distribution_column_is_local(1);
SELECT shard_of_distribution_column_is_local(6);
SELECT shard_of_distribution_column_is_local(500);
SELECT shard_of_distribution_column_is_local(701);

-- distribution key values of 11 and 12 are REMOTE to shards 
SELECT shard_of_distribution_column_is_local(11);
SELECT shard_of_distribution_column_is_local(12);

--- enable logging to see which tasks are executed locally
SET client_min_messages TO LOG;
SET citus.log_local_commands TO ON;

-- first, make sure that local execution works fine 
-- with simple queries that are not in transcation blocks
SELECT count(*) FROM distributed_table WHERE key = 1;

-- multiple tasks both of which are local should NOT use local execution
-- because local execution means executing the tasks locally, so the executor
-- favors parallel execution even if everyting is local to node
SELECT count(*) FROM distributed_table WHERE key IN (1,6);

-- queries that hit any remote shards should NOT use local execution
SELECT count(*) FROM distributed_table WHERE key IN (1,11);
SELECT count(*) FROM distributed_table;

-- modifications also follow the same rules
INSERT INTO reference_table VALUES (1) ON CONFLICT DO NOTHING;
INSERT INTO distributed_table VALUES (1, '1', 21) ON CONFLICT DO NOTHING;

-- local query
DELETE FROM distributed_table WHERE key = 1 AND age = 21;

-- hitting multiple shards, so should be a distributed execution
DELETE FROM distributed_table WHERE age = 21;

-- although both shards are local, the executor choose the parallel execution
-- over the wire because as noted above local execution is sequential
DELETE FROM second_distributed_table WHERE key IN (1,6);

-- similarly, any multi-shard query just follows distributed execution
DELETE FROM second_distributed_table;

-- load some more data for the following tests
INSERT INTO second_distributed_table VALUES (1, '1');

-- INSERT .. SELECT hitting a single single (co-located) shard(s) should 
-- be executed locally
INSERT INTO distributed_table 
SELECT 
	distributed_table.* 
FROM 
	distributed_table, second_distributed_table 
WHERE 
	distributed_table.key = 1 and distributed_table.key=second_distributed_table.key 
ON CONFLICT(key) DO UPDATE SET value = '22'
RETURNING *;

-- INSERT .. SELECT hitting multi-shards should go thourgh distributed execution
INSERT INTO distributed_table 
SELECT 
	distributed_table.* 
FROM 
	distributed_table, second_distributed_table 
WHERE 
	distributed_table.key != 1 and distributed_table.key=second_distributed_table.key 
ON CONFLICT(key) DO UPDATE SET value = '22'
RETURNING *;

-- INSERT..SELECT via coordinator consists of two steps, select + COPY
-- that's why it is disallowed to use local execution even if the SELECT
-- can be executed locally
INSERT INTO distributed_table SELECT * FROM distributed_table WHERE key = 1 OFFSET 0 ON CONFLICT DO NOTHING;
INSERT INTO distributed_table SELECT 1, '1',15 FROM distributed_table WHERE key = 2 LIMIT 1 ON CONFLICT DO NOTHING;

-- sanity check: multi-shard INSERT..SELECT pushdown goes through distributed execution
INSERT INTO distributed_table SELECT * FROM distributed_table ON CONFLICT DO NOTHING;


-- EXPLAIN for local execution just works fine
-- though going through distributed execution
EXPLAIN (COSTS OFF) SELECT * FROM distributed_table WHERE key = 1 AND age = 20;

-- TODO: Fix #2922
-- EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)   SELECT * FROM distributed_table WHERE key = 1 AND age = 20;

EXPLAIN (COSTS OFF) DELETE FROM distributed_table WHERE key = 1 AND age = 20;

-- TODO: Fix #2922
-- EXPLAIN ANALYZE DELETE FROM distributed_table WHERE key = 1 AND age = 20;

-- show that EXPLAIN ANALYZE deleted the row
SELECT * FROM distributed_table WHERE key = 1 AND age = 20 ORDER BY 1,2,3;

-- copy always happens via distributed execution irrespective of the
-- shards that are accessed
COPY reference_table FROM STDIN;
6
11
\.

COPY distributed_table FROM STDIN WITH CSV;
6,'6',25
11,'11',121
\.

COPY second_distributed_table FROM STDIN WITH CSV;
6,'6'
\.

-- the behaviour in transaction blocks is the following: 
	-- (a) Unless the first query is a local query, always use distributed execution.
	-- (b) If the executor has used local execution, it has to use local execution 
	--     for the remaining of the transaction block. If that's not possible, the 
	-- 	   executor has to error out (e.g., TRUNCATE is a utility command and we 
	--	   currently do not support local execution of utility commands)

-- rollback should be able to rollback local execution
BEGIN;
	INSERT INTO distributed_table VALUES (1, '11',21) ON CONFLICT(key) DO UPDATE SET value = '29' RETURNING *;
	SELECT * FROM distributed_table WHERE key = 1 ORDER BY 1,2,3;
ROLLBACK;

-- make sure that the value is rollbacked
SELECT * FROM distributed_table WHERE key = 1 ORDER BY 1,2,3;


-- rollback should be able to rollback both the local and distributed executions
BEGIN;
	INSERT INTO distributed_table VALUES (1, '11',21) ON CONFLICT(key) DO UPDATE SET value = '29' RETURNING *;
	DELETE FROM distributed_table;

	-- DELETE should cascade, and we should not see any rows
	SELECT count(*) FROM second_distributed_table;
ROLLBACK;

-- make sure that everything is rollbacked
SELECT * FROM distributed_table WHERE key = 1 ORDER BY 1,2,3;
SELECT count(*) FROM second_distributed_table;

-- very simple examples, an SELECTs should see the modifications
-- that has done before
BEGIN;
	-- INSERT is executed locally
	INSERT INTO distributed_table VALUES (1, '11',21) ON CONFLICT(key) DO UPDATE SET value = '23' RETURNING *; 

	-- since the INSERT is executed locally, the SELECT should also be 
	-- executed locally and see the changes
	SELECT * FROM distributed_table WHERE key = 1 ORDER BY 1,2,3;

	-- multi-shard SELECTs are now forced to use local execution on
	-- the shards that reside on this node
	SELECT * FROM distributed_table WHERE value = '23' ORDER BY 1,2,3;

	-- similarly, multi-shard modifications should use local exection
	-- on the shards that reside on this node
	DELETE FROM distributed_table WHERE value = '23';

	-- make sure that the value is deleted
	SELECT * FROM distributed_table WHERE value = '23' ORDER BY 1,2,3;
COMMIT;

-- make sure that we've committed everything
SELECT * FROM distributed_table WHERE key = 1 ORDER BY 1,2,3;

-- if we start with a distributed execution, we should keep
-- using that and never switch back to local execution 
BEGIN;
	DELETE FROM distributed_table WHERE value = '11';

	-- although this command could have been executed
	-- locally, it is not going to be executed locally
	SELECT * FROM distributed_table WHERE key = 1 ORDER BY 1,2,3;

	-- but we can still execute parallel queries, even if
	-- they are utility commands
	TRUNCATE distributed_table CASCADE;

	-- TRUNCATE cascaded into second_distributed_table
	SELECT count(*) FROM second_distributed_table;
ROLLBACK;

-- load some data so that foreign keys won't complain with the next tests
INSERT INTO reference_table SELECT i FROM generate_series(500, 600) i;

-- a very similar set of operation, but this time use
-- COPY as the first command
BEGIN;
	INSERT INTO distributed_table SELECT i, i::text, i % 10 + 25 FROM generate_series(500, 600) i;

	-- this could go through local execution, but doesn't because we've already
	-- done distributed execution
	SELECT * FROM distributed_table WHERE key = 500 ORDER BY 1,2,3;

	-- utility commands could still use distributed execution
	TRUNCATE distributed_table CASCADE;

	-- ensure that TRUNCATE made it
	SELECT * FROM distributed_table WHERE key = 500 ORDER BY 1,2,3;
ROLLBACK;

-- show that cascading foreign keys just works fine with local execution
BEGIN;
	INSERT INTO reference_table VALUES (701);
	INSERT INTO distributed_table VALUES (701, '701', 701);
	INSERT INTO second_distributed_table VALUES (701, '701');

	DELETE FROM reference_table WHERE key = 701;

	SELECT count(*) FROM distributed_table WHERE key = 701;
	SELECT count(*) FROM second_distributed_table WHERE key = 701;

	-- multi-shard commands should also see the changes
	SELECT count(*) FROM distributed_table WHERE key > 700;

	-- we can still do multi-shard commands
	DELETE FROM distributed_table;
ROLLBACK;

-- multiple queries hitting different shards can be executed locally
BEGIN;
	SELECT count(*) FROM distributed_table WHERE key = 1;
	SELECT count(*) FROM distributed_table WHERE key = 6;
	SELECT count(*) FROM distributed_table WHERE key = 500;
ROLLBACK;

-- a local query is followed by a command that cannot be executed locally
BEGIN;
	SELECT count(*) FROM distributed_table WHERE key = 1;
	TRUNCATE distributed_table CASCADE;
ROLLBACK;

-- a local query is followed by a command that cannot be executed locally
BEGIN;
	SELECT count(*) FROM distributed_table WHERE key = 1;

	-- even no need to supply any data
	COPY distributed_table FROM STDIN WITH CSV;
ROLLBACK;

-- a local query is followed by a command that cannot be executed locally
BEGIN;
	SELECT count(*) FROM distributed_table WHERE key = 1;
	
	INSERT INTO distributed_table (key) SELECT i FROM generate_series(1,10)i; 
ROLLBACK;

-- a local query is followed by a command that cannot be executed locally
BEGIN;
	SELECT count(*) FROM distributed_table WHERE key = 1;
	
	INSERT INTO distributed_table (key) SELECT key+1 FROM distributed_table; 
ROLLBACK;

INSERT INTO distributed_table VALUES (1, '11',21) ON CONFLICT(key) DO UPDATE SET value = '29' RETURNING *;

BEGIN;
	DELETE FROM distributed_table WHERE key = 1;
	EXPLAIN ANALYZE DELETE FROM distributed_table WHERE key = 1;
ROLLBACK;

BEGIN;
	INSERT INTO distributed_table VALUES (11, '111',29) ON CONFLICT(key) DO UPDATE SET value = '29' RETURNING *;

	-- this is already disallowed on the nodes, adding it in case we
	-- support DDLs from the worker nodes in the future
	ALTER TABLE distributed_table ADD COLUMN x INT;
ROLLBACK;

BEGIN;
	INSERT INTO distributed_table VALUES (11, '111',29) ON CONFLICT(key) DO UPDATE SET value = '29' RETURNING *;

	-- this is already disallowed because VACUUM cannot be executed in tx block
	-- adding in case this is supported some day
	VACUUM second_distributed_table;
ROLLBACK;

-- make sure that functions can use local execution
CREATE OR REPLACE PROCEDURE only_local_execution() AS $$
		DECLARE cnt INT;
		BEGIN
			INSERT INTO distributed_table VALUES (1, '11',21) ON CONFLICT(key) DO UPDATE SET value = '29';
			SELECT count(*) INTO cnt FROM distributed_table WHERE key = 1;
			DELETE FROM distributed_table WHERE key = 1;	
        END;
$$ LANGUAGE plpgsql;

CALL only_local_execution();

CREATE OR REPLACE PROCEDURE only_local_execution_with_params(int) AS $$
		DECLARE cnt INT;
		BEGIN
			INSERT INTO distributed_table VALUES ($1, '11',21) ON CONFLICT(key) DO UPDATE SET value = '29';
			SELECT count(*) INTO cnt FROM distributed_table WHERE key = $1;
			DELETE FROM distributed_table WHERE key = $1;
        END;
$$ LANGUAGE plpgsql;

CALL only_local_execution_with_params(1);

CREATE OR REPLACE PROCEDURE local_execution_followed_by_dist() AS $$
		DECLARE cnt INT;
		BEGIN
			INSERT INTO distributed_table VALUES (1, '11',21) ON CONFLICT(key) DO UPDATE SET value = '29';
			SELECT count(*) INTO cnt FROM distributed_table WHERE key = 1;
			DELETE FROM distributed_table;
			SELECT count(*) INTO cnt FROM distributed_table;
        END;
$$ LANGUAGE plpgsql;

CALL local_execution_followed_by_dist();

-- test CTEs, including modifying CTEs
WITH local_insert AS (INSERT INTO distributed_table VALUES (1, '11',21) ON CONFLICT(key) DO UPDATE SET value = '29' RETURNING *),
distributed_local_mixed AS (SELECT * FROM reference_table WHERE key IN (SELECT key FROM local_insert))
SELECT * FROM local_insert, distributed_local_mixed;

-- since we start with parallel execution, we do not switch back to local execution in the
-- latter CTEs
WITH distributed_local_mixed AS (SELECT * FROM distributed_table),
local_insert AS (INSERT INTO distributed_table VALUES (1, '11',21) ON CONFLICT(key) DO UPDATE SET value = '29' RETURNING *)
SELECT * FROM local_insert, distributed_local_mixed ORDER BY 1,2,3,4,5;

-- router CTE pushdown
WITH all_data AS (SELECT * FROM distributed_table WHERE key = 1)
SELECT 
	count(*) 
FROM 
	distributed_table, all_data 
WHERE 
	distributed_table.key = all_data.key AND distributed_table.key = 1;

INSERT INTO reference_table VALUES (2);
INSERT INTO distributed_table VALUES (2, '29', 29);
INSERT INTO second_distributed_table VALUES (2, '29');

-- single shard that is not a local query followed by a local query
WITH all_data AS (SELECT * FROM second_distributed_table WHERE key = 2)
SELECT 
	distributed_table.key
FROM 
	distributed_table, all_data 
WHERE 
	distributed_table.value = all_data.value AND distributed_table.key = 1
ORDER BY 
	1 DESC;

-- multi-shard CTE is followed by a query which could be executed locally, but
-- since the query started with a parallel query, it doesn't use local execution
WITH all_data AS (SELECT * FROM distributed_table)
SELECT 
	count(*) 
FROM 
	distributed_table, all_data 
WHERE 
	distributed_table.key = all_data.key AND distributed_table.key = 1;


-- get ready for the next commands
TRUNCATE reference_table, distributed_table, second_distributed_table;

-- local execution of returning of reference tables
INSERT INTO reference_table VALUES (1),(2),(3),(4),(5),(6) RETURNING *;

-- local execution of multi-row INSERTs
INSERT INTO distributed_table VALUES (1, '11',21), (5,'55',22) ON CONFLICT(key) DO UPDATE SET value = (EXCLUDED.value::int + 1)::text RETURNING *;


-- distributed execution of multi-rows INSERTs, where some part of the execution 
-- could have been done via local execution but the executor choose the other way around
-- because the command is a multi-shard query
INSERT INTO distributed_table VALUES (1, '11',21), (2,'22',22), (3,'33',33), (4,'44',44),(5,'55',55) ON CONFLICT(key) DO UPDATE SET value = (EXCLUDED.value::int + 1)::text RETURNING *;


PREPARE local_prepare_no_param AS SELECT count(*) FROM distributed_table WHERE key = 1;
PREPARE local_prepare_param (int) AS SELECT count(*) FROM distributed_table WHERE key = $1;
PREPARE remote_prepare_param (int) AS SELECT count(*) FROM distributed_table WHERE key != $1;
BEGIN;
	-- 6 local execution without params
	EXECUTE local_prepare_no_param;
	EXECUTE local_prepare_no_param;
	EXECUTE local_prepare_no_param;
	EXECUTE local_prepare_no_param;
	EXECUTE local_prepare_no_param;
	EXECUTE local_prepare_no_param;

	-- 6 local executions with params
	EXECUTE local_prepare_param(1);
	EXECUTE local_prepare_param(5);
	EXECUTE local_prepare_param(6);
	EXECUTE local_prepare_param(1);
	EXECUTE local_prepare_param(5);
	EXECUTE local_prepare_param(6);

	-- followed by a non-local execution
	EXECUTE remote_prepare_param(1);
COMMIT;	


-- failures of local execution should rollback both the 
-- local execution and remote executions

-- fail on a local execution
BEGIN;
	INSERT INTO distributed_table VALUES (1, '11',21) ON CONFLICT(key) DO UPDATE SET value = '100' RETURNING *;

	UPDATE distributed_table SET value = '200';

	INSERT INTO distributed_table VALUES (1, '100',21) ON CONFLICT(key) DO UPDATE SET value = (1 / (100.0 - EXCLUDED.value::int))::text RETURNING *;
ROLLBACK;

-- we've rollbacked everything
SELECT count(*) FROM distributed_table WHERE value = '200';

-- RETURNING should just work fine for reference tables
INSERT INTO reference_table VALUES (500) RETURNING *;
DELETE FROM reference_table WHERE key = 500 RETURNING *;

-- should be able to skip local execution even if in a sequential mode of execution
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO sequential ;

	DELETE FROM distributed_table;
	INSERT INTO distributed_table VALUES (1, '11',21) ON CONFLICT(key) DO UPDATE SET value = '100' RETURNING *;
ROLLBACK;

-- sequential execution should just work fine after a local execution
BEGIN;
	SET citus.multi_shard_modify_mode TO sequential ;

	INSERT INTO distributed_table VALUES (1, '11',21) ON CONFLICT(key) DO UPDATE SET value = '100' RETURNING *;
	DELETE FROM distributed_table;
ROLLBACK;



-- load some data so that foreign keys won't complain with the next tests
TRUNCATE reference_table CASCADE;
INSERT INTO reference_table SELECT i FROM generate_series(500, 600) i;
INSERT INTO distributed_table SELECT i, i::text, i % 10 + 25 FROM generate_series(500, 600) i;

-- show that both local, and mixed local-distributed executions
-- calculate rows processed correctly
BEGIN;
	DELETE FROM distributed_table WHERE key = 500;

	DELETE FROM distributed_table WHERE value != '123123213123213';
ROLLBACK;

BEGIN;
	
	DELETE FROM reference_table WHERE key = 500 RETURNING *;

	DELETE FROM reference_table;
ROLLBACK;


-- mix with other executors should fail

-- router modify execution should error
BEGIN;
	DELETE FROM distributed_table WHERE key = 500;

	SET LOCAL citus.task_executor_type = 'real-time';

	DELETE FROM distributed_table;
ROLLBACK;

-- local execution should not be executed locally
-- becase a multi-shard router query has already been executed
BEGIN;

	SET LOCAL citus.task_executor_type = 'real-time';

	DELETE FROM distributed_table;

	DELETE FROM distributed_table WHERE key = 500;
ROLLBACK;

-- router select execution
BEGIN;
	DELETE FROM distributed_table WHERE key = 500;

	SET LOCAL citus.task_executor_type = 'real-time';

	SELECT count(*) FROM distributed_table WHERE key = 500;
ROLLBACK;

-- local execution should not be executed locally
-- becase a single-shard router query has already been executed
BEGIN;
	SET LOCAL citus.task_executor_type = 'real-time';

	SELECT count(*) FROM distributed_table WHERE key = 500;

	DELETE FROM distributed_table WHERE key = 500;
ROLLBACK;

-- real-time select execution
BEGIN;
	DELETE FROM distributed_table WHERE key = 500;

	SET LOCAL citus.task_executor_type = 'real-time';

	SELECT count(*) FROM distributed_table;
ROLLBACK;

-- local execution should not be executed locally
-- becase a real-time query has already been executed
BEGIN;
	SET LOCAL citus.task_executor_type = 'real-time';

	SELECT count(*) FROM distributed_table;

	DELETE FROM distributed_table WHERE key = 500;
ROLLBACK;

-- task-tracker select execution
BEGIN;
	DELETE FROM distributed_table WHERE key = 500;

	SET LOCAL citus.task_executor_type = 'task-tracker';

	SELECT count(*) FROM distributed_table;
ROLLBACK;

-- local execution should not be executed locally
-- becase a task-tracker query has already been executed
BEGIN;
	SET LOCAL citus.task_executor_type = 'task-tracker';
	SET LOCAL client_min_messages TO INFO;
	SELECT count(*) FROM distributed_table;
	SET LOCAL client_min_messages TO LOG;

	DELETE FROM distributed_table WHERE key = 500;
ROLLBACK;

-- probably not a realistic case since views are not very
-- well supported with MX
CREATE VIEW v_local_query_execution AS 
SELECT * FROM distributed_table WHERE key = 500;

SELECT * FROM v_local_query_execution;

-- similar test, but this time the view itself is a non-local
-- query, but the query on the view is local
CREATE VIEW v_local_query_execution_2 AS 
SELECT * FROM distributed_table;

SELECT * FROM v_local_query_execution_2 WHERE key = 500;

-- even if we switch from remote execution -> local execution,
-- we are able to use remote execution after rollback
BEGIN;
	SAVEPOINT my_savepoint;

	SELECT count(*) FROM distributed_table;

    DELETE FROM distributed_table WHERE key = 500;
	
    ROLLBACK TO SAVEPOINT my_savepoint;
	
	DELETE FROM distributed_table WHERE key = 500;

COMMIT;

-- even if we switch from local execution -> remote execution,
-- we are able to use local execution after rollback
BEGIN;
	   
	SAVEPOINT my_savepoint;

    DELETE FROM distributed_table WHERE key = 500;
	
	SELECT count(*) FROM distributed_table;

    ROLLBACK TO SAVEPOINT my_savepoint;
	
	DELETE FROM distributed_table WHERE key = 500;

COMMIT;

-- sanity check: local execution on partitions
BEGIN;
	INSERT INTO collections_list (key, collection_id) VALUES (1,0);
	SELECT count(*) FROM collections_list_0 WHERE key = 1;
	SELECT count(*) FROM collections_list;
COMMIT;

-- the final queries for the following CTEs are going to happen on the intermediate results only
-- one of them will be executed remotely, and the other is locally 
-- Citus currently doesn't allow using task_assignment_policy for intermediate results
WITH distributed_local_mixed AS (INSERT INTO reference_table VALUES (1000) RETURNING *) SELECT * FROM distributed_local_mixed;

\c - - - :master_port
SET client_min_messages TO ERROR;
SET search_path TO public;
DROP SCHEMA local_shard_execution CASCADE;

