--
-- LOCAL_SHARD_EXECUTION
--

CREATE SCHEMA local_shard_execution;
SET search_path TO local_shard_execution;

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
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
	key bigserial,
	ser bigserial,
	ts timestamptz,
	collection_id integer,
	value numeric,
	PRIMARY KEY(key, collection_id)
) PARTITION BY LIST (collection_id );

SELECT create_distributed_table('collections_list', 'key');

CREATE TABLE collections_list_0
	PARTITION OF collections_list (key, ser, ts, collection_id, value)
	FOR VALUES IN ( 0 );

-- create a volatile function that returns the local node id
CREATE OR REPLACE FUNCTION get_local_node_id_volatile()
RETURNS INT AS $$
DECLARE localGroupId int;
BEGIN
        SELECT groupid INTO localGroupId FROM pg_dist_local_group;
  RETURN localGroupId;
END; $$ language plpgsql VOLATILE;
SELECT create_distributed_function('get_local_node_id_volatile()');

-- test case for issue #3556
CREATE TABLE accounts (id text PRIMARY KEY);
CREATE TABLE stats (account_id text PRIMARY KEY, spent int);

SELECT create_distributed_table('accounts', 'id', colocate_with => 'none');
SELECT create_distributed_table('stats', 'account_id', colocate_with => 'accounts');

INSERT INTO accounts (id) VALUES ('foo');
INSERT INTO stats (account_id, spent) VALUES ('foo', 100);

CREATE TABLE abcd(a int, b int, c int, d int);
SELECT create_distributed_table('abcd', 'b');

INSERT INTO abcd VALUES (1,2,3,4);
INSERT INTO abcd VALUES (2,3,4,5);
INSERT INTO abcd VALUES (3,4,5,6);

ALTER TABLE abcd DROP COLUMN a;

-- connection worker and get ready for the tests
\c - - - :worker_1_port
SET search_path TO local_shard_execution;
SET citus.enable_unique_job_ids TO off;

-- returns true of the distribution key filter
-- on the distributed tables (e.g., WHERE key = 1), we'll hit a shard
-- placement which is local to this not
SET citus.enable_metadata_sync TO OFF;
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
RESET citus.enable_metadata_sync;

-- test case for issue #3556
SET citus.log_intermediate_results TO TRUE;
SET client_min_messages TO DEBUG1;

SELECT *
FROM
(
    WITH accounts_cte AS (
        SELECT id AS account_id
        FROM accounts
    ),
    joined_stats_cte_1 AS (
        SELECT spent, account_id
        FROM stats
        INNER JOIN accounts_cte USING (account_id)
    ),
    joined_stats_cte_2 AS (
        SELECT spent, account_id
        FROM joined_stats_cte_1
        INNER JOIN accounts_cte USING (account_id)
    )
    SELECT SUM(spent) OVER (PARTITION BY coalesce(account_id, NULL))
    FROM accounts_cte
    INNER JOIN joined_stats_cte_2 USING (account_id)
) inner_query;

SET citus.log_intermediate_results TO DEFAULT;
SET client_min_messages TO DEFAULT;

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
INSERT INTO distributed_table SELECT sum(key), value FROM distributed_table WHERE key = 1 GROUP BY value ON CONFLICT DO NOTHING;
INSERT INTO distributed_table SELECT 1, '1',15 FROM distributed_table WHERE key = 2 LIMIT 1 ON CONFLICT DO NOTHING;

-- sanity check: multi-shard INSERT..SELECT pushdown goes through distributed execution
INSERT INTO distributed_table SELECT * FROM distributed_table ON CONFLICT DO NOTHING;

-- Ensure tuple data in explain analyze output is the same on all PG versions
SET citus.enable_binary_protocol = TRUE;

-- EXPLAIN for local execution just works fine
-- though going through distributed execution
EXPLAIN (COSTS OFF) SELECT * FROM distributed_table WHERE key = 1 AND age = 20;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)   SELECT * FROM distributed_table WHERE key = 1 AND age = 20;

EXPLAIN (ANALYZE ON, COSTS OFF, SUMMARY OFF, TIMING OFF)
WITH r AS ( SELECT GREATEST(random(), 2) z,* FROM distributed_table)
SELECT 1 FROM r WHERE z < 3;

EXPLAIN (COSTS OFF) DELETE FROM distributed_table WHERE key = 1 AND age = 20;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) DELETE FROM distributed_table WHERE key = 1 AND age = 20;
-- show that EXPLAIN ANALYZE deleted the row and cascades deletes
SELECT * FROM distributed_table WHERE key = 1 AND age = 20 ORDER BY 1,2,3;
SELECT * FROM second_distributed_table WHERE key = 1 ORDER BY 1,2;
-- Put rows back for other tests
INSERT INTO distributed_table VALUES (1, '22', 20);
INSERT INTO second_distributed_table VALUES (1, '1');

SET citus.enable_ddl_propagation TO OFF;
CREATE VIEW abcd_view AS SELECT * FROM abcd;
RESET citus.enable_ddl_propagation;

SELECT * FROM abcd first join abcd second on first.b = second.b ORDER BY 1,2,3,4;

BEGIN;
SELECT * FROM abcd first join abcd second on first.b = second.b ORDER BY 1,2,3,4;
END;

BEGIN;
SELECT * FROM abcd_view first join abcd_view second on first.b = second.b ORDER BY 1,2,3,4;
END;

BEGIN;
SELECT * FROM abcd first full join abcd second on first.b = second.b ORDER BY 1,2,3,4;
END;

BEGIN;
SELECT * FROM abcd first join abcd second USING(b) ORDER BY 1,2,3,4;
END;

BEGIN;
SELECT * FROM abcd first join abcd second USING(b) join abcd third on first.b=third.b ORDER BY 1,2,3,4;
END;

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
	-- 	   executor has to error out

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
SELECT * FROM second_distributed_table ORDER BY 1;

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

-- a local query followed by TRUNCATE command can be executed locally
BEGIN;
	SELECT count(*) FROM distributed_table WHERE key = 1;
	TRUNCATE distributed_table CASCADE;
ROLLBACK;

-- a local query is followed by an INSERT..SELECT via the coordinator
BEGIN;
	SELECT count(*) FROM distributed_table WHERE key = 1;

	INSERT INTO distributed_table (key) SELECT i FROM generate_series(1,1) i;
ROLLBACK;

BEGIN;
SET citus.enable_repartition_joins TO ON;
SELECT count(*) FROM distributed_table;
SELECT count(*) FROM distributed_table d1 join distributed_table d2 using(age);
ROLLBACK;

-- a local query is followed by an INSERT..SELECT with re-partitioning
BEGIN;
	SELECT count(*) FROM distributed_table WHERE key = 6;
	INSERT INTO reference_table (key) SELECT -key FROM distributed_table;
	INSERT INTO distributed_table (key) SELECT -key FROM distributed_table;
	SELECT count(*) FROM distributed_table WHERE key = -6;
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
SET citus.enable_metadata_sync TO OFF;
CREATE OR REPLACE PROCEDURE only_local_execution() AS $$
		DECLARE cnt INT;
		BEGIN
			INSERT INTO distributed_table VALUES (1, '11',21) ON CONFLICT(key) DO UPDATE SET value = '29';
			SELECT count(*) INTO cnt FROM distributed_table WHERE key = 1;
			DELETE FROM distributed_table WHERE key = 1;
        END;
$$ LANGUAGE plpgsql;

CALL only_local_execution();

-- insert a row that we need in the next tests
INSERT INTO distributed_table VALUES (1, '11',21) ON CONFLICT(key) DO UPDATE SET value = '29';

-- make sure that functions can use local execution
CREATE OR REPLACE PROCEDURE only_local_execution_with_function_evaluation() AS $$
		DECLARE nodeId INT;
		BEGIN
			-- fast path router
			SELECT get_local_node_id_volatile() INTO nodeId FROM distributed_table WHERE key = 1;
			IF nodeId <= 0 THEN
				RAISE NOTICE 'unexpected node id';
			END IF;

			-- regular router
			SELECT get_local_node_id_volatile() INTO nodeId FROM distributed_table d1 JOIN distributed_table d2 USING (key) WHERE d1.key = 1;
			IF nodeId <= 0 THEN
				RAISE NOTICE 'unexpected node id';
			END IF;
		END;
$$ LANGUAGE plpgsql;

CALL only_local_execution_with_function_evaluation();

CREATE OR REPLACE PROCEDURE only_local_execution_with_params(int) AS $$
		DECLARE cnt INT;
		BEGIN
			INSERT INTO distributed_table VALUES ($1, '11',21) ON CONFLICT(key) DO UPDATE SET value = '29';
			SELECT count(*) INTO cnt FROM distributed_table WHERE key = $1;
			DELETE FROM distributed_table WHERE key = $1;
        END;
$$ LANGUAGE plpgsql;

CALL only_local_execution_with_params(1);

CREATE OR REPLACE PROCEDURE only_local_execution_with_function_evaluation_param(int) AS $$
		DECLARE nodeId INT;
		BEGIN
			-- fast path router
			SELECT get_local_node_id_volatile() INTO nodeId FROM distributed_table WHERE key = $1;
			IF nodeId <= 0 THEN
				RAISE NOTICE 'unexpected node id';
			END IF;

			-- regular router
			SELECT get_local_node_id_volatile() INTO nodeId FROM distributed_table d1 JOIN distributed_table d2 USING (key) WHERE d1.key = $1;
			IF nodeId <= 0 THEN
				RAISE NOTICE 'unexpected node id';
			END IF;
		END;
$$ LANGUAGE plpgsql;

CALL only_local_execution_with_function_evaluation_param(1);

CREATE OR REPLACE PROCEDURE local_execution_followed_by_dist() AS $$
		DECLARE cnt INT;
		BEGIN
			INSERT INTO distributed_table VALUES (1, '11',21) ON CONFLICT(key) DO UPDATE SET value = '29';
			SELECT count(*) INTO cnt FROM distributed_table WHERE key = 1;
			DELETE FROM distributed_table;
			SELECT count(*) INTO cnt FROM distributed_table;
        END;
$$ LANGUAGE plpgsql;
RESET citus.enable_metadata_sync;

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
-- note that if we allow Postgres to inline the CTE (e.g., not have the EXISTS
-- subquery), then it'd pushdown the filters and the query becomes single-shard,
-- locally executable query
WITH all_data AS (SELECT * FROM distributed_table)
SELECT
	count(*)
FROM
	distributed_table, all_data
WHERE
	distributed_table.key = all_data.key AND distributed_table.key = 1
	AND EXISTS (SELECT * FROM all_data);

-- in pg12, the following CTE can be inlined, still the query becomes
-- a subquery that needs to be recursively planned and a parallel
-- query, so do not use local execution
WITH all_data AS (SELECT age FROM distributed_table)
SELECT
	count(*)
FROM
	distributed_table, all_data
WHERE
	distributed_table.key = all_data.age AND distributed_table.key = 1;

-- get ready for the next commands
TRUNCATE reference_table, distributed_table, second_distributed_table;

-- local execution of returning of reference tables
INSERT INTO reference_table VALUES (1),(2),(3),(4),(5),(6) RETURNING *;

-- local execution of multi-row INSERTs
INSERT INTO distributed_table VALUES (1, '11',21), (5,'55',22) ON CONFLICT(key) DO UPDATE SET value = (EXCLUDED.value::int + 1)::text RETURNING *;


-- distributed execution of multi-rows INSERTs, where executor
-- is smart enough to execute local tasks via local execution
INSERT INTO distributed_table VALUES (1, '11',21), (2,'22',22), (3,'33',33), (4,'44',44),(5,'55',55) ON CONFLICT(key) DO UPDATE SET value = (EXCLUDED.value::int + 1)::text RETURNING *;


PREPARE local_prepare_no_param AS SELECT count(*) FROM distributed_table WHERE key = 1;
PREPARE local_prepare_no_param_subquery AS
SELECT DISTINCT trim(value) FROM (
    SELECT value FROM distributed_table
    WHERE
        key IN (1, 6, 500, 701)
        AND (select 2) > random()
        order by 1
        limit 2
    ) t;
PREPARE local_prepare_param (int) AS SELECT count(*) FROM distributed_table WHERE key = $1;
PREPARE remote_prepare_param (int) AS SELECT count(*) FROM distributed_table WHERE key != $1;
BEGIN;
	-- 8 local execution without params
	EXECUTE local_prepare_no_param;
	EXECUTE local_prepare_no_param;
	EXECUTE local_prepare_no_param;
	EXECUTE local_prepare_no_param;
	EXECUTE local_prepare_no_param;
	EXECUTE local_prepare_no_param;
	EXECUTE local_prepare_no_param;
	EXECUTE local_prepare_no_param;

	-- 8 local execution without params and some subqueries
	EXECUTE local_prepare_no_param_subquery;
	EXECUTE local_prepare_no_param_subquery;
	EXECUTE local_prepare_no_param_subquery;
	EXECUTE local_prepare_no_param_subquery;
	EXECUTE local_prepare_no_param_subquery;
	EXECUTE local_prepare_no_param_subquery;
	EXECUTE local_prepare_no_param_subquery;
	EXECUTE local_prepare_no_param_subquery;

	-- 8 local executions with params
	EXECUTE local_prepare_param(1);
	EXECUTE local_prepare_param(5);
	EXECUTE local_prepare_param(6);
	EXECUTE local_prepare_param(1);
	EXECUTE local_prepare_param(5);
	EXECUTE local_prepare_param(6);
	EXECUTE local_prepare_param(6);
	EXECUTE local_prepare_param(6);

	-- followed by a non-local execution
	EXECUTE remote_prepare_param(1);
COMMIT;

PREPARE local_insert_prepare_no_param AS INSERT INTO distributed_table VALUES (1+0*random(), '11',21::int) ON CONFLICT(key) DO UPDATE SET value = '29' || '28' RETURNING *, key + 1, value || '30', age * 15;
PREPARE local_insert_prepare_param (int) AS INSERT INTO distributed_table VALUES ($1+0*random(), '11',21::int) ON CONFLICT(key) DO UPDATE SET value = '29' || '28' RETURNING *, key + 1, value || '30', age * 15;
BEGIN;
	-- 8 local execution without params
	EXECUTE local_insert_prepare_no_param;
	EXECUTE local_insert_prepare_no_param;
	EXECUTE local_insert_prepare_no_param;
	EXECUTE local_insert_prepare_no_param;
	EXECUTE local_insert_prepare_no_param;
	EXECUTE local_insert_prepare_no_param;
	EXECUTE local_insert_prepare_no_param;
	EXECUTE local_insert_prepare_no_param;

	-- 8 local executions with params
	EXECUTE local_insert_prepare_param(1);
	EXECUTE local_insert_prepare_param(5);
	EXECUTE local_insert_prepare_param(6);
	EXECUTE local_insert_prepare_param(1);
	EXECUTE local_insert_prepare_param(5);
	EXECUTE local_insert_prepare_param(6);
	EXECUTE local_insert_prepare_param(6);
	EXECUTE local_insert_prepare_param(6);

	-- followed by a non-local execution
	EXECUTE remote_prepare_param(2);
COMMIT;

PREPARE local_multi_row_insert_prepare_no_param AS
	INSERT INTO distributed_table VALUES (1,'55', 21), (5,'15',33) ON CONFLICT (key) WHERE key > 3 and key < 4 DO UPDATE SET value = '88' || EXCLUDED.value;

PREPARE local_multi_row_insert_prepare_no_param_multi_shard AS
	INSERT INTO distributed_table VALUES (6,'55', 21), (5,'15',33) ON CONFLICT (key) WHERE key > 3 AND key < 4 DO UPDATE SET value = '88' || EXCLUDED.value;;

PREPARE local_multi_row_insert_prepare_params(int,int) AS
	INSERT INTO distributed_table VALUES ($1,'55', 21), ($2,'15',33) ON CONFLICT (key) WHERE key > 3 and key < 4 DO UPDATE SET value = '88' || EXCLUDED.value;;
INSERT INTO reference_table VALUES (11);
BEGIN;
	EXECUTE local_multi_row_insert_prepare_no_param;
	EXECUTE local_multi_row_insert_prepare_no_param;
	EXECUTE local_multi_row_insert_prepare_no_param;
	EXECUTE local_multi_row_insert_prepare_no_param;
	EXECUTE local_multi_row_insert_prepare_no_param;
	EXECUTE local_multi_row_insert_prepare_no_param;
	EXECUTE local_multi_row_insert_prepare_no_param;
	EXECUTE local_multi_row_insert_prepare_no_param;

	EXECUTE local_multi_row_insert_prepare_no_param_multi_shard;
	EXECUTE local_multi_row_insert_prepare_no_param_multi_shard;
	EXECUTE local_multi_row_insert_prepare_no_param_multi_shard;
	EXECUTE local_multi_row_insert_prepare_no_param_multi_shard;
	EXECUTE local_multi_row_insert_prepare_no_param_multi_shard;
	EXECUTE local_multi_row_insert_prepare_no_param_multi_shard;
	EXECUTE local_multi_row_insert_prepare_no_param_multi_shard;
	EXECUTE local_multi_row_insert_prepare_no_param_multi_shard;

	EXECUTE local_multi_row_insert_prepare_params(1,6);
	EXECUTE local_multi_row_insert_prepare_params(1,5);
	EXECUTE local_multi_row_insert_prepare_params(6,5);
	EXECUTE local_multi_row_insert_prepare_params(5,1);
	EXECUTE local_multi_row_insert_prepare_params(5,6);
	EXECUTE local_multi_row_insert_prepare_params(5,1);
	EXECUTE local_multi_row_insert_prepare_params(1,6);
	EXECUTE local_multi_row_insert_prepare_params(1,5);

	-- one task is remote
	EXECUTE local_multi_row_insert_prepare_params(5,11);
ROLLBACK;

-- make sure that we still get results if we switch off local execution
PREPARE ref_count_prepare AS SELECT count(*) FROM reference_table;
EXECUTE ref_count_prepare;
EXECUTE ref_count_prepare;
EXECUTE ref_count_prepare;
EXECUTE ref_count_prepare;
EXECUTE ref_count_prepare;
SET citus.enable_local_execution TO off;
EXECUTE ref_count_prepare;
RESET citus.enable_local_execution;

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


BEGIN;
	DELETE FROM distributed_table WHERE key = 500;

	SELECT count(*) FROM distributed_table;
ROLLBACK;

BEGIN;
	SET LOCAL client_min_messages TO INFO;
	SELECT count(*) FROM distributed_table;
	SET LOCAL client_min_messages TO LOG;

	DELETE FROM distributed_table WHERE key = 500;
ROLLBACK;

-- probably not a realistic case since views are not very
-- well supported with MX
SET citus.enable_ddl_propagation TO OFF;
CREATE VIEW v_local_query_execution AS
SELECT * FROM distributed_table WHERE key = 500;
RESET citus.enable_ddl_propagation;

SELECT * FROM v_local_query_execution;

-- similar test, but this time the view itself is a non-local
-- query, but the query on the view is local
SET citus.enable_ddl_propagation TO OFF;
CREATE VIEW v_local_query_execution_2 AS
SELECT * FROM distributed_table;
RESET citus.enable_ddl_propagation;

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
INSERT INTO collections_list (collection_id) VALUES (0) RETURNING *;

BEGIN;
	INSERT INTO collections_list (key, collection_id) VALUES (1,0);
	SELECT count(*) FROM collections_list_0 WHERE key = 1;
	SELECT count(*) FROM collections_list;
	SELECT * FROM collections_list ORDER BY 1,2,3,4;
COMMIT;


TRUNCATE collections_list;

-- make sure that even if local execution is used, the sequence values
-- are generated locally
SET citus.enable_ddl_propagation TO OFF;
ALTER SEQUENCE collections_list_key_seq NO MINVALUE NO MAXVALUE;
RESET citus.enable_ddl_propagation;

PREPARE serial_prepared_local AS INSERT INTO collections_list (collection_id) VALUES (0) RETURNING key, ser;

SELECT setval('collections_list_key_seq', 4);
EXECUTE serial_prepared_local;
SELECT setval('collections_list_key_seq', 5);
EXECUTE serial_prepared_local;
SELECT setval('collections_list_key_seq', 499);
EXECUTE serial_prepared_local;
SELECT setval('collections_list_key_seq', 700);
EXECUTE serial_prepared_local;
SELECT setval('collections_list_key_seq', 708);
EXECUTE serial_prepared_local;
SELECT setval('collections_list_key_seq', 709);
EXECUTE serial_prepared_local;

-- get ready for the next executions
DELETE FROM collections_list WHERE key IN (5,6);
SELECT setval('collections_list_key_seq', 4);
EXECUTE serial_prepared_local;
SELECT setval('collections_list_key_seq', 5);
EXECUTE serial_prepared_local;

-- and, one remote test
SELECT setval('collections_list_key_seq', 10);
EXECUTE serial_prepared_local;

-- the final queries for the following CTEs are going to happen on the intermediate results only
-- one of them will be executed remotely, and the other is locally
-- Citus currently doesn't allow using task_assignment_policy for intermediate results
WITH distributed_local_mixed AS (INSERT INTO reference_table VALUES (1000) RETURNING *) SELECT * FROM distributed_local_mixed;

-- clean the table for the next tests
SET search_path TO local_shard_execution;
TRUNCATE distributed_table CASCADE;

-- load some data on a remote shard
INSERT INTO reference_table (key) VALUES (1), (2);
INSERT INTO distributed_table (key) VALUES (2);
BEGIN;
    -- local execution followed by a distributed query
    INSERT INTO distributed_table (key) VALUES (1);
    DELETE FROM distributed_table RETURNING key;
COMMIT;

-- a similar test with a reference table
TRUNCATE reference_table CASCADE;

-- load some data on a remote shard
INSERT INTO reference_table (key) VALUES (2);
BEGIN;
    -- local execution followed by a distributed query
    INSERT INTO reference_table (key) VALUES (1);
    DELETE FROM reference_table RETURNING key;
COMMIT;

-- however complex the query, local execution can handle
SET client_min_messages TO LOG;
SET citus.log_local_commands TO ON;
WITH cte_1 AS
  (SELECT *
   FROM
     (WITH cte_1 AS
        (SELECT *
         FROM distributed_table
         WHERE key = 1) SELECT *
      FROM cte_1) AS foo)
SELECT count(*)
FROM cte_1
JOIN distributed_table USING (key)
WHERE distributed_table.key = 1
  AND distributed_table.key IN
    (SELECT key
     FROM distributed_table
     WHERE key = 1);

RESET client_min_messages;
RESET citus.log_local_commands;

\c - - - :master_port
SET search_path TO local_shard_execution;
SET citus.next_shard_id TO 1480000;
-- test both local and remote execution with custom type
SET citus.shard_replication_factor TO 1;
CREATE TYPE invite_resp AS ENUM ('yes', 'no', 'maybe');

CREATE TABLE event_responses (
  event_id int,
  user_id int,
  response invite_resp,
  primary key (event_id, user_id)
);

SELECT create_distributed_table('event_responses', 'event_id');

INSERT INTO event_responses VALUES (1, 1, 'yes'), (2, 2, 'yes'), (3, 3, 'no'), (4, 4, 'no');


CREATE TABLE event_responses_no_pkey (
  event_id int,
  user_id int,
  response invite_resp
);

SELECT create_distributed_table('event_responses_no_pkey', 'event_id');



CREATE OR REPLACE FUNCTION regular_func(p invite_resp)
RETURNS int AS $$
DECLARE
	q1Result INT;
	q2Result INT;
	q3Result INT;
BEGIN
SELECT count(*) INTO q1Result FROM event_responses WHERE response = $1;
SELECT count(*) INTO q2Result FROM event_responses e1 LEFT JOIN event_responses e2 USING (event_id) WHERE e2.response = $1;
SELECT count(*) INTO q3Result FROM (SELECT * FROM event_responses WHERE response = $1 LIMIT 5) as foo;
RETURN  q3Result+q2Result+q1Result;
END;
$$ LANGUAGE plpgsql;

SELECT regular_func('yes');
SELECT regular_func('yes');
SELECT regular_func('yes');
SELECT regular_func('yes');
SELECT regular_func('yes');
SELECT regular_func('yes');
SELECT regular_func('yes');
SELECT regular_func('yes');

CREATE OR REPLACE PROCEDURE regular_procedure(p invite_resp)
AS $$
BEGIN
PERFORM * FROM event_responses WHERE response = $1 ORDER BY 1 DESC, 2 DESC, 3 DESC;
PERFORM * FROM event_responses e1 LEFT JOIN event_responses e2 USING (event_id) WHERE e2.response = $1 ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC;
PERFORM  * FROM (SELECT * FROM event_responses WHERE response = $1 LIMIT 5) as foo ORDER BY 1 DESC, 2 DESC, 3 DESC;
END;
$$ LANGUAGE plpgsql;

CALL regular_procedure('no');
CALL regular_procedure('no');
CALL regular_procedure('no');
CALL regular_procedure('no');
CALL regular_procedure('no');
CALL regular_procedure('no');
CALL regular_procedure('no');
CALL regular_procedure('no');

PREPARE multi_shard_no_dist_key(invite_resp) AS select * from event_responses where response = $1::invite_resp ORDER BY 1 DESC, 2 DESC, 3 DESC LIMIT 1;
EXECUTE multi_shard_no_dist_key('yes');
EXECUTE multi_shard_no_dist_key('yes');
EXECUTE multi_shard_no_dist_key('yes');
EXECUTE multi_shard_no_dist_key('yes');
EXECUTE multi_shard_no_dist_key('yes');
EXECUTE multi_shard_no_dist_key('yes');
EXECUTE multi_shard_no_dist_key('yes');
EXECUTE multi_shard_no_dist_key('yes');

PREPARE multi_shard_with_dist_key(int, invite_resp) AS select * from event_responses where event_id > $1 AND response = $2::invite_resp ORDER BY 1 DESC, 2 DESC, 3 DESC LIMIT 1;
EXECUTE multi_shard_with_dist_key(1, 'yes');
EXECUTE multi_shard_with_dist_key(1, 'yes');
EXECUTE multi_shard_with_dist_key(1, 'yes');
EXECUTE multi_shard_with_dist_key(1, 'yes');
EXECUTE multi_shard_with_dist_key(1, 'yes');
EXECUTE multi_shard_with_dist_key(1, 'yes');
EXECUTE multi_shard_with_dist_key(1, 'yes');
EXECUTE multi_shard_with_dist_key(1, 'yes');

PREPARE query_pushdown_no_dist_key(invite_resp) AS select * from event_responses e1 LEFT JOIN event_responses e2 USING(event_id) where e1.response = $1::invite_resp ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC LIMIT 1;
EXECUTE query_pushdown_no_dist_key('yes');
EXECUTE query_pushdown_no_dist_key('yes');
EXECUTE query_pushdown_no_dist_key('yes');
EXECUTE query_pushdown_no_dist_key('yes');
EXECUTE query_pushdown_no_dist_key('yes');
EXECUTE query_pushdown_no_dist_key('yes');
EXECUTE query_pushdown_no_dist_key('yes');
EXECUTE query_pushdown_no_dist_key('yes');

PREPARE insert_select_via_coord(invite_resp) AS INSERT INTO event_responses SELECT * FROM event_responses where response = $1::invite_resp LIMIT 1 ON CONFLICT (event_id, user_id) DO NOTHING ;
EXECUTE insert_select_via_coord('yes');
EXECUTE insert_select_via_coord('yes');
EXECUTE insert_select_via_coord('yes');
EXECUTE insert_select_via_coord('yes');
EXECUTE insert_select_via_coord('yes');
EXECUTE insert_select_via_coord('yes');
EXECUTE insert_select_via_coord('yes');
EXECUTE insert_select_via_coord('yes');

PREPARE insert_select_pushdown(invite_resp) AS INSERT INTO event_responses SELECT * FROM event_responses where response = $1::invite_resp ON CONFLICT (event_id, user_id) DO NOTHING;
EXECUTE insert_select_pushdown('yes');
EXECUTE insert_select_pushdown('yes');
EXECUTE insert_select_pushdown('yes');
EXECUTE insert_select_pushdown('yes');
EXECUTE insert_select_pushdown('yes');
EXECUTE insert_select_pushdown('yes');
EXECUTE insert_select_pushdown('yes');
EXECUTE insert_select_pushdown('yes');

PREPARE router_select_with_no_dist_key_filter(invite_resp) AS select * from event_responses where event_id = 1 AND response = $1::invite_resp ORDER BY 1 DESC, 2 DESC, 3 DESC LIMIT 1;
EXECUTE router_select_with_no_dist_key_filter('yes');
EXECUTE router_select_with_no_dist_key_filter('yes');
EXECUTE router_select_with_no_dist_key_filter('yes');
EXECUTE router_select_with_no_dist_key_filter('yes');
EXECUTE router_select_with_no_dist_key_filter('yes');
EXECUTE router_select_with_no_dist_key_filter('yes');
EXECUTE router_select_with_no_dist_key_filter('yes');
EXECUTE router_select_with_no_dist_key_filter('yes');

-- rest of the tests assume the table is empty
TRUNCATE event_responses;

CREATE OR REPLACE PROCEDURE register_for_event(p_event_id int, p_user_id int, p_choice invite_resp)
LANGUAGE plpgsql
SET search_path TO local_shard_execution
AS $fn$
BEGIN
   INSERT INTO event_responses VALUES (p_event_id, p_user_id, p_choice)
   ON CONFLICT (event_id, user_id)
   DO UPDATE SET response = EXCLUDED.response;

   PERFORM count(*) FROM event_responses WHERE event_id = p_event_id;

   PERFORM count(*) FROM event_responses WHERE event_id = p_event_id AND false;

   UPDATE event_responses SET response = p_choice WHERE event_id = p_event_id;

END;
$fn$;

SELECT create_distributed_function('register_for_event(int,int,invite_resp)', 'p_event_id', 'event_responses');

-- call 8 times to make sure it works after the 5th time(postgres binds values after the 5th time and Citus 2nd time)
-- after 6th, the local execution caches the local plans and uses it
-- execute it both locally and remotely
CALL register_for_event(16, 1, 'yes');
CALL register_for_event(16, 1, 'yes');
CALL register_for_event(16, 1, 'yes');
CALL register_for_event(16, 1, 'yes');
CALL register_for_event(16, 1, 'yes');
CALL register_for_event(16, 1, 'yes');
CALL register_for_event(16, 1, 'yes');
CALL register_for_event(16, 1, 'yes');
CALL register_for_event(16, 1, 'yes');

\c - - - :worker_2_port
SET search_path TO local_shard_execution;
CALL register_for_event(16, 1, 'yes');
CALL register_for_event(16, 1, 'yes');
CALL register_for_event(16, 1, 'yes');
CALL register_for_event(16, 1, 'yes');
CALL register_for_event(16, 1, 'yes');
CALL register_for_event(16, 1, 'yes');
CALL register_for_event(16, 1, 'yes');
CALL register_for_event(16, 1, 'yes');
CALL register_for_event(16, 1, 'yes');

-- values 16, 17 and 19 hits the same
-- shard, so we're re-using the same cached
-- plans per statement across different distribution
--  key values
CALL register_for_event(17, 1, 'yes');
CALL register_for_event(19, 1, 'yes');
CALL register_for_event(17, 1, 'yes');
CALL register_for_event(19, 1, 'yes');

-- should work fine if the logs are enabled
\set VERBOSITY terse
SET citus.log_local_commands TO ON;
SET client_min_messages TO DEBUG2;
CALL register_for_event(19, 1, 'yes');

-- should be fine even if no parameters exists in the query
SELECT count(*) FROM event_responses WHERE event_id = 16;
SELECT count(*) FROM event_responses WHERE event_id = 16;
UPDATE event_responses SET response = 'no' WHERE event_id = 16;
INSERT INTO event_responses VALUES (16, 666, 'maybe')
ON CONFLICT (event_id, user_id)
DO UPDATE SET response = EXCLUDED.response RETURNING *;

-- multi row INSERTs hitting the same shard
INSERT INTO event_responses VALUES (16, 666, 'maybe'), (17, 777, 'no')
ON CONFLICT (event_id, user_id)
DO UPDATE SET response = EXCLUDED.response RETURNING *;

-- now, similar tests with some settings changed
SET citus.enable_local_execution TO false;
SET citus.enable_fast_path_router_planner TO false;
CALL register_for_event(19, 1, 'yes');

-- should be fine even if no parameters exists in the query
SELECT count(*) FROM event_responses WHERE event_id = 16;
SELECT count(*) FROM event_responses WHERE event_id = 16;
UPDATE event_responses SET response = 'no' WHERE event_id = 16;
INSERT INTO event_responses VALUES (16, 666, 'maybe')
ON CONFLICT (event_id, user_id)
DO UPDATE SET response = EXCLUDED.response RETURNING *;

-- multi row INSERTs hitting the same shard
INSERT INTO event_responses VALUES (16, 666, 'maybe'), (17, 777, 'no')
ON CONFLICT (event_id, user_id)
DO UPDATE SET response = EXCLUDED.response RETURNING *;

-- set back to sane settings
RESET citus.enable_local_execution;
RESET citus.enable_fast_path_router_planner;


-- we'll test some 2PC states
SET citus.enable_metadata_sync TO OFF;

-- coordinated_transaction_should_use_2PC prints the internal
-- state for 2PC decision on Citus. However, even if 2PC is decided,
-- we may not necessarily use 2PC over a connection unless it does
-- a modification
CREATE OR REPLACE FUNCTION coordinated_transaction_should_use_2PC()
RETURNS BOOL LANGUAGE C STRICT VOLATILE AS 'citus',
$$coordinated_transaction_should_use_2PC$$;

-- make tests consistent
SET citus.max_adaptive_executor_pool_size TO 1;

RESET citus.enable_metadata_sync;
SELECT recover_prepared_transactions();


SET citus.log_remote_commands TO ON;

-- we use event_id = 2 for local execution and event_id = 1 for reemote execution
--show it here, if anything changes here, all the tests below might be broken
-- we prefer this to avoid excessive logging below
SELECT * FROM event_responses_no_pkey WHERE event_id = 2;
SELECT * FROM event_responses_no_pkey WHERE event_id = 1;
RESET citus.log_remote_commands;
RESET citus.log_local_commands;
RESET client_min_messages;

-- single shard local command without transaction block does set the
-- internal state for 2PC, but does not require any actual entries
WITH cte_1 AS (INSERT INTO event_responses_no_pkey VALUES (2, 2, 'yes') RETURNING *)
SELECT coordinated_transaction_should_use_2PC() FROM cte_1;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- two local commands without transaction block set the internal 2PC state
-- but does not use remotely
WITH cte_1 AS (INSERT INTO event_responses_no_pkey VALUES (2, 2, 'yes') RETURNING *),
	 cte_2 AS (INSERT INTO event_responses_no_pkey VALUES (2, 2, 'yes') RETURNING *)
SELECT bool_or(coordinated_transaction_should_use_2PC()) FROM cte_1, cte_2;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- single shard local modification followed by another single shard
-- local modification sets the 2PC state, but does not use remotely
BEGIN;
	INSERT INTO event_responses_no_pkey VALUES (2, 2, 'yes') RETURNING *;
	INSERT INTO event_responses_no_pkey VALUES (2, 2, 'yes') RETURNING *;
	SELECT coordinated_transaction_should_use_2PC();
COMMIT;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- single shard local modification followed by a single shard
-- remote modification uses 2PC because multiple nodes involved
-- in the modification
BEGIN;
	INSERT INTO event_responses_no_pkey VALUES (2, 2, 'yes') RETURNING *;
	INSERT INTO event_responses_no_pkey VALUES (1, 2, 'yes') RETURNING *;
	SELECT coordinated_transaction_should_use_2PC();
COMMIT;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- single shard local modification followed by a single shard
-- remote modification uses 2PC even if it is not in an explicit
-- tx block as multiple nodes involved in the modification
WITH cte_1 AS (INSERT INTO event_responses_no_pkey VALUES (2, 2, 'yes') RETURNING *),
	 cte_2 AS (INSERT INTO event_responses_no_pkey VALUES (1, 1, 'yes') RETURNING *)
SELECT bool_or(coordinated_transaction_should_use_2PC()) FROM cte_1, cte_2;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();


-- single shard remote modification followed by a single shard
-- local modification uses 2PC as multiple nodes involved
-- in the modification
BEGIN;
	INSERT INTO event_responses_no_pkey VALUES (1, 2, 'yes') RETURNING *;
	INSERT INTO event_responses_no_pkey VALUES (2, 2, 'yes') RETURNING *;
	SELECT coordinated_transaction_should_use_2PC();
COMMIT;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- single shard remote modification followed by a single shard
-- local modification uses 2PC even if it is not in an explicit
-- tx block
WITH cte_1 AS (INSERT INTO event_responses_no_pkey VALUES (1, 1, 'yes') RETURNING *),
	 cte_2 AS (INSERT INTO event_responses_no_pkey VALUES (2, 2, 'yes') RETURNING *)
SELECT bool_or(coordinated_transaction_should_use_2PC()) FROM cte_1, cte_2;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- single shard local SELECT command without transaction block does not set the
-- internal state for 2PC
WITH cte_1 AS (SELECT * FROM event_responses_no_pkey WHERE event_id = 2)
SELECT coordinated_transaction_should_use_2PC() FROM cte_1;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- two local SELECT commands without transaction block does not set the internal 2PC state
-- and does not use remotely
WITH cte_1 AS (SELECT count(*) FROM event_responses_no_pkey WHERE event_id = 2),
	 cte_2 AS (SELECT count(*) FROM event_responses_no_pkey WHERE event_id = 2)
SELECT count(*) FROM cte_1, cte_2;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- two local SELECT commands without transaction block does not set the internal 2PC state
-- and does not use remotely
BEGIN;
	SELECT count(*) FROM event_responses_no_pkey WHERE event_id = 2;
	SELECT count(*) FROM event_responses_no_pkey WHERE event_id = 2;
	SELECT coordinated_transaction_should_use_2PC();
COMMIT;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- a local SELECT followed by a remote SELECT does not require to
-- use actual 2PC
BEGIN;
	SELECT count(*) FROM event_responses_no_pkey WHERE event_id = 2;
	SELECT count(*) FROM event_responses_no_pkey;
COMMIT;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- single shard local SELECT followed by a single shard
-- remote modification does not use 2PC, because only a single
-- machine involved in the modification
BEGIN;
	SELECT * FROM event_responses_no_pkey WHERE event_id = 2;
	INSERT INTO event_responses_no_pkey VALUES (1, 2, 'yes') RETURNING *;
	SELECT coordinated_transaction_should_use_2PC();
COMMIT;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- single shard local SELECT followed by a single shard
-- remote modification does not use 2PC, because only a single
-- machine involved in the modification
WITH cte_1 AS (SELECT * FROM event_responses_no_pkey WHERE event_id = 2),
	 cte_2 AS (INSERT INTO event_responses_no_pkey VALUES (1, 1, 'yes') RETURNING *)
SELECT bool_or(coordinated_transaction_should_use_2PC()) FROM cte_1, cte_2;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- single shard remote modification followed by a single shard
-- local SELECT does not use 2PC, because only a single
-- machine involved in the modification
BEGIN;
	INSERT INTO event_responses_no_pkey VALUES (1, 2, 'yes') RETURNING *;
	SELECT count(*) FROM event_responses_no_pkey WHERE event_id = 2;
	SELECT coordinated_transaction_should_use_2PC();
COMMIT;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- single shard remote modification followed by a single shard
-- local SELECT does not use 2PC, because only a single
-- machine involved in the modification
WITH cte_1 AS (INSERT INTO event_responses_no_pkey VALUES (1, 1, 'yes') RETURNING *),
	 cte_2 AS (SELECT * FROM event_responses_no_pkey WHERE event_id = 2)
SELECT bool_or(coordinated_transaction_should_use_2PC()) FROM cte_1, cte_2;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- multi shard local SELECT command without transaction block does not set the
-- internal state for 2PC
WITH cte_1 AS (SELECT count(*) FROM event_responses_no_pkey)
SELECT coordinated_transaction_should_use_2PC() FROM cte_1;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- two multi-shard SELECT commands without transaction block does not set the internal 2PC state
-- and does not use remotely
WITH cte_1 AS (SELECT count(*) FROM event_responses_no_pkey),
	 cte_2 AS (SELECT count(*) FROM event_responses_no_pkey)
SELECT bool_or(coordinated_transaction_should_use_2PC()) FROM cte_1, cte_2;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- two multi-shard SELECT commands without transaction block does not set the internal 2PC state
-- and does not use remotely
BEGIN;
	SELECT count(*) FROM event_responses_no_pkey;
	SELECT count(*) FROM event_responses_no_pkey;
	SELECT coordinated_transaction_should_use_2PC();
COMMIT;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- multi-shard shard SELECT followed by a single shard
-- remote modification does not use 2PC, because only a single
-- machine involved in the modification
BEGIN;
	SELECT count(*) FROM event_responses_no_pkey;
	INSERT INTO event_responses_no_pkey VALUES (1, 2, 'yes') RETURNING *;
	SELECT coordinated_transaction_should_use_2PC();
COMMIT;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- multi shard SELECT followed by a single shard
-- remote single shard modification does not use 2PC, because only a single
-- machine involved in the modification
WITH cte_1 AS (SELECT count(*) FROM event_responses_no_pkey),
	 cte_2 AS (INSERT INTO event_responses_no_pkey VALUES (1, 1, 'yes') RETURNING *)
SELECT bool_or(coordinated_transaction_should_use_2PC()) FROM cte_1, cte_2;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- single shard remote modification followed by a multi shard
-- SELECT does not use 2PC, because only a single
-- machine involved in the modification
BEGIN;
	INSERT INTO event_responses_no_pkey VALUES (1, 2, 'yes') RETURNING *;
	SELECT count(*) FROM event_responses_no_pkey;
	SELECT coordinated_transaction_should_use_2PC();
COMMIT;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- single shard remote modification followed by a multi shard
-- SELECT does not use 2PC, because only a single
-- machine involved in the modification
WITH cte_1 AS (INSERT INTO event_responses_no_pkey VALUES (1, 1, 'yes') RETURNING *),
	 cte_2 AS (SELECT count(*) FROM event_responses_no_pkey)
SELECT bool_or(coordinated_transaction_should_use_2PC()) FROM cte_1, cte_2;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- single shard local modification followed by remote multi-shard
-- modification uses 2PC as multiple nodes are involved in modifications
WITH cte_1 AS (INSERT INTO event_responses_no_pkey VALUES (2, 2, 'yes') RETURNING *),
	 cte_2 AS (UPDATE event_responses_no_pkey SET user_id = 1000 RETURNING *)
SELECT bool_or(coordinated_transaction_should_use_2PC()) FROM cte_1, cte_2;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- a local SELECT followed by a remote multi-shard UPDATE requires to
-- use actual 2PC as multiple nodes are involved in modifications
BEGIN;
	SELECT count(*) FROM event_responses_no_pkey WHERE event_id = 2;
	UPDATE event_responses_no_pkey SET user_id = 1;
COMMIT;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- a local SELECT followed by a remote single-shard UPDATE does not require to
-- use actual 2PC. This is because a single node is involved in modification
BEGIN;
	SELECT count(*) FROM event_responses_no_pkey WHERE event_id = 2;
	UPDATE event_responses_no_pkey SET user_id = 1 WHERE event_id = 1;
COMMIT;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- a remote single-shard UPDATE followed by a local single shard SELECT
-- does not require to use actual 2PC. This is because a single node
-- is involved in modification
BEGIN;
	UPDATE event_responses_no_pkey SET user_id = 1 WHERE event_id = 1;
	SELECT count(*) FROM event_responses_no_pkey WHERE event_id = 2;
COMMIT;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

\c - - - :master_port
SET search_path TO local_shard_execution;

-- verify the local_hostname guc is used for local executions that should connect to the
-- local host
ALTER SYSTEM SET citus.local_hostname TO 'foobar';
SELECT pg_reload_conf();
SELECT pg_sleep(0.1); -- wait to make sure the config has changed before running the GUC
SET citus.enable_local_execution TO false; -- force a connection to the dummy placements

-- run queries that use dummy placements for local execution
SELECT * FROM event_responses WHERE FALSE;
WITH cte_1 AS (SELECT * FROM event_responses LIMIT 1) SELECT count(*) FROM cte_1;

ALTER SYSTEM RESET citus.local_hostname;
SELECT pg_reload_conf();
SELECT pg_sleep(.1); -- wait to make sure the config has changed before running the GUC

SET client_min_messages TO ERROR;
SET search_path TO public;
DROP SCHEMA local_shard_execution CASCADE;

