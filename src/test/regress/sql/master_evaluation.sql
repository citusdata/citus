-- This test relies on metadata being synced
-- that's why is should be executed on MX schedule
CREATE SCHEMA master_evaluation;
SET search_path TO master_evaluation;

-- create a volatile function that returns the local node id
CREATE OR REPLACE FUNCTION get_local_node_id_volatile()
RETURNS INT AS $$
DECLARE localGroupId int;
BEGIN
	SELECT groupid INTO localGroupId FROM pg_dist_local_group;
  RETURN localGroupId;
END; $$ language plpgsql VOLATILE;
SELECT create_distributed_function('get_local_node_id_volatile()');

CREATE TABLE master_evaluation_table (key int, value int);
SELECT create_distributed_table('master_evaluation_table', 'key');

-- show that local id is 0, we'll use this information
SELECT get_local_node_id_volatile();

-- load data such that we have 1 row per node
INSERT INTO master_evaluation_table SELECT i, 0 FROM generate_series(0,100)i;

-- we expect that the function is evaluated on the worker node, so we should get a row
SELECT get_local_node_id_volatile() > 0 FROM master_evaluation_table WHERE key = 1;

-- make sure that it is also true for  fast-path router queries with paramaters
PREPARE p1(int) AS SELECT get_local_node_id_volatile() > 0 FROM master_evaluation_table WHERE key  = $1;

execute p1(1);
execute p1(2);
execute p1(3);
execute p1(4);
execute p1(5);
execute p1(6);
execute p1(7);
execute p1(8);

-- for multi-shard queries, we  still expect the evaluation to happen on the workers
SELECT count(*), max(get_local_node_id_volatile()) != 0,  min(get_local_node_id_volatile()) != 0 FROM master_evaluation_table;

-- when executed locally, we expect to get the result from the coordinator
SELECT (SELECT count(*) FROM master_evaluation_table), get_local_node_id_volatile() = 0;

-- make sure that we get the results from the workers when the query is sent to workers
SET citus.task_assignment_policy TO "round-robin";
SELECT (SELECT count(*) FROM master_evaluation_table), get_local_node_id_volatile() > 0;

RESET citus.task_assignment_policy;

-- for multi-shard SELECTs, we don't try to evaluate on the coordinator
SELECT min(get_local_node_id_volatile())  > 0 FROM master_evaluation_table;
SELECT count(*) FROM master_evaluation_table WHERE value >= get_local_node_id_volatile();

SET client_min_messages TO ERROR;
DROP SCHEMA master_evaluation CASCADE;
