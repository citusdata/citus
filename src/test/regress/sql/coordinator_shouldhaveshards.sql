-- Test queries on a distributed table with shards on the coordinator

CREATE SCHEMA coordinator_shouldhaveshards;
SET search_path TO coordinator_shouldhaveshards;
SET citus.next_shard_id TO 1503000;

-- idempotently add node to allow this test to run without add_coordinator
SET client_min_messages TO WARNING;
SELECT 1 FROM master_add_node('localhost', :master_port, groupid => 0);
RESET client_min_messages;

SELECT 1 FROM master_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

SET citus.shard_replication_factor TO 1;

CREATE TABLE test (x int, y int);
SELECT create_distributed_table('test','x', colocate_with := 'none');

SELECT count(*) FROM pg_dist_shard JOIN pg_dist_placement USING (shardid)
WHERE logicalrelid = 'test'::regclass AND groupid = 0;

--- enable logging to see which tasks are executed locally
SET client_min_messages TO LOG;
SET citus.log_local_commands TO ON;


-- INSERT..SELECT with COPY under the covers
INSERT INTO test SELECT s,s FROM generate_series(2,100) s;

-- router queries execute locally
INSERT INTO test VALUES (1, 1);
SELECT y FROM test WHERE x = 1;

-- multi-shard queries connect to localhost
SELECT count(*) FROM test;
WITH a AS (SELECT * FROM test) SELECT count(*) FROM test;

-- multi-shard queries in transaction blocks execute locally
BEGIN;
SELECT y FROM test WHERE x = 1;
SELECT count(*) FROM test;
END;

BEGIN;
SELECT y FROM test WHERE x = 1;
SELECT count(*) FROM test;
END;

-- DDL connects to locahost
ALTER TABLE test ADD COLUMN z int;

-- DDL after local execution
BEGIN;
SELECT y FROM test WHERE x = 1;
ALTER TABLE test DROP COLUMN z;
ROLLBACK;

BEGIN;
ALTER TABLE test DROP COLUMN z;
SELECT y FROM test WHERE x = 1;
END;

DELETE FROM test;
DROP TABLE test;

DROP SCHEMA coordinator_shouldhaveshards CASCADE;

SELECT 1 FROM master_set_node_property('localhost', :master_port, 'shouldhaveshards', false);
