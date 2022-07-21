CREATE SCHEMA citus_locks;
SET search_path TO citus_locks;
SET citus.next_shard_id TO 1000;

CREATE TABLE dist_locked_table(id int, data text);
SELECT create_distributed_table('dist_locked_table', 'id');

BEGIN;
-- Alter a distributed table so that we get some locks
ALTER TABLE dist_locked_table ADD COLUMN new_data_column text;

-- list the locks on relations for current distributed transaction
SELECT relation_name, citus_nodename_for_nodeid(nodeid), citus_nodeport_for_nodeid(nodeid), mode, granted
FROM citus_locks
WHERE global_pid = citus_backend_gpid() AND locktype = 'relation' AND relation_name LIKE '%dist_locked_table%'
ORDER BY 1, 2, 3, 4;

ROLLBACK;

DROP SCHEMA citus_locks CASCADE;
