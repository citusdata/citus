CREATE SCHEMA "citus_split_failure_test_schema";

SET search_path TO "citus_split_failure_test_schema";
SET citus.shard_count TO 1;
SET citus.next_shard_id TO 890000;
SET citus.shard_replication_factor TO 1;

-- BEGIN: Create table to split
CREATE TABLE sensors(
    measureid               integer,
    eventdatetime           date);

CREATE TABLE sensors_colocated(
    measureid               integer,
    eventdatetime2          date);

SELECT create_distributed_table('sensors', 'measureid');
SELECT create_distributed_table('sensors_colocated', 'measureid', colocate_with:='sensors');
-- END: Create table to split

-- BEGIN : Switch to worker and create split shards already so workflow fails.
\c - - - :worker_1_port
SET search_path TO "citus_split_failure_test_schema";

-- Don't create sensors_8981001, workflow will create and clean it.
-- Create rest of the shards so that the workflow fails, but will not clean them.

CREATE TABLE sensors_8981002(
    measureid               integer,
    eventdatetime           date);

CREATE TABLE sensors_colocated_8981003(
    measureid               integer,
    eventdatetime           date);

CREATE TABLE sensors_colocated_8981004(
    measureid               integer,
    eventdatetime           date);

-- A random table which should not be deleted.
CREATE TABLE sensors_nodelete(
    measureid               integer,
    eventdatetime           date);
-- List tables in worker.
SET search_path TO "citus_split_failure_test_schema";
SET citus.show_shards_for_app_name_prefixes = '*';
SELECT tbl.relname
    FROM pg_catalog.pg_class tbl
    WHERE tbl.relname like 'sensors%'
    ORDER BY 1;
-- END : Switch to worker and create split shards already so workflow fails.

-- BEGIN : Set node id variables
\c - postgres - :master_port
SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
-- END   : Set node id variables

-- BEGIN : Split Shard, which is expected to fail.
SET citus.next_shard_id TO 8981001;
SELECT pg_catalog.citus_split_shard_by_split_points(
    890000,
    ARRAY['-1073741824'],
    ARRAY[:worker_1_node, :worker_1_node],
    'block_writes');
-- BEGIN : Split Shard, which is expected to fail.

SELECT public.wait_for_resource_cleanup();

-- BEGIN : Ensure tables were cleaned from worker
\c - - - :worker_1_port
SET search_path TO "citus_split_failure_test_schema";
SET citus.show_shards_for_app_name_prefixes = '*';
SELECT tbl.relname
    FROM pg_catalog.pg_class tbl
    WHERE tbl.relname like 'sensors%'
    ORDER BY 1;
-- END : Ensure tables were cleaned from worker

--BEGIN : Cleanup
\c - postgres - :master_port
DROP SCHEMA "citus_split_failure_test_schema" CASCADE;
--END : Cleanup
