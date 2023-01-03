--
-- SHARD_MOVE_DEFERRED_DELETE
--

SET citus.next_shard_id TO 20000000;

SET citus.shard_count TO 6;
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA shard_move_deferred_delete;
SET search_path TO shard_move_deferred_delete;

CREATE TABLE t1 ( id int PRIMARY KEY);
SELECT create_distributed_table('t1', 'id');

-- by counting how ofter we see the specific shard on all workers we can verify is the shard is there
SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000000';
$cmd$);

SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000001';
$cmd$);

-- move shard
SELECT master_move_shard_placement(20000000, 'localhost', :worker_1_port, 'localhost', :worker_2_port);

-- we expect the shard to be on both workers now
SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000000';
$cmd$);

-- Make sure this cannot be run in a transaction
BEGIN;
CALL citus_cleanup_orphaned_resources();
COMMIT;

-- citus_cleanup_orphaned_shards is deprecated
CALL citus_cleanup_orphaned_shards();

-- execute delayed removal
CALL citus_cleanup_orphaned_resources();

-- we expect the shard to be on only the second worker
SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000000';
$cmd$);

SELECT master_move_shard_placement(20000000, 'localhost', :worker_2_port, 'localhost', :worker_1_port);

-- we expect the shard to be on both workers now
SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000000';
$cmd$);

-- enable auto delete
ALTER SYSTEM SET citus.defer_shard_delete_interval TO 10;
SELECT pg_reload_conf();

-- Sleep 1 second to give Valgrind enough time to clear transactions
SELECT pg_sleep(1);
SELECT public.wait_for_resource_cleanup();

-- we expect the shard to be on only the first worker
SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000000';
$cmd$);

-- reset test suite
ALTER SYSTEM SET citus.defer_shard_delete_interval TO -1;
SELECT pg_reload_conf();

-- move shard
SELECT master_move_shard_placement(20000000, 'localhost', :worker_1_port, 'localhost', :worker_2_port);

-- we expect shard 0 to be on both workers now
SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000000';
$cmd$);

-- master_move_shard_placement automatically cleans up orphaned shards if
-- needed.
SELECT master_move_shard_placement(20000000, 'localhost', :worker_2_port, 'localhost', :worker_1_port);

-- when a move tx is aborted, there remains a shard on the target node
BEGIN;
SELECT master_move_shard_placement(20000000, 'localhost', :worker_1_port, 'localhost', :worker_2_port);
ROLLBACK;

-- see the cleanup record for the shard on the target node
-- https://github.com/citusdata/citus/issues/6580
select object_name, object_type from pg_dist_cleanup where object_type = 1;

-- see the shard on both workers
SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000000';
$cmd$);

-- clean it up
SELECT public.wait_for_resource_cleanup();

-- verify the shard is cleaned up
SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000000';
$cmd$);

-- override the function for testing purpose
-- since it is extension owned function, propagate it to workers manually
create or replace function pg_catalog.citus_local_disk_space_stats(OUT available_disk_size bigint, OUT total_disk_size bigint)
as $BODY$
begin
    select 20 into available_disk_size;
    select 8500 into total_disk_size;
end
$BODY$ language plpgsql;

\c - - - :worker_1_port
SET citus.enable_metadata_sync TO OFF;
create or replace function pg_catalog.citus_local_disk_space_stats(OUT available_disk_size bigint, OUT total_disk_size bigint)
as $BODY$
begin
    select 20 into available_disk_size;
    select 8500 into total_disk_size;
end
$BODY$ language plpgsql;

\c - - - :worker_2_port
SET citus.enable_metadata_sync TO OFF;
create or replace function pg_catalog.citus_local_disk_space_stats(OUT available_disk_size bigint, OUT total_disk_size bigint)
as $BODY$
begin
    select 20 into available_disk_size;
    select 8500 into total_disk_size;
end
$BODY$ language plpgsql;

\c - - - :master_port

SET search_path TO shard_move_deferred_delete;

SELECT citus_shard_cost_by_disk_size(20000001);

-- When there's not enough space the move should fail
SELECT master_move_shard_placement(20000001, 'localhost', :worker_2_port, 'localhost', :worker_1_port);


BEGIN;
-- when we disable the setting, the move should not give "not enough space" error
set citus.check_available_space_before_move to false;
SELECT master_move_shard_placement(20000001, 'localhost', :worker_2_port, 'localhost', :worker_1_port);
ROLLBACK;

SELECT run_command_on_workers($cmd$
    SELECT count(*) FROM pg_class WHERE relname = 't1_20000000';
$cmd$);

-- override the function for testing purpose
-- since it is extension owned function, propagate it to workers manually
create or replace function pg_catalog.citus_local_disk_space_stats(OUT available_disk_size bigint, OUT total_disk_size bigint)
as $BODY$
begin
    select 8300 into available_disk_size;
    select 8500 into total_disk_size;
end
$BODY$ language plpgsql;

\c - - - :worker_1_port
SET citus.enable_metadata_sync TO OFF;
create or replace function pg_catalog.citus_local_disk_space_stats(OUT available_disk_size bigint, OUT total_disk_size bigint)
as $BODY$
begin
    select 8300 into available_disk_size;
    select 8500 into total_disk_size;
end
$BODY$ language plpgsql;

\c - - - :worker_2_port
SET citus.enable_metadata_sync TO OFF;
create or replace function pg_catalog.citus_local_disk_space_stats(OUT available_disk_size bigint, OUT total_disk_size bigint)
as $BODY$
begin
    select 8300 into available_disk_size;
    select 8500 into total_disk_size;
end
$BODY$ language plpgsql;

\c - - - :master_port

SET search_path TO shard_move_deferred_delete;

-- When there would not be enough free space left after the move, the move should fail
SELECT master_move_shard_placement(20000001, 'localhost', :worker_2_port, 'localhost', :worker_1_port);

-- Restore the original function on workers
\c - - - :worker_1_port
SET citus.enable_metadata_sync TO OFF;

CREATE OR REPLACE FUNCTION pg_catalog.citus_local_disk_space_stats(
OUT available_disk_size bigint,
OUT total_disk_size bigint)
RETURNS record
LANGUAGE C STRICT
AS 'citus', $$citus_local_disk_space_stats$$;
COMMENT ON FUNCTION pg_catalog.citus_local_disk_space_stats()
IS 'returns statistics on available disk space on the local node';

\c - - - :worker_2_port
SET citus.enable_metadata_sync TO OFF;

CREATE OR REPLACE FUNCTION pg_catalog.citus_local_disk_space_stats(
OUT available_disk_size bigint,
OUT total_disk_size bigint)
RETURNS record
LANGUAGE C STRICT
AS 'citus', $$citus_local_disk_space_stats$$;
COMMENT ON FUNCTION pg_catalog.citus_local_disk_space_stats()
IS 'returns statistics on available disk space on the local node';

\c - - - :master_port
DROP SCHEMA shard_move_deferred_delete CASCADE;
