CREATE SCHEMA mdsb;
SET search_path TO mdsb;
SET client_min_messages TO WARNING;
SET citus.shard_count TO 1;              -- single-shard tables keep per-table cost low
SET citus.shard_replication_factor TO 1;

-- create distributed tables mdsb.t_<lo> .. mdsb.t_<hi>
CREATE OR REPLACE FUNCTION public.create_dist_tables(lo int, hi int)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
  FOR i IN lo..hi LOOP
    EXECUTE format('CREATE TABLE mdsb.t_%s (id int PRIMARY KEY, v int)', i);
    PERFORM create_distributed_table(format('mdsb.t_%s', i), 'id');
  END LOOP;
END;
$fn$;

-- TRUE iff every node (coordinator + workers) reports the same Citus metadata,
CREATE OR REPLACE FUNCTION public.metadata_in_sync()
RETURNS boolean LANGUAGE sql AS $fn$
  SELECT
    (SELECT count(DISTINCT result) = 1
       FROM run_command_on_all_nodes('SELECT count(*) FROM pg_dist_partition'))
    AND
    (SELECT count(DISTINCT result) = 1
       FROM run_command_on_all_nodes('SELECT count(*) FROM pg_dist_shard'))
    AND
    (SELECT count(DISTINCT result) = 1
       FROM run_command_on_all_nodes('SELECT count(*) FROM pg_dist_object'));
$fn$;

-- ==========================================================================
-- Scenario A: batch size 1  (batching disabled; non-transactional never pipelines)
-- ==========================================================================
SELECT public.create_dist_tables(1, 5);
SET citus.metadata_sync_batch_size TO 1;

SET citus.metadata_sync_mode TO 'transactional';
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT public.metadata_in_sync() AS a_size1_transactional;

SET citus.metadata_sync_mode TO 'nontransactional';
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT public.metadata_in_sync() AS a_size1_nontransactional;

DROP SCHEMA mdsb CASCADE;
CREATE SCHEMA mdsb;

-- ==========================================================================
-- Scenario B: batch size 10  -> straddle at 9 (partial), 10 (exact), 45 (multi)
-- ==========================================================================
SET citus.metadata_sync_batch_size TO 10;

-- 9 objects: below the batch size (single partial batch)
SELECT public.create_dist_tables(1, 9);
SET citus.metadata_sync_mode TO 'transactional';
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT public.metadata_in_sync() AS b_size10_lt_transactional;
SET citus.metadata_sync_mode TO 'nontransactional';
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT public.metadata_in_sync() AS b_size10_lt_nontransactional;

-- 10 objects: exactly one full batch (no trailing partial)
SELECT public.create_dist_tables(10, 10);
SET citus.metadata_sync_mode TO 'transactional';
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT public.metadata_in_sync() AS b_size10_eq_transactional;
SET citus.metadata_sync_mode TO 'nontransactional';
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT public.metadata_in_sync() AS b_size10_eq_nontransactional;

-- 45 objects: several full batches + a trailing partial batch
SELECT public.create_dist_tables(11, 45);
SET citus.metadata_sync_mode TO 'transactional';
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT public.metadata_in_sync() AS b_size10_gt_transactional;
SET citus.metadata_sync_mode TO 'nontransactional';
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT public.metadata_in_sync() AS b_size10_gt_nontransactional;

DROP SCHEMA mdsb CASCADE;
CREATE SCHEMA mdsb;

-- ==========================================================================
-- Scenario D: multiple workers + mixed FK / FK-less tables, small batch.
-- Activating *all* nodes non-transactionally with batching > 1 drives the
-- multi-connection pipeline path (one libpq pipeline per worker).  This is a
-- correctness oracle: every worker must end up with identical, complete
-- metadata, so a per-connection drain bug that dropped or duplicated a command
-- at a batch boundary would leave a worker behind (or error).
--
-- Interleaving FK-less tables (which emit an empty inter-table relationship
-- command list) with co-located FK tables (which emit a real one) also exercises
-- the batch flush across empty and non-empty relationship batches.
-- ==========================================================================
DROP SCHEMA mdsb CASCADE;
CREATE SCHEMA mdsb;
SET citus.metadata_sync_batch_size TO 3;
SET citus.metadata_sync_mode TO 'nontransactional';

-- FK-less tables (emit empty inter-table relationship command lists)
SELECT public.create_dist_tables(1, 6);

-- co-located FK pairs: each child emits a real inter-table relationship command
CREATE TABLE mdsb.p_1 (id int PRIMARY KEY);
SELECT create_distributed_table('mdsb.p_1', 'id');
CREATE TABLE mdsb.c_1 (id int PRIMARY KEY REFERENCES mdsb.p_1(id));
SELECT create_distributed_table('mdsb.c_1', 'id', colocate_with => 'mdsb.p_1');
CREATE TABLE mdsb.p_2 (id int PRIMARY KEY);
SELECT create_distributed_table('mdsb.p_2', 'id');
CREATE TABLE mdsb.c_2 (id int PRIMARY KEY REFERENCES mdsb.p_2(id));
SELECT create_distributed_table('mdsb.c_2', 'id', colocate_with => 'mdsb.p_2');

SELECT 1 FROM start_metadata_sync_to_all_nodes();
SELECT public.metadata_in_sync() AS d_multiworker_fk_in_sync;

-- Every node must carry the same number of distributed FK constraints; a
-- relationship command dropped on one worker's pipeline would make this differ.
-- conrelid is scoped to shell tables (pg_dist_partition) so shard-table FKs,
-- which only exist on workers, do not skew the coordinator's count.
SELECT count(DISTINCT result) = 1 AS d_multiworker_fk_counts_agree
  FROM run_command_on_all_nodes(
    $cmd$ SELECT count(*) FROM pg_constraint
           WHERE contype = 'f'
             AND conrelid IN (SELECT logicalrelid FROM pg_dist_partition) $cmd$);

-- cleanup
RESET citus.metadata_sync_mode;
RESET citus.metadata_sync_batch_size;
DROP SCHEMA mdsb CASCADE;
DROP FUNCTION public.create_dist_tables(int, int);
DROP FUNCTION public.metadata_in_sync();
