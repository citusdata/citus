-- Test Secneario
-- 1) Setup shared memory segment by calling worker_split_shard_replication_setup.
-- 2) Redo step 1 as the earlier memory. Redoing will trigger warning as earlier memory isn't released.
-- 3) Execute worker_split_shard_release_dsm to release the dynamic shared memory segment
-- 4) Redo step 1 and expect no warning as the earlier memory is cleanedup.

SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

\c - - - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;
SET client_min_messages TO ERROR;
SELECT count(*) FROM pg_catalog.worker_split_shard_replication_setup(ARRAY[
    ROW(1, 'id', 2, '-2147483648', '-1', :worker_1_node)::pg_catalog.split_shard_info,
    ROW(1, 'id', 3, '0', '2147483647', :worker_1_node)::pg_catalog.split_shard_info
    ]);

SET client_min_messages TO WARNING;
SELECT count(*) FROM pg_catalog.worker_split_shard_replication_setup(ARRAY[
    ROW(1, 'id', 2, '-2147483648', '-1', :worker_1_node)::pg_catalog.split_shard_info,
    ROW(1, 'id', 3, '0', '2147483647', :worker_1_node)::pg_catalog.split_shard_info
    ]);

SELECT pg_catalog.worker_split_shard_release_dsm();
SELECT count(*) FROM pg_catalog.worker_split_shard_replication_setup(ARRAY[
    ROW(1, 'id', 2, '-2147483648', '-1', :worker_1_node)::pg_catalog.split_shard_info,
    ROW(1, 'id', 3, '0', '2147483647', :worker_1_node)::pg_catalog.split_shard_info
    ]);
