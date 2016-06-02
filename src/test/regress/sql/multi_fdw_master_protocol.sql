--
-- MULTI_FDW_MASTER_PROTOCOL
--

-- Tests that check the metadata returned by the master node.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 600000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 600000;


SELECT part_storage_type, part_key, part_replica_count, part_max_size,
	   part_placement_policy FROM master_get_table_metadata('lineitem');

SELECT * FROM master_get_table_ddl_events('lineitem');

SELECT * FROM master_get_new_shardid();

SELECT node_name FROM master_get_local_first_candidate_nodes();
