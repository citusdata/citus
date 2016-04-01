--
-- MULTI_FDW_MASTER_PROTOCOL
--

-- Tests that check the metadata returned by the master node.

SELECT part_storage_type, part_key, part_replica_count, part_max_size,
	   part_placement_policy FROM get_table_metadata('lineitem');

SELECT * FROM get_table_ddl_events('lineitem');

SELECT * FROM get_new_shardid();

SELECT node_name FROM get_local_first_candidate_nodes();
