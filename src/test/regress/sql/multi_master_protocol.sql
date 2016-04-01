--
-- MULTI_MASTER_PROTOCOL
--

-- Tests that check the metadata returned by the master node.

SELECT part_storage_type, part_key, part_replica_count, part_max_size,
	   part_placement_policy FROM get_table_metadata('lineitem');

SELECT * FROM get_table_ddl_events('lineitem');

SELECT * FROM get_new_shardid();

SELECT * FROM get_local_first_candidate_nodes();

SELECT * FROM get_round_robin_candidate_nodes(1);

SELECT * FROM get_round_robin_candidate_nodes(2);

SELECT * FROM get_active_worker_nodes();
