--
-- MULTI_MASTER_PROTOCOL
--
-- Tests that check the metadata returned by the master node.


SET citus.next_shard_id TO 740000;


SELECT * FROM master_get_table_ddl_events('lineitem') order by 1;

SELECT * FROM master_get_new_shardid();

SELECT * FROM master_get_active_worker_nodes();
