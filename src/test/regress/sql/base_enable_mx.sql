--
-- Setup MX data syncing
--
SELECT start_metadata_sync_to_node(:'worker_1_host', :worker_1_port);
SELECT start_metadata_sync_to_node(:'worker_2_host', :worker_2_port);

