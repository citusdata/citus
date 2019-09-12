--
-- Setup MX data syncing
--
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT start_metadata_sync_to_node('localhost', :worker_2_port);

