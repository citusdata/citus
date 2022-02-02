ALTER SYSTEM SET citus.enable_metadata_sync TO OFF;
SELECT pg_reload_conf();

SET client_min_messages TO ERROR;
SELECT stop_metadata_sync_to_node(nodename, nodeport) FROM pg_dist_node WHERE isactive = 't' and noderole = 'primary';
