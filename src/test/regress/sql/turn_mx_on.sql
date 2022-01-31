ALTER SYSTEM SET citus.enable_metadata_sync_by_default TO ON;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

SET client_min_messages TO ERROR;
SELECT start_metadata_sync_to_node(nodename, nodeport) FROM pg_dist_node WHERE isactive = 't' and noderole = 'primary';
