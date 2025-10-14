-- Citus upgrades are finished by calling a procedure

-- this is a transactional procedure, so rollback should be fine
BEGIN;
	CALL citus_finish_citus_upgrade();
ROLLBACK;

-- do the actual job
CALL citus_finish_citus_upgrade();

-- show that the upgrade is successfull

SELECT metadata->>'last_upgrade_version' = extversion
FROM pg_dist_node_metadata, pg_extension WHERE extname = 'citus';

-- idempotent, should be called multiple times
-- still, do not NOTICE the version as it changes per release
SET client_min_messages TO WARNING;
CALL citus_finish_citus_upgrade();

-- we should be able to sync metadata in nontransactional way as well
SET citus.metadata_sync_mode TO 'nontransactional';
SELECT start_metadata_sync_to_all_nodes();
RESET citus.metadata_sync_mode;
