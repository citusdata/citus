SET search_path TO upgrade_distributed_function_before, public;

-- make sure that the metadata synced
SELECT bool_and(metadatasynced) FROM pg_dist_node WHERE isactive AND noderole = 'primary';
SET client_min_messages TO DEBUG1;

-- these are simple select functions, so doesn't have any
-- side effects, safe to be called without BEGIN;..;ROLLBACK;
SELECT count_values(11);
SELECT count_values(12);

