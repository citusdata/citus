--
-- Test that citus_promote_clone_and_rebalance() errors out, rather than
-- registering the clone, if the transaction that gives the promoted clone its
-- own identity fails at COMMIT. A deferred trigger that the clone copies from
-- its primary makes that COMMIT fail after each of its commands succeeded.
--
SET client_min_messages TO WARNING;
SELECT 1 FROM master_add_node('localhost', :worker_7_port);
SELECT citus_add_clone_node('localhost', :follower_worker_7_port, 'localhost', :worker_7_port) AS clone_node_id \gset
RESET client_min_messages;
SELECT groupid AS clone_group_id FROM pg_dist_node WHERE nodeid = :clone_node_id \gset

\c - - - :worker_7_port
SET citus.enable_ddl_propagation TO off;
CREATE FUNCTION public.fail_identity_commit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'failing the commit of the clone''s identity';
END;
$$;
CREATE CONSTRAINT TRIGGER fail_identity_commit
    AFTER UPDATE ON pg_catalog.pg_dist_node_metadata
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION public.fail_identity_commit();
SELECT groupid AS primary_group_id FROM pg_dist_local_group \gset
SELECT metadata->>'server_id' AS primary_server_id FROM pg_dist_node_metadata \gset
\c - - - :master_port

SET client_min_messages TO WARNING;
SELECT citus_promote_clone_and_rebalance(:clone_node_id);
RESET client_min_messages;

-- the clone is not registered
SELECT isactive, nodeisclone FROM pg_dist_node WHERE nodeid = :clone_node_id;

-- it is promoted, but it kept the identity of the primary
\c - - - :follower_worker_7_port
SELECT pg_is_in_recovery() AS clone_in_recovery,
       groupid = :primary_group_id AS has_primary_group_id,
       metadata->>'server_id' = :'primary_server_id' AS has_primary_server_id
FROM pg_dist_local_group, pg_dist_node_metadata;

-- such a clone needs to be shut down; give it the identity that the promotion
-- would have given it instead, so that it does not act as the primary later
SET citus.enable_ddl_propagation TO off;
DROP TRIGGER fail_identity_commit ON pg_catalog.pg_dist_node_metadata;
DROP FUNCTION public.fail_identity_commit();
UPDATE pg_dist_local_group SET groupid = :clone_group_id;
UPDATE pg_dist_node_metadata
SET metadata = jsonb_set(metadata, '{server_id}', to_jsonb(citus_server_id()::text));

-- cleanup
\c - - - :worker_7_port
SET citus.enable_ddl_propagation TO off;
DROP TRIGGER fail_identity_commit ON pg_catalog.pg_dist_node_metadata;
DROP FUNCTION public.fail_identity_commit();
\c - - - :master_port
