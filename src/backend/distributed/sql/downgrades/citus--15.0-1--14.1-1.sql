-- citus--15.0-1--14.1-1
-- downgrade version to 14.1-1

-- cluster changes block UDFs
DROP FUNCTION IF EXISTS pg_catalog.citus_cluster_changes_block(int);
DROP FUNCTION IF EXISTS pg_catalog.citus_cluster_changes_unblock();
DROP FUNCTION IF EXISTS pg_catalog.citus_cluster_changes_block_status();
