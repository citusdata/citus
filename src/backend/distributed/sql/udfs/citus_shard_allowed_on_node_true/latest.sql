CREATE OR REPLACE FUNCTION pg_catalog.citus_shard_allowed_on_node_true(bigint, int)
    RETURNS boolean AS $$ SELECT true $$ LANGUAGE sql;
COMMENT ON FUNCTION pg_catalog.citus_shard_allowed_on_node_true(bigint,int)
  IS 'a shard_allowed_on_node_function for use by the rebalance algorithm that always returns true';

