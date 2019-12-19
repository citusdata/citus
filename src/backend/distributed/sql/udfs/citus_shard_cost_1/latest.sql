CREATE OR REPLACE FUNCTION pg_catalog.citus_shard_cost_1(bigint)
    RETURNS float4 AS $$ SELECT 1.0::float4 $$ LANGUAGE sql;
COMMENT ON FUNCTION pg_catalog.citus_shard_cost_1(bigint)
  IS 'a shard cost function for use by the rebalance algorithm that always returns 1';
