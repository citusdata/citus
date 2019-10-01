CREATE OR REPLACE FUNCTION pg_catalog.citus_node_capacity_1(int)
    RETURNS float4 AS $$ SELECT 1.0::float4 $$ LANGUAGE sql;
COMMENT ON FUNCTION pg_catalog.citus_node_capacity_1(int)
  IS 'a node capacity function for use by the rebalance algorithm that always returns 1';
