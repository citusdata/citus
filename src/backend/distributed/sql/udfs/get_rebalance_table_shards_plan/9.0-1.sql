-- get_rebalance_table_shards_plan shows the actual events that will be performed
-- if a rebalance operation will be performed with the same arguments, which allows users
-- to understand the impact of the change overall availability of the application and
-- network trafic.
--
CREATE OR REPLACE FUNCTION pg_catalog.get_rebalance_table_shards_plan(
        relation regclass,
        threshold float4 default 0,
        max_shard_moves int default 1000000,
        excluded_shard_list bigint[] default '{}')
    RETURNS TABLE (table_name regclass,
                   shardid bigint,
                   shard_size bigint,
                   sourcename text,
                   sourceport int,
                   targetname text,
                   targetport int)
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION pg_catalog.get_rebalance_table_shards_plan(regclass, float4, int, bigint[])
    IS 'returns the list of shard placement moves to be done on a rebalance operation';
