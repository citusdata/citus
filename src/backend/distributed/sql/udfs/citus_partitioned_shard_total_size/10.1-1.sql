CREATE OR REPLACE FUNCTION pg_catalog.citus_partitioned_shard_total_size(text)
    RETURNS bigint
    AS 'MODULE_PATHNAME', $$citus_partitioned_shard_total_size$$
    LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION pg_catalog.citus_partitioned_shard_total_size(text)
    IS 'Finds and returns the total size of a partitioned shard';
