CREATE OR REPLACE FUNCTION pg_catalog.citus_auto_shard_split_start(
    )
    RETURNS VOID

    AS 'MODULE_PATHNAME'

    LANGUAGE C VOLATILE;

COMMENT ON FUNCTION pg_catalog.citus_auto_shard_split_start()

    IS 'automatically split the necessary shards in the cluster in the background';

GRANT EXECUTE ON FUNCTION pg_catalog.citus_auto_shard_split_start() TO PUBLIC;