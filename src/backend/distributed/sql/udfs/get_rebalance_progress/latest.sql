DROP FUNCTION pg_catalog.get_rebalance_progress();

CREATE OR REPLACE FUNCTION pg_catalog.get_rebalance_progress()
  RETURNS TABLE(sessionid integer,
                table_name regclass,
                shardid bigint,
                shard_size bigint,
                sourcename text,
                sourceport int,
                targetname text,
                targetport int,
                progress bigint,
                source_shard_size bigint,
                target_shard_size bigint,
                operation_type text,
                source_lsn pg_lsn,
                target_lsn pg_lsn,
                status text
            )
  AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT;
COMMENT ON FUNCTION pg_catalog.get_rebalance_progress()
    IS 'provides progress information about the ongoing rebalance operations';
