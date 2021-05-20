-- get_rebalance_progress returns the list of shard placement move operations along with
-- their progressions for ongoing rebalance operations.
--
CREATE OR REPLACE FUNCTION pg_catalog.get_rebalance_progress()
  RETURNS TABLE(sessionid integer,
                table_name regclass,
                shardid bigint,
                shard_size bigint,
                sourcename text,
                sourceport int,
                targetname text,
                targetport int,
                progress bigint)
  AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT;
COMMENT ON FUNCTION pg_catalog.get_rebalance_progress()
    IS 'provides progress information about the ongoing rebalance operations';
