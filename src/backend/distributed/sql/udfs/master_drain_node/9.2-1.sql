DROP FUNCTION pg_catalog.master_drain_node;
CREATE FUNCTION pg_catalog.master_drain_node(
    nodename text,
    nodeport integer,
    shard_transfer_mode citus.shard_transfer_mode default 'auto',
    rebalance_strategy name default NULL
  )
  RETURNS VOID
  LANGUAGE C
  AS 'MODULE_PATHNAME', $$master_drain_node$$;
COMMENT ON FUNCTION pg_catalog.master_drain_node(text,int,citus.shard_transfer_mode,name)
  IS 'mark a node to be drained of data and actually drain it as well';

REVOKE ALL ON FUNCTION pg_catalog.master_drain_node(text,int,citus.shard_transfer_mode,name) FROM PUBLIC;
