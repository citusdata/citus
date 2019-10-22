CREATE FUNCTION pg_catalog.master_drain_node(
    nodename text,
    nodeport integer,
    threshold float4 default 0,
    max_shard_moves int default 1000000,
    excluded_shard_list bigint[] default '{}',
    shard_transfer_mode citus.shard_transfer_mode default 'auto')
  RETURNS VOID
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_drain_node$$;
COMMENT ON FUNCTION pg_catalog.master_drain_node(text,int,float4,int,bigint[],citus.shard_transfer_mode)
  IS 'mark a node to be drained of data and actually drain it as well';

REVOKE ALL ON FUNCTION pg_catalog.master_drain_node(text,int,float4,int,bigint[],citus.shard_transfer_mode) FROM PUBLIC;
