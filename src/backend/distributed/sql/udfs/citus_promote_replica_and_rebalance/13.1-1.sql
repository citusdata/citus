CREATE OR REPLACE FUNCTION pg_catalog.citus_promote_replica_and_rebalance(
    replica_nodeid integer,
    rebalance_strategy name DEFAULT NULL
)
RETURNS VOID
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION pg_catalog.citus_promote_replica_and_rebalance(integer, name) IS
'Promotes a registered replica node to a primary, performs necessary metadata updates, and rebalances a portion of shards from its original primary to the newly promoted node.';

REVOKE ALL ON FUNCTION pg_catalog.citus_promote_replica_and_rebalance(integer, name) FROM PUBLIC;
