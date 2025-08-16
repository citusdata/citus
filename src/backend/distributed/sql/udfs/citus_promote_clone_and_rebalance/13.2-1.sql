CREATE OR REPLACE FUNCTION pg_catalog.citus_promote_clone_and_rebalance(
    clone_nodeid integer,
    rebalance_strategy name DEFAULT NULL,
    catchup_timeout_seconds integer DEFAULT 300
)
RETURNS VOID
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION pg_catalog.citus_promote_clone_and_rebalance(integer, name, integer) IS
'Promotes a registered clone node to a primary, performs necessary metadata updates, and rebalances a portion of shards from its original primary to the newly promoted node. The catchUpTimeoutSeconds parameter controls how long to wait for the clone to catch up with the primary (default: 300 seconds).';

REVOKE ALL ON FUNCTION pg_catalog.citus_promote_clone_and_rebalance(integer, name, integer) FROM PUBLIC;
