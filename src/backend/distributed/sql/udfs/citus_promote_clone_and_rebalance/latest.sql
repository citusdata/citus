CREATE OR REPLACE FUNCTION pg_catalog.citus_promote_clone_and_rebalance(
    clone_nodeid integer,
    rebalance_strategy name DEFAULT NULL,
    catchup_timeout_seconds integer DEFAULT 300
)
RETURNS VOID
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION pg_catalog.citus_promote_clone_and_rebalance(integer, name, integer) IS
'Promotes a registered clone node to an additional primary, updates metadata, and rebalances a portion of shards from its original primary. catchup_timeout_seconds defaults to 300; zero disables the catch-up deadline and negative values are rejected before locking. The budget starts after acquiring shard write locks and includes connection establishment and WAL position queries, but excludes promotion and rebalancing. No replay probe starts after the budget expires; a successful in-flight probe is accepted. This is not a hard remote-I/O deadline. Cancellation and statement_timeout remain effective; unlimited waiting can hold shard write locks indefinitely.';

REVOKE ALL ON FUNCTION pg_catalog.citus_promote_clone_and_rebalance(integer, name, integer) FROM PUBLIC;
