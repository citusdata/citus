-- See the comments for the function in
-- src/backend/distributed/stats/stat_counters.c for more details.
CREATE OR REPLACE FUNCTION pg_catalog.citus_stat_counters(
	database_id oid DEFAULT 0,

	-- must always be the first column or you should accordingly update
	-- StoreDatabaseStatsIntoTupStore() function in src/backend/distributed/stats/stat_counters.c
	OUT database_id oid,

	-- Following stat counter columns must be in the same order as the
	-- StatType enum defined in src/include/distributed/stats/stat_counters.h
	OUT connection_establishment_succeeded bigint,
	OUT connection_establishment_failed bigint,
	OUT connection_reused bigint,
	OUT query_execution_single_shard bigint,
	OUT query_execution_multi_shard bigint,

	-- must always be the last column or you should accordingly update
	-- StoreDatabaseStatsIntoTupStore() function in src/backend/distributed/stats/stat_counters.c
	OUT stats_reset timestamp with time zone
)
RETURNS SETOF RECORD
LANGUAGE C STRICT VOLATILE PARALLEL SAFE
AS 'MODULE_PATHNAME', $$citus_stat_counters$$;
COMMENT ON FUNCTION pg_catalog.citus_stat_counters(oid) IS 'Returns Citus stat counters for the given database OID, or for all databases if 0 is passed. Includes only databases with at least one connection since last restart, including dropped ones.';

-- returns the stat counters for all the databases in local node
CREATE VIEW citus.citus_stat_counters AS
SELECT pg_database.oid,
	   pg_database.datname as name,

	   -- We always COALESCE the counters to 0 because the LEFT JOIN
	   -- will bring the databases that have never been connected to
	   -- since the last restart with NULL counters, but we want to
	   -- show them with 0 counters in the view.
	   COALESCE(citus_stat_counters.connection_establishment_succeeded, 0) as connection_establishment_succeeded,
	   COALESCE(citus_stat_counters.connection_establishment_failed, 0) as connection_establishment_failed,
	   COALESCE(citus_stat_counters.connection_reused, 0) as connection_reused,
	   COALESCE(citus_stat_counters.query_execution_single_shard, 0) as query_execution_single_shard,
	   COALESCE(citus_stat_counters.query_execution_multi_shard, 0) as query_execution_multi_shard,

	   citus_stat_counters.stats_reset
FROM pg_catalog.pg_database
LEFT JOIN (SELECT (pg_catalog.citus_stat_counters(0)).*) citus_stat_counters
ON (oid = database_id);

ALTER VIEW citus.citus_stat_counters SET SCHEMA pg_catalog;

GRANT SELECT ON pg_catalog.citus_stat_counters TO PUBLIC;
