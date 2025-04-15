-- See the comments for the function in
-- src/backend/distributed/stat_counters.c for more details.
CREATE OR REPLACE FUNCTION pg_catalog.citus_stat_counters(
	database_id oid DEFAULT 0,

	-- must always be the first column or you should accordingly update
	-- StoreStatCountersFromArray() function in src/backend/distributed/stat_counters.c

	-- Following stat counter columns must be in the same order as the
	-- StatType enum defined in src/include/distributed/stat_counters.h
	OUT connection_establishment_succeeded bigint,
	OUT connection_establishment_failed bigint,
	OUT connection_reused bigint,
	OUT query_execution_single_shard bigint,
	OUT query_execution_multi_shard bigint,

	-- must always be the last column or you should accordingly update
	-- StoreStatCountersFromArray() function in src/backend/distributed/stat_counters.c
	OUT stats_reset timestamp with time zone
)
RETURNS SETOF RECORD
LANGUAGE C VOLATILE PARALLEL UNSAFE
AS 'MODULE_PATHNAME', $$citus_stat_counters$$;
COMMENT ON FUNCTION pg_catalog.citus_stat_counters(oid) IS 'Returns Citus stat counters for the given database OID, or for all databases if 0 is passed. Includes only databases with at least one connection since last restart. Zero counters are replaced with NULL.';

-- Returns the stat counters for all the databases in local node, possibly
-- with NULL values for the counters which we haven't observed any counter
-- increments since the last reset or server restart.
CREATE VIEW citus.citus_stat_counters AS
SELECT pg_database.oid,
	   pg_database.datname as name,
	   citus_stat_counters.connection_establishment_succeeded,
	   citus_stat_counters.connection_establishment_failed,
	   citus_stat_counters.connection_reused,
	   citus_stat_counters.query_execution_single_shard,
	   citus_stat_counters.query_execution_multi_shard,
	   citus_stat_counters.stats_reset
FROM pg_catalog.pg_database
LEFT JOIN (SELECT (pg_catalog.citus_stat_counters(0)).*) citus_stat_counters
ON (oid = database_id);

ALTER VIEW citus.citus_stat_counters SET SCHEMA pg_catalog;

GRANT SELECT ON pg_catalog.citus_stat_counters TO PUBLIC;
