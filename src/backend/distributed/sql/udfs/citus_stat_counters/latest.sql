CREATE OR REPLACE FUNCTION pg_catalog.citus_stat_counters(
	database_oid oid,

	-- Following stat counter columns must be in the same order as the
	-- StatType enum defined in src/include/distributed/stat_counters.h
	OUT connection_establishment_succeeded bigint,
	OUT connection_establishment_failed bigint,
	OUT connection_reused bigint,

	OUT query_execution_single_shard bigint,
	OUT query_execution_multi_shard bigint,

	-- Must always be the last column or you should accordingly update
	-- StoreStatCountersFromArray() function in src/backend/distributed/stat_counters.c
	OUT stats_reset timestamp with time zone
)
RETURNS SETOF RECORD
LANGUAGE C VOLATILE PARALLEL UNSAFE
AS 'MODULE_PATHNAME', $$citus_stat_counters$$;
COMMENT ON FUNCTION pg_catalog.citus_stat_counters(oid) IS 'Returns Citus stat counters for the given database';

CREATE VIEW citus.citus_stat_counters AS
SELECT oid, datname, (pg_catalog.citus_stat_counters(oid)).* FROM pg_catalog.pg_database;

ALTER VIEW citus.citus_stat_counters SET SCHEMA pg_catalog;

GRANT SELECT ON pg_catalog.citus_stat_counters TO PUBLIC;
