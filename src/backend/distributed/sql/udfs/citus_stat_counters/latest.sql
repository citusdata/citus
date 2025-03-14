CREATE OR REPLACE FUNCTION pg_catalog.citus_stat_counters(
	/*
	 * If you change the order of database_oid column, make sure to update
	 * StoreAllStatCounters() in src/backend/distributed/stat_counters.c
	 */
	OUT database_oid oid,

	/*
	 * Following stat counter columns must be in the same order as the
	 * StatType enum defined in src/include/distributed/stat_counters.h
	 */
	OUT connection_establishment_succeeded bigint,
	OUT connection_establishment_failed bigint,
	OUT connection_reused bigint,

	OUT query_execution_single_shard bigint,
	OUT query_execution_multi_shard bigint
)
RETURNS SETOF RECORD
LANGUAGE C VOLATILE PARALLEL UNSAFE
AS 'MODULE_PATHNAME', $$citus_stat_counters$$;
COMMENT ON FUNCTION pg_catalog.citus_stat_counters() IS 'Returns Citus stat counters';

CREATE VIEW citus.citus_stat_counters AS SELECT * FROM pg_catalog.citus_stat_counters();

ALTER VIEW citus.citus_stat_counters SET SCHEMA pg_catalog;

GRANT SELECT ON pg_catalog.citus_stat_counters TO PUBLIC;
