CREATE OR REPLACE FUNCTION pg_catalog.citus_stat_counters(
	OUT name text,
	OUT value bigint)
RETURNS SETOF RECORD
LANGUAGE C VOLATILE PARALLEL UNSAFE
AS 'MODULE_PATHNAME', $$citus_stat_counters$$;
COMMENT ON FUNCTION pg_catalog.citus_stat_counters() IS 'Returns Citus stat counters';

CREATE VIEW citus.citus_stat_counters AS SELECT * FROM pg_catalog.citus_stat_counters();

ALTER VIEW citus.citus_stat_counters SET SCHEMA pg_catalog;

GRANT SELECT ON pg_catalog.citus_stat_counters TO PUBLIC;
