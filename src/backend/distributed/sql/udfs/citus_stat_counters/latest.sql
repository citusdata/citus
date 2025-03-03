CREATE OR REPLACE FUNCTION pg_catalog.citus_stat_counters(
	OUT name text,
	OUT value bigint)
RETURNS SETOF RECORD
LANGUAGE C VOLATILE PARALLEL UNSAFE
AS 'MODULE_PATHNAME', $$citus_stat_counters$$;
COMMENT ON FUNCTION pg_catalog.citus_stat_counters() IS 'Returns Citus stat counters';

CREATE VIEW citus_stat_counters AS SELECT * FROM citus_stat_counters();

GRANT SELECT ON citus_stat_counters TO PUBLIC;
