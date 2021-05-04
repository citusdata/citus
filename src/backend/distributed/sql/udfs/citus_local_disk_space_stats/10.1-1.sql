CREATE OR REPLACE FUNCTION pg_catalog.citus_local_disk_space_stats(
  OUT available_disk_size bigint,
  OUT total_disk_size bigint)
RETURNS record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_local_disk_space_stats$$;
COMMENT ON FUNCTION pg_catalog.citus_local_disk_space_stats()
IS 'returns statistics on available disk space on the local node';
