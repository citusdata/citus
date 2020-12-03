CREATE OR REPLACE FUNCTION pg_catalog.citus_node_disk_space_stats(
  nodename text,
  nodeport int,
  OUT available_disk_size bigint,
  OUT total_disk_size bigint)
RETURNS record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_node_disk_space_stats$$;
COMMENT ON FUNCTION pg_catalog.citus_node_disk_space_stats(text,int)
IS 'returns statistics on available disk space for the given node';
