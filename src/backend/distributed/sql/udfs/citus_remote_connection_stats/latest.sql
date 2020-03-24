CREATE OR REPLACE FUNCTION citus_remote_connection_stats(OUT hostname text, OUT port int, OUT database_name text, OUT connection_count_to_node int)
	RETURNS SETOF RECORD
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$citus_remote_connection_stats$$;
 COMMENT ON FUNCTION citus_remote_connection_stats(OUT hostname text, OUT port int, OUT database_name text, OUT connection_count_to_node int)
     IS 'returns statistics about remote connections';
