CREATE FUNCTION pg_catalog.citus_remove_node(nodename text, nodeport integer)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$citus_remove_node$$;
COMMENT ON FUNCTION pg_catalog.citus_remove_node(nodename text, nodeport integer)
	IS 'remove node from the cluster';
REVOKE ALL ON FUNCTION pg_catalog.citus_remove_node(text,int) FROM PUBLIC;
