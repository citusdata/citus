CREATE FUNCTION pg_catalog.citus_disable_node(nodename text, nodeport integer)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$citus_disable_node$$;
COMMENT ON FUNCTION pg_catalog.citus_disable_node(nodename text, nodeport integer)
	IS 'removes node from the cluster temporarily';

REVOKE ALL ON FUNCTION pg_catalog.citus_disable_node(text,int) FROM PUBLIC;
