DROP FUNCTION  pg_catalog.citus_disable_node(nodename text, nodeport integer, force bool);
CREATE FUNCTION pg_catalog.citus_disable_node(nodename text, nodeport integer, force bool default false, synchronous bool default false)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$citus_disable_node$$;
COMMENT ON FUNCTION pg_catalog.citus_disable_node(nodename text, nodeport integer, force bool, synchronous bool)
	IS 'removes node from the cluster temporarily';

REVOKE ALL ON FUNCTION pg_catalog.citus_disable_node(text,int, bool, bool) FROM PUBLIC;
