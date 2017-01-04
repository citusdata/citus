/* citus--6.1-12--6.1-13.sql */

SET search_path = 'pg_catalog';

CREATE FUNCTION master_disable_node(nodename text, nodeport integer)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$master_disable_node$$;
COMMENT ON FUNCTION master_disable_node(nodename text, nodeport integer)
	IS 'removes node from the cluster temporarily';
    
RESET search_path;
