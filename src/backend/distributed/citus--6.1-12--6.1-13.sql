/* citus--6.1-12--6.1-13.sql */

SET search_path = 'pg_catalog';

DROP FUNCTION IF EXISTS master_remove_node(nodename text, nodeport integer);

CREATE FUNCTION master_remove_node(nodename text, nodeport integer,
								   force bool DEFAULT false)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$master_remove_node$$;
COMMENT ON FUNCTION master_remove_node(nodename text, nodeport integer, force bool)
	IS 'remove node from the cluster';
    
RESET search_path;
