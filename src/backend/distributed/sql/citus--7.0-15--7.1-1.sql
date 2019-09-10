/* citus--7.0-15--7.1-1 */

SET search_path = 'pg_catalog';

CREATE OR REPLACE FUNCTION master_update_node(node_id int,
                                              new_node_name text,
                                              new_node_port int)
  RETURNS void
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_update_node$$;
COMMENT ON FUNCTION master_update_node(node_id int, new_node_name text, new_node_port int)
  IS 'change the location of a node';

RESET search_path;
