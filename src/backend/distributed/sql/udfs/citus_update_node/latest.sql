CREATE FUNCTION pg_catalog.citus_update_node(node_id int,
                                              new_node_name text,
                                              new_node_port int,
                                              force bool DEFAULT false,
                                              lock_cooldown int DEFAULT 10000)
  RETURNS void
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$citus_update_node$$;

COMMENT ON FUNCTION pg_catalog.citus_update_node(node_id int,
                                       new_node_name text,
                                       new_node_port int,
                                       force bool,
                                       lock_cooldown int)
  IS 'change the location of a node. when force => true it will wait lock_cooldown ms before killing competing locks';

REVOKE ALL ON FUNCTION pg_catalog.citus_update_node(int,text,int,bool,int) FROM PUBLIC;
