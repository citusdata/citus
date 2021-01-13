CREATE FUNCTION pg_catalog.citus_add_secondary_node(nodename text,
                                         nodeport integer,
                                         primaryname text,
                                         primaryport integer,
                                         nodecluster name default 'default')
  RETURNS INTEGER
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$citus_add_secondary_node$$;
COMMENT ON FUNCTION pg_catalog.citus_add_secondary_node(nodename text, nodeport integer,
                                             primaryname text, primaryport integer,
                                             nodecluster name)
  IS 'add a secondary node to the cluster';

REVOKE ALL ON FUNCTION pg_catalog.citus_add_secondary_node(text,int,text,int,name) FROM PUBLIC;
