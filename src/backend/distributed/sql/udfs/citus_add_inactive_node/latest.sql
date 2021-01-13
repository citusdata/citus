CREATE FUNCTION pg_catalog.citus_add_inactive_node(nodename text,
                                                   nodeport integer,
                                        groupid integer default -1,
                                        noderole noderole default 'primary',
                                        nodecluster name default 'default')
  RETURNS INTEGER
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME',$$citus_add_inactive_node$$;
COMMENT ON FUNCTION pg_catalog.citus_add_inactive_node(nodename text,nodeport integer,
                                            groupid integer, noderole noderole,
                                            nodecluster name)
  IS 'prepare node by adding it to pg_dist_node';
REVOKE ALL ON FUNCTION pg_catalog.citus_add_inactive_node(text,int,int,noderole,name) FROM PUBLIC;
