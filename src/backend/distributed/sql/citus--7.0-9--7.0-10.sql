/* citus-7.0-9--7.0-10 */

SET search_path = 'pg_catalog';

CREATE FUNCTION master_add_secondary_node(nodename text,
                                          nodeport integer,
                                          primaryname text,
                                          primaryport integer,
                                          nodecluster name default 'default',
                                          OUT nodeid integer,
                                          OUT groupid integer,
                                          OUT nodename text,
                                          OUT nodeport integer,
                                          OUT noderack text,
                                          OUT hasmetadata boolean,
                                          OUT isactive bool,
                                          OUT noderole noderole,
                                          OUT nodecluster name)
  RETURNS record
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_add_secondary_node$$;
COMMENT ON FUNCTION master_add_secondary_node(nodename text, nodeport integer,
                                              primaryname text, primaryport integer,
                                              nodecluster name)
  IS 'add a secondary node to the cluster';

RESET search_path;
