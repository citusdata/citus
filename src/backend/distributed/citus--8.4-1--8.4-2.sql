/* citus--8.3-1--8.4-1 */

SET search_path = 'pg_catalog';

ALTER TABLE pg_dist_node ADD COLUMN metadatasynced BOOLEAN DEFAULT FALSE;
COMMENT ON COLUMN pg_dist_node.metadatasynced IS
    'indicates whether the node has the most recent metadata';

DROP FUNCTION master_add_node(text, integer, integer, noderole, name);
CREATE FUNCTION master_add_node(nodename text,
                                nodeport integer,
                                groupid integer default 0,
                                noderole noderole default 'primary',
                                nodecluster name default 'default',
                                OUT nodeid integer,
                                OUT groupid integer,
                                OUT nodename text,
                                OUT nodeport integer,
                                OUT noderack text,
                                OUT hasmetadata boolean,
                                OUT isactive boolean,
                                OUT noderole noderole,
                                OUT nodecluster name,
                                OUT metadatasynced boolean)
  RETURNS record
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_add_node$$;
COMMENT ON FUNCTION master_add_node(nodename text, nodeport integer,
                                    groupid integer, noderole noderole, nodecluster name)
  IS 'add node to the cluster';

DROP FUNCTION master_add_inactive_node(text, integer, integer, noderole, name);
CREATE FUNCTION master_add_inactive_node(nodename text,
                                         nodeport integer,
                                         groupid integer default 0,
                                         noderole noderole default 'primary',
                                         nodecluster name default 'default',
                                         OUT nodeid integer,
                                         OUT groupid integer,
                                         OUT nodename text,
                                         OUT nodeport integer,
                                         OUT noderack text,
                                         OUT hasmetadata boolean,
                                         OUT isactive boolean,
                                         OUT noderole noderole,
                                         OUT nodecluster name,
                                         OUT metadatasynced boolean)
  RETURNS record
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME',$$master_add_inactive_node$$;
COMMENT ON FUNCTION master_add_inactive_node(nodename text,nodeport integer,
                                             groupid integer, noderole noderole,
                                             nodecluster name)
  IS 'prepare node by adding it to pg_dist_node';


DROP FUNCTION master_activate_node(text, integer);
CREATE FUNCTION master_activate_node(nodename text,
                                     nodeport integer,
                                     OUT nodeid integer,
                                     OUT groupid integer,
                                     OUT nodename text,
                                     OUT nodeport integer,
                                     OUT noderack text,
                                     OUT hasmetadata boolean,
                                     OUT isactive boolean,
                                     OUT noderole noderole,
                                     OUT nodecluster name,
                                     OUT metadatasynced boolean)
    RETURNS record
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME',$$master_activate_node$$;
COMMENT ON FUNCTION master_activate_node(nodename text, nodeport integer)
    IS 'activate a node which is in the cluster';

DROP FUNCTION master_add_secondary_node(text, integer, text, integer, name);
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
                                          OUT isactive boolean,
                                          OUT noderole noderole,
                                          OUT nodecluster name,
                                          OUT metadatasynced boolean)
  RETURNS record
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_add_secondary_node$$;
COMMENT ON FUNCTION master_add_secondary_node(nodename text, nodeport integer,
                                              primaryname text, primaryport integer,
                                              nodecluster name)
  IS 'add a secondary node to the cluster';


REVOKE ALL ON FUNCTION master_activate_node(text,int) FROM PUBLIC;
REVOKE ALL ON FUNCTION master_add_inactive_node(text,int,int,noderole,name) FROM PUBLIC;
REVOKE ALL ON FUNCTION master_add_node(text,int,int,noderole,name) FROM PUBLIC;
REVOKE ALL ON FUNCTION master_add_secondary_node(text,int,text,int,name) FROM PUBLIC;

RESET search_path;
