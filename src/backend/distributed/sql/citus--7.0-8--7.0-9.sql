/* citus-7.0-8--7.0-9 */

SET search_path = 'pg_catalog';

ALTER TABLE pg_dist_node ADD COLUMN nodecluster name NOT NULL DEFAULT 'default';
ALTER TABLE pg_dist_node
  ADD CONSTRAINT primaries_are_only_allowed_in_the_default_cluster
  CHECK (NOT (nodecluster <> 'default' AND noderole = 'primary'));

DROP FUNCTION master_add_node(text, integer, integer, noderole);
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
                                OUT isactive bool,
                                OUT noderole noderole,
                                OUT nodecluster name)
  RETURNS record
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_add_node$$;
COMMENT ON FUNCTION master_add_node(nodename text, nodeport integer,
                                    groupid integer, noderole noderole, nodecluster name)
  IS 'add node to the cluster';

DROP FUNCTION master_add_inactive_node(text, integer, integer, noderole);
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
                                         OUT isactive bool,
                                         OUT noderole noderole,
                                         OUT nodecluster name)
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
                                     OUT isactive bool,
                                     OUT noderole noderole,
                                     OUT nodecluster name)
    RETURNS record
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME',$$master_activate_node$$;
COMMENT ON FUNCTION master_activate_node(nodename text, nodeport integer)
    IS 'activate a node which is in the cluster';

RESET search_path;
