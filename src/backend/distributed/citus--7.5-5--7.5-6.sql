/* citus--7.5-5--7.5-6 */

SET search_path = 'pg_catalog';

ALTER TABLE pg_catalog.pg_dist_node
    ADD COLUMN directname text,
    ADD COLUMN directport integer,
    ADD CONSTRAINT direct_complete
        CHECK ((directname IS NULL) = (directport IS NULL));

DROP FUNCTION master_add_node(text, integer, integer, noderole, name);
CREATE FUNCTION master_add_node(nodename text,
                                nodeport integer,
                                groupid integer default 0,
                                noderole noderole default 'primary',
                                nodecluster name default 'default',
                                directname text default NULL,
                                directport integer default NULL,
                                OUT nodeid integer,
                                OUT groupid integer,
                                OUT nodename text,
                                OUT nodeport integer,
                                OUT noderack text,
                                OUT hasmetadata boolean,
                                OUT isactive bool,
                                OUT noderole noderole,
                                OUT nodecluster name,
                                OUT directname text,
                                OUT directport integer)
  RETURNS record
  LANGUAGE C
  AS 'MODULE_PATHNAME', $$master_add_node$$;
COMMENT ON FUNCTION master_add_node(nodename text, nodeport integer,
                                    groupid integer, noderole noderole, nodecluster name,
                                    directname text, directport integer)
  IS 'add node to the cluster';

DROP FUNCTION master_add_inactive_node(text, integer, integer, noderole, name);
CREATE FUNCTION master_add_inactive_node(nodename text,
                                         nodeport integer,
                                         groupid integer default 0,
                                         noderole noderole default 'primary',
                                         nodecluster name default 'default',
                                         directname text default NULL,
                                         directport integer default NULL,
                                         OUT nodeid integer,
                                         OUT groupid integer,
                                         OUT nodename text,
                                         OUT nodeport integer,
                                         OUT noderack text,
                                         OUT hasmetadata boolean,
                                         OUT isactive bool,
                                         OUT noderole noderole,
                                         OUT nodecluster name,
                                         OUT directname text,
                                         OUT directport integer)
  RETURNS record
  LANGUAGE C
  AS 'MODULE_PATHNAME',$$master_add_inactive_node$$;
COMMENT ON FUNCTION master_add_inactive_node(nodename text, nodeport integer,
                                             groupid integer, noderole noderole,
                                             nodecluster name, directname text,
                                             directport integer)
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
                                     OUT nodecluster name,
                                     OUT directname text,
                                     OUT directport integer)
    RETURNS record
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME',$$master_activate_node$$;
COMMENT ON FUNCTION master_activate_node(nodename text, nodeport integer)
    IS 'activate a node which is in the cluster';

ALTER TABLE pg_dist_authinfo
    ADD COLUMN directauthinfo text
               CONSTRAINT directauthinfo_valid
						  CHECK (authinfo_valid(directauthinfo));

RESET search_path;