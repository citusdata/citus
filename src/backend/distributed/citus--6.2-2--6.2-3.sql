/* citus--6.2-2--6.2-3.sql */

SET search_path = 'pg_catalog';

ALTER TABLE pg_dist_node ADD isactive bool;

DROP FUNCTION IF EXISTS master_add_node(text, integer);

CREATE FUNCTION master_add_node(nodename text,
                                nodeport integer,
                                OUT nodeid integer,
                                OUT groupid integer,
                                OUT nodename text,
                                OUT nodeport integer,
                                OUT noderack text,
                                OUT hasmetadata boolean,
                                OUT isactive bool)
    RETURNS record 
    LANGUAGE C STRICT 
    AS 'MODULE_PATHNAME',$$master_add_node$$;
COMMENT ON FUNCTION master_add_node(nodename text, nodeport integer)
    IS 'add node to the cluster'; 

CREATE FUNCTION master_add_inactive_node(nodename text,
                                         nodeport integer,
                                         OUT nodeid integer,
                                         OUT groupid integer,
                                         OUT nodename text,
                                         OUT nodeport integer,
                                         OUT noderack text,
                                         OUT hasmetadata boolean,
                                         OUT isactive bool)
    RETURNS record 
    LANGUAGE C STRICT 
    AS 'MODULE_PATHNAME',$$master_add_inactive_node$$;
COMMENT ON FUNCTION master_add_inactive_node(nodename text,nodeport integer)
    IS 'prepare node by adding it to pg_dist_node';

CREATE FUNCTION master_activate_node(nodename text,
                                     nodeport integer,
                                     OUT nodeid integer,
                                     OUT groupid integer,
                                     OUT nodename text,
                                     OUT nodeport integer,
                                     OUT noderack text,
                                     OUT hasmetadata boolean,
                                     OUT isactive bool)
    RETURNS record 
    LANGUAGE C STRICT 
    AS 'MODULE_PATHNAME',$$master_activate_node$$;
COMMENT ON FUNCTION master_activate_node(nodename text, nodeport integer)
    IS 'activate a node which is in the cluster'; 

RESET search_path;
