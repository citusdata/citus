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
                                     OUT shouldhavedata bool)
    RETURNS record
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME',$$master_activate_node$$;
COMMENT ON FUNCTION master_activate_node(nodename text, nodeport integer)
    IS 'activate a node which is in the cluster';

REVOKE ALL ON FUNCTION master_activate_node(text,int) FROM PUBLIC;
