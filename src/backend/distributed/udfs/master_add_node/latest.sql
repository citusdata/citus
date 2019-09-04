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
                                OUT isactive bool,
                                OUT noderole noderole,
                                OUT nodecluster name,
                                OUT shouldhavedata bool)
  RETURNS record
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_add_node$$;
COMMENT ON FUNCTION master_add_node(nodename text, nodeport integer,
                                    groupid integer, noderole noderole, nodecluster name)
  IS 'add node to the cluster';
REVOKE ALL ON FUNCTION master_add_node(text, integer, integer, noderole, name) FROM PUBLIC;
