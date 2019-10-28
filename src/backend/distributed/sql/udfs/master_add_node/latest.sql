-- Update the default groupId to -1
DROP FUNCTION master_add_node(text, integer, integer, noderole, name);
CREATE FUNCTION master_add_node(nodename text,
                                nodeport integer,
                                groupid integer default -1,
                                noderole noderole default 'primary',
                                nodecluster name default 'default')
  RETURNS INTEGER
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_add_node$$;
COMMENT ON FUNCTION master_add_node(nodename text, nodeport integer,
                                    groupid integer, noderole noderole, nodecluster name)
  IS 'add node to the cluster';
REVOKE ALL ON FUNCTION master_add_node(text,int,int,noderole,name) FROM PUBLIC;
