-- Add activate_on_rebalance
DROP FUNCTION master_add_inactive_node(text, integer, integer, noderole, name);
CREATE FUNCTION master_add_inactive_node(nodename text,
                                         nodeport integer,
                                         groupid integer default -1,
                                         noderole noderole default 'primary',
                                         nodecluster name default 'default',
                                         activate_on_rebalance boolean default false)
  RETURNS INTEGER
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME',$$master_add_inactive_node$$;
COMMENT ON FUNCTION master_add_inactive_node(text,integer,integer,noderole,name,boolean)
  IS 'prepare node by adding it to pg_dist_node';
REVOKE ALL ON FUNCTION master_add_inactive_node(text,int,int,noderole,name,boolean) FROM PUBLIC;
