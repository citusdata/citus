CREATE FUNCTION pg_catalog.master_set_node_property(
    nodename text,
    nodeport integer,
    property text,
    value boolean)
  RETURNS VOID
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', 'master_set_node_property';
COMMENT ON FUNCTION pg_catalog.master_set_node_property(
    nodename text,
    nodeport integer,
    property text,
    value boolean)
  IS 'set a property of a node in pg_dist_node';

REVOKE ALL ON FUNCTION pg_catalog.master_set_node_property(
    nodename text,
    nodeport integer,
    property text,
    value boolean)
  FROM PUBLIC;
