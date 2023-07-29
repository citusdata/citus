CREATE FUNCTION pg_catalog.citus_pause_node_within_txn(node_id int)
  RETURNS void
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$citus_pause_node_within_txn$$;

COMMENT ON FUNCTION pg_catalog.citus_pause_node_within_txn(node_id int)
  IS 'pauses node with given id which leads to add lock in tables and prevent any queries to be executed on that node';

REVOKE ALL ON FUNCTION pg_catalog.citus_pause_node_within_txn(int) FROM PUBLIC;
