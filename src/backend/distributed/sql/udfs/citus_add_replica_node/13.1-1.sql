CREATE OR REPLACE FUNCTION pg_catalog.citus_add_replica_node(
    replica_hostname text,
    replica_port integer,
    primary_hostname text,
    primary_port integer)
 RETURNS INTEGER
 LANGUAGE C VOLATILE STRICT
 AS 'MODULE_PATHNAME', $$citus_add_replica_node$$;

COMMENT ON FUNCTION pg_catalog.citus_add_replica_node(text, integer, text, integer) IS
'Adds a new node as a replica of an existing primary node. The replica is initially inactive. Returns the nodeid of the new replica node.';

REVOKE ALL ON FUNCTION pg_catalog.citus_add_replica_node(text, int, text, int) FROM PUBLIC;
