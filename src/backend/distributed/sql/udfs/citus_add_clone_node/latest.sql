CREATE OR REPLACE FUNCTION pg_catalog.citus_add_clone_node(
    replica_hostname text,
    replica_port integer,
    primary_hostname text,
    primary_port integer)
 RETURNS INTEGER
 LANGUAGE C VOLATILE STRICT
 AS 'MODULE_PATHNAME', $$citus_add_clone_node$$;

COMMENT ON FUNCTION pg_catalog.citus_add_clone_node(text, integer, text, integer) IS
'Adds a new node as a clone of an existing primary node. The clone is initially inactive. Returns the nodeid of the new clone node.';

REVOKE ALL ON FUNCTION pg_catalog.citus_add_clone_node(text, int, text, int) FROM PUBLIC;

CREATE OR REPLACE FUNCTION pg_catalog.citus_add_clone_node_with_nodeid(
    replica_hostname text,
    replica_port integer,
    primary_nodeid integer)
 RETURNS INTEGER
 LANGUAGE C VOLATILE STRICT
 AS 'MODULE_PATHNAME', $$citus_add_clone_node_with_nodeid$$;

COMMENT ON FUNCTION pg_catalog.citus_add_clone_node_with_nodeid(text, integer, integer) IS
'Adds a new node as a clone of an existing primary node using the primary node''s ID. The clone is initially inactive. Returns the nodeid of the new clone node.';

REVOKE ALL ON FUNCTION pg_catalog.citus_add_clone_node_with_nodeid(text, int, int) FROM PUBLIC;
