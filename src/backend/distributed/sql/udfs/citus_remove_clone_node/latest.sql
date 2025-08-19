CREATE OR REPLACE FUNCTION pg_catalog.citus_remove_clone_node(
    nodename text,
    nodeport integer
)
RETURNS VOID
LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME', $$citus_remove_clone_node$$;

COMMENT ON FUNCTION pg_catalog.citus_remove_clone_node(text, integer)
IS 'Removes an inactive streaming clone node from Citus metadata. Errors if the node is not found, not registered as a clone, or is currently marked active.';

REVOKE ALL ON FUNCTION pg_catalog.citus_remove_clone_node(text, integer) FROM PUBLIC;

CREATE OR REPLACE FUNCTION pg_catalog.citus_remove_clone_node_with_nodeid(
    nodeid integer
)
RETURNS VOID
LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME', $$citus_remove_clone_node_with_nodeid$$;

COMMENT ON FUNCTION pg_catalog.citus_remove_clone_node_with_nodeid(integer)
IS 'Removes an inactive streaming clone node from Citus metadata using its node ID. Errors if the node is not found, not registered as a clone, or is currently marked active.';

REVOKE ALL ON FUNCTION pg_catalog.citus_remove_clone_node_with_nodeid(integer) FROM PUBLIC;
