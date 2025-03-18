CREATE FUNCTION pg_catalog.citus_is_primary_node()
 RETURNS bool
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $$citus_is_primary_node$$;
COMMENT ON FUNCTION pg_catalog.citus_is_primary_node()
 IS 'returns whether the current node is the primary node in the group';
