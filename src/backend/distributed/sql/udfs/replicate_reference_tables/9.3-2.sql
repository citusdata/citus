CREATE FUNCTION pg_catalog.replicate_reference_tables()
  RETURNS VOID
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$replicate_reference_tables$$;
COMMENT ON FUNCTION pg_catalog.replicate_reference_tables()
  IS 'replicate reference tables to all nodes';
REVOKE ALL ON FUNCTION pg_catalog.replicate_reference_tables() FROM PUBLIC;
