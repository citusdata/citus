CREATE OR REPLACE FUNCTION pg_catalog.worker_create_or_replace_object(statement text)
  RETURNS bool
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$worker_create_or_replace_object$$;
COMMENT ON FUNCTION pg_catalog.worker_create_or_replace_object(statement text)
    IS 'takes a sql CREATE statement, before executing the create it will check if an object with that name already exists and safely replaces that named object with the new object';
