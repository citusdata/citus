CREATE OR REPLACE FUNCTION pg_catalog.worker_create_or_replace_object(statement text)
  RETURNS bool
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$worker_create_or_replace_object$$;

COMMENT ON FUNCTION pg_catalog.worker_create_or_replace_object(statement text)
    IS 'takes a sql CREATE statement, before executing the create it will check if an object with that name already exists and safely replaces that named object with the new object';

CREATE OR REPLACE FUNCTION pg_catalog.worker_create_or_replace_object(statements text[])
  RETURNS bool
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$worker_create_or_replace_object_array$$;

COMMENT ON FUNCTION pg_catalog.worker_create_or_replace_object(statements text[])
    IS 'takes an array of sql statements, before executing these it will check if the object already exists in that exact state otherwise replaces that named object with the new object';
