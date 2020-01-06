CREATE OR REPLACE FUNCTION pg_catalog.alter_role_if_exists(
    role_name text,
    utility_query text)
    RETURNS BOOL
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$alter_role_if_exists$$;

COMMENT ON FUNCTION pg_catalog.alter_role_if_exists(
    role_name text,
    utility_query text)
    IS 'runs the utility query, if the role exists';
