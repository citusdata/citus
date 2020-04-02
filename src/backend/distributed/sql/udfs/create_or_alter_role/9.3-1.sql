CREATE OR REPLACE FUNCTION pg_catalog.create_or_alter_role(
    role_name text,
    create_role_utility_query text,
    alter_role_utility_query text,
    grant_role_utility_queries text[])
    RETURNS BOOL
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$create_or_alter_role$$;

COMMENT ON FUNCTION pg_catalog.create_or_alter_role(
    role_name text,
    create_role_utility_query text,
    alter_role_utility_query text,
    grant_role_utility_queries text[])
    IS 'runs the create role query, if the role doesn''t exists, runs the alter role query if it does';
