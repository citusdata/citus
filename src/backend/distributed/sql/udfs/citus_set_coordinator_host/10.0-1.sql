CREATE FUNCTION pg_catalog.citus_set_coordinator_host(
    host text,
    port integer default current_setting('port')::int,
    node_role noderole default 'primary',
    node_cluster name default 'default')
RETURNS VOID
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_set_coordinator_host$$;

COMMENT ON FUNCTION pg_catalog.citus_set_coordinator_host(text,integer,noderole,name)
IS 'set the host and port of the coordinator';

REVOKE ALL ON FUNCTION pg_catalog.citus_set_coordinator_host(text,int,noderole,name) FROM PUBLIC;
