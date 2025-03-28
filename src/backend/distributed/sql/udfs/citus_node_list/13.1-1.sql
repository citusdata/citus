CREATE TYPE pg_catalog.citus_noderole AS ENUM (
  'coordinator', -- node is coordinator
  'worker'       -- node is worker
);

CREATE OR REPLACE FUNCTION pg_catalog.citus_node_list(
  IN active BOOL DEFAULT null,
  IN role citus_noderole DEFAULT null,
  OUT node_name text,
  OUT node_port bigint)
    RETURNS SETOF record
    LANGUAGE C VOLATILE ROWS 100
    AS 'MODULE_PATHNAME', $$citus_node_list$$;
COMMENT ON FUNCTION pg_catalog.citus_node_list(active BOOL, role citus_noderole)
    IS 'fetch set of nodes which match the given criteria';
