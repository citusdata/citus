/* citus--7.1-2--7.1-3 */

CREATE TABLE citus.pg_dist_node_metadata(
    metadata jsonb NOT NULL
);
ALTER TABLE citus.pg_dist_node_metadata SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_node_metadata TO public;

CREATE FUNCTION pg_catalog.citus_server_id()
    RETURNS uuid
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_server_id$$;
COMMENT ON FUNCTION citus_server_id()
    IS 'generates a random UUID to be used as server identifier';

INSERT INTO pg_dist_node_metadata
    VALUES (jsonb_build_object('server_id', citus_server_id()::text));
