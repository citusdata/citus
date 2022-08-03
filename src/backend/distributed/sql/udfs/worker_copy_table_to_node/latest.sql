CREATE OR REPLACE FUNCTION pg_catalog.worker_copy_table_to_node(
    source_table regclass,
    target_node_id integer)
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$worker_copy_table_to_node$$;
COMMENT ON FUNCTION pg_catalog.worker_copy_table_to_node(regclass, integer)
    IS 'Perform copy of a shard';
