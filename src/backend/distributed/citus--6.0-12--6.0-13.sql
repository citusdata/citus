/* citus--6.0-12--6.0-13.sql */

CREATE FUNCTION pg_catalog.worker_apply_inter_shard_ddl_command(referencing_shard bigint,
																referencing_schema_name text,
																referenced_shard bigint,
																referenced_schema_name text,
																command text)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_apply_inter_shard_ddl_command$$;
COMMENT ON FUNCTION pg_catalog.worker_apply_inter_shard_ddl_command(referencing_shard bigint,
																	referencing_schema_name text,
																	referenced_shard bigint,
																	referenced_schema_name text,
																	command text)
    IS 'executes inter shard ddl command';
