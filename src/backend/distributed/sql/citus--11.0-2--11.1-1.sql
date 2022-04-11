DROP FUNCTION pg_catalog.worker_create_schema(bigint,text);
DROP FUNCTION pg_catalog.worker_cleanup_job_schema_cache();
DROP FUNCTION pg_catalog.worker_fetch_foreign_file(text, text, bigint, text[], integer[]);
DROP FUNCTION pg_catalog.worker_fetch_partition_file(bigint, integer, integer, integer, text, integer);
DROP FUNCTION pg_catalog.worker_hash_partition_table(bigint, integer, text, text, oid, anyarray);
DROP FUNCTION pg_catalog.worker_merge_files_into_table(bigint, integer, text[], text[]);
DROP FUNCTION pg_catalog.worker_range_partition_table(bigint, integer, text, text, oid, anyarray);
DROP FUNCTION pg_catalog.worker_repartition_cleanup(bigint);

SET search_path = 'pg_catalog';

CREATE OR REPLACE FUNCTION lock_relation_if_exists(table_name text, lock_mode text, nowait boolean)
RETURNS BOOL
LANGUAGE C STRICT as 'MODULE_PATHNAME',
$$lock_relation_if_exists$$;
COMMENT ON FUNCTION lock_relation_if_exists(table_name text, lock_mode text, nowait boolean)
IS 'locks relation in the lock_mode if the relation exists';

RESET search_path;
