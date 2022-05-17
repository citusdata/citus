CREATE FUNCTION pg_catalog.worker_create_schema(jobid bigint, username text)
 RETURNS void
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$worker_create_schema$function$;

CREATE FUNCTION pg_catalog.worker_cleanup_job_schema_cache()
 RETURNS void
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$worker_cleanup_job_schema_cache$function$;

CREATE FUNCTION pg_catalog.worker_fetch_foreign_file(text, text, bigint, text[], integer[])
 RETURNS void
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$worker_fetch_foreign_file$function$;

CREATE FUNCTION pg_catalog.worker_fetch_partition_file(bigint, integer, integer, integer, text, integer)
 RETURNS void
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$worker_fetch_partition_file$function$;

CREATE FUNCTION pg_catalog.worker_hash_partition_table(bigint, integer, text, text, oid, anyarray)
 RETURNS void
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$worker_hash_partition_table$function$;

CREATE FUNCTION pg_catalog.worker_merge_files_into_table(bigint, integer, text[], text[])
 RETURNS void
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$worker_merge_files_into_table$function$;

CREATE FUNCTION pg_catalog.worker_range_partition_table(bigint, integer, text, text, oid, anyarray)
 RETURNS void
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$worker_range_partition_table$function$;

CREATE FUNCTION pg_catalog.worker_repartition_cleanup(bigint)
 RETURNS void
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$worker_repartition_cleanup$function$;

#include "../../../columnar/sql/downgrades/columnar--11.1-1--11.0-2.sql"

-- add relations to citus
ALTER EXTENSION citus ADD SCHEMA columnar;
ALTER EXTENSION citus ADD SEQUENCE columnar.storageid_seq;
ALTER EXTENSION citus ADD TABLE columnar.options;
ALTER EXTENSION citus ADD TABLE columnar.stripe;
ALTER EXTENSION citus ADD TABLE columnar.chunk_group;
ALTER EXTENSION citus ADD TABLE columnar.chunk;

ALTER EXTENSION citus ADD FUNCTION columnar.columnar_handler;
ALTER EXTENSION citus ADD ACCESS METHOD columnar;
ALTER EXTENSION citus ADD FUNCTION pg_catalog.alter_columnar_table_set;
ALTER EXTENSION citus ADD FUNCTION pg_catalog.alter_columnar_table_reset;

ALTER EXTENSION citus ADD FUNCTION citus_internal.upgrade_columnar_storage;
ALTER EXTENSION citus ADD FUNCTION citus_internal.downgrade_columnar_storage;
ALTER EXTENSION citus ADD FUNCTION citus_internal.columnar_ensure_am_depends_catalog;
