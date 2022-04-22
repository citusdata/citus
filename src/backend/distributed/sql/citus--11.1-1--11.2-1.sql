--  bump version to 11.2-1
--  drop columnar objects if they exists in citus extension 

DO $check_citus$
BEGIN
IF EXISTS (SELECT 1 FROM pg_catalog.pg_extension AS e 
INNER JOIN pg_catalog.pg_depend AS d ON (d.refobjid = e.oid)
INNER JOIN pg_catalog.pg_proc AS p ON (p.oid = d.objid)
WHERE e.extname='citus' and p.proname = 'columnar_handler'
) THEN
ALTER EXTENSION citus DROP SCHEMA columnar;
ALTER EXTENSION citus DROP SEQUENCE columnar.storageid_seq;
    
-- columnar tables
ALTER EXTENSION citus DROP TABLE columnar.options;
ALTER EXTENSION citus DROP TABLE columnar.stripe;
ALTER EXTENSION citus DROP TABLE columnar.chunk_group;
ALTER EXTENSION citus DROP TABLE columnar.chunk;

DO $proc$
BEGIN
-- columnar functions
IF substring(current_Setting('server_version'), '\d+')::int >= 12 THEN
    EXECUTE $$
        ALTER EXTENSION citus DROP FUNCTION columnar.columnar_handler;
        ALTER EXTENSION citus DROP ACCESS METHOD columnar;
        ALTER EXTENSION citus DROP FUNCTION pg_catalog.alter_columnar_table_set;
        ALTER EXTENSION citus DROP FUNCTION pg_catalog.alter_columnar_table_reset;
    $$;
END IF;
END$proc$;

-- functions under citus_internal for columnar
ALTER EXTENSION citus DROP FUNCTION citus_internal.upgrade_columnar_storage;
ALTER EXTENSION citus DROP FUNCTION citus_internal.downgrade_columnar_storage;
ALTER EXTENSION citus DROP FUNCTION citus_internal.columnar_ensure_am_depends_catalog;
END IF;
END $check_citus$;