CREATE OR REPLACE FUNCTION pg_catalog.alter_columnar_table_set(
    table_name regclass,
    chunk_group_row_limit int DEFAULT NULL,
    stripe_row_limit int DEFAULT NULL,
    compression name DEFAULT null,
    compression_level int DEFAULT NULL)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'alter_columnar_table_set';

COMMENT ON FUNCTION pg_catalog.alter_columnar_table_set(
    table_name regclass,
    chunk_group_row_limit int,
    stripe_row_limit int,
    compression name,
    compression_level int)
IS 'set one or more options on a columnar table, when set to NULL no change is made';
CREATE OR REPLACE FUNCTION pg_catalog.alter_columnar_table_reset(
    table_name regclass,
    chunk_group_row_limit bool DEFAULT false,
    stripe_row_limit bool DEFAULT false,
    compression bool DEFAULT false,
    compression_level bool DEFAULT false)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'alter_columnar_table_reset';

COMMENT ON FUNCTION pg_catalog.alter_columnar_table_reset(
    table_name regclass,
    chunk_group_row_limit bool,
    stripe_row_limit bool,
    compression bool,
    compression_level bool)
IS 'reset on or more options on a columnar table to the system defaults';

CREATE OR REPLACE FUNCTION columnar_internal.columnar_ensure_am_depends_catalog()
  RETURNS void
  LANGUAGE plpgsql
  SET search_path = pg_catalog
AS $func$
BEGIN
  INSERT INTO pg_depend
  SELECT -- Define a dependency edge from "columnar table access method" ..
         'pg_am'::regclass::oid as classid,
         (select oid from pg_am where amname = 'columnar') as objid,
         0 as objsubid,
         -- ... to each object that is registered to pg_class and that lives
         -- in "columnar" schema. That contains catalog tables, indexes
         -- created on them and the sequences created in "columnar" schema.
         --
         -- Given the possibility of user might have created their own objects
         -- in columnar schema, we explicitly specify list of objects that we
         -- are interested in.
         'pg_class'::regclass::oid as refclassid,
         columnar_schema_members.relname::regclass::oid as refobjid,
         0 as refobjsubid,
         'n' as deptype
  FROM (VALUES ('columnar.chunk'),
               ('columnar.chunk_group'),
               ('columnar.chunk_group_pkey'),
               ('columnar.chunk_pkey'),
               ('columnar.options'),
               ('columnar.options_pkey'),
               ('columnar.storageid_seq'),
               ('columnar.stripe'),
               ('columnar.stripe_first_row_number_idx'),
               ('columnar.stripe_pkey')
       ) columnar_schema_members(relname)
  -- Avoid inserting duplicate entries into pg_depend.
  EXCEPT TABLE pg_depend;
END;
$func$;
COMMENT ON FUNCTION columnar_internal.columnar_ensure_am_depends_catalog()
  IS 'internal function responsible for creating dependencies from columnar '
     'table access method to the rel objects in columnar schema';

DROP VIEW columnar.options;
DROP VIEW columnar.stripe;
DROP VIEW columnar.chunk_group;
DROP VIEW columnar.chunk;
DROP VIEW columnar.storage;
DROP FUNCTION columnar.get_storage_id(regclass);

DROP SCHEMA columnar;

-- move columnar_internal functions back to citus_internal

ALTER FUNCTION columnar_internal.upgrade_columnar_storage(regclass) SET SCHEMA citus_internal;
ALTER FUNCTION columnar_internal.downgrade_columnar_storage(regclass) SET SCHEMA citus_internal;
ALTER FUNCTION columnar_internal.columnar_ensure_am_depends_catalog() SET SCHEMA citus_internal;

ALTER SCHEMA columnar_internal RENAME TO columnar;
GRANT USAGE ON SCHEMA columnar TO PUBLIC;
GRANT SELECT ON columnar.options TO PUBLIC;
GRANT SELECT ON columnar.stripe TO PUBLIC;
GRANT SELECT ON columnar.chunk_group TO PUBLIC;

-- detach relations from citus_columnar

ALTER EXTENSION citus_columnar DROP SCHEMA columnar;
ALTER EXTENSION citus_columnar DROP SEQUENCE columnar.storageid_seq;
-- columnar tables
ALTER EXTENSION citus_columnar DROP TABLE columnar.options;
ALTER EXTENSION citus_columnar DROP TABLE columnar.stripe;
ALTER EXTENSION citus_columnar DROP TABLE columnar.chunk_group;
ALTER EXTENSION citus_columnar DROP TABLE columnar.chunk;

ALTER EXTENSION citus_columnar DROP FUNCTION columnar.columnar_handler;
ALTER EXTENSION citus_columnar DROP ACCESS METHOD columnar;
ALTER EXTENSION citus_columnar DROP FUNCTION pg_catalog.alter_columnar_table_set;
ALTER EXTENSION citus_columnar DROP FUNCTION pg_catalog.alter_columnar_table_reset;

-- functions under citus_internal for columnar
ALTER EXTENSION citus_columnar DROP FUNCTION citus_internal.upgrade_columnar_storage;
ALTER EXTENSION citus_columnar DROP FUNCTION citus_internal.downgrade_columnar_storage;
ALTER EXTENSION citus_columnar DROP FUNCTION citus_internal.columnar_ensure_am_depends_catalog;
