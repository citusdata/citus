-- add columnar objects back

ALTER EXTENSION citus_columnar ADD SCHEMA columnar;
ALTER EXTENSION citus_columnar ADD SEQUENCE columnar.storageid_seq;
ALTER EXTENSION citus_columnar ADD TABLE columnar.options;
ALTER EXTENSION citus_columnar ADD TABLE columnar.stripe;
ALTER EXTENSION citus_columnar ADD TABLE columnar.chunk_group;
ALTER EXTENSION citus_columnar ADD TABLE columnar.chunk;

ALTER EXTENSION citus_columnar ADD FUNCTION columnar.columnar_handler;
ALTER EXTENSION citus_columnar ADD ACCESS METHOD columnar;
ALTER EXTENSION citus_columnar ADD FUNCTION pg_catalog.alter_columnar_table_set;
ALTER EXTENSION citus_columnar ADD FUNCTION pg_catalog.alter_columnar_table_reset;

ALTER EXTENSION citus_columnar ADD FUNCTION citus_internal.upgrade_columnar_storage;
ALTER EXTENSION citus_columnar ADD FUNCTION citus_internal.downgrade_columnar_storage;
ALTER EXTENSION citus_columnar ADD FUNCTION citus_internal.columnar_ensure_am_depends_catalog;

CREATE OR REPLACE FUNCTION pg_catalog.alter_columnar_table_set(
    table_name regclass,
    chunk_group_row_limit int DEFAULT NULL,
    stripe_row_limit int DEFAULT NULL,
    compression name DEFAULT null,
    compression_level int DEFAULT NULL)
    RETURNS void
    LANGUAGE plpgsql AS
$alter_columnar_table_set$
declare
  noop BOOLEAN := true;
  cmd  TEXT    := 'ALTER TABLE ' || table_name::text || ' SET (';
begin
  if (chunk_group_row_limit is not null) then
    if (not noop) then cmd := cmd || ', '; end if;
    cmd := cmd  || 'columnar.chunk_group_row_limit=' || chunk_group_row_limit;
    noop := false;
  end if;
  if (stripe_row_limit is not null) then
    if (not noop) then cmd := cmd || ', '; end if;
    cmd := cmd || 'columnar.stripe_row_limit=' || stripe_row_limit;
    noop := false;
  end if;
  if (compression is not null) then
    if (not noop) then cmd := cmd || ', '; end if;
    cmd := cmd || 'columnar.compression=' || compression;
    noop := false;
  end if;
  if (compression_level is not null) then
    if (not noop) then cmd := cmd || ', '; end if;
    cmd := cmd || 'columnar.compression_level=' || compression_level;
    noop := false;
  end if;
  cmd := cmd || ')';
  if (not noop) then
    execute cmd;
  end if;
  return;
end;
$alter_columnar_table_set$;

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
    LANGUAGE plpgsql AS
$alter_columnar_table_reset$
declare
  noop BOOLEAN := true;
  cmd  TEXT    := 'ALTER TABLE ' || table_name::text || ' RESET (';
begin
  if (chunk_group_row_limit) then
    if (not noop) then cmd := cmd || ', '; end if;
    cmd := cmd   || 'columnar.chunk_group_row_limit';
    noop := false;
  end if;
  if (stripe_row_limit) then
    if (not noop) then cmd := cmd || ', '; end if;
    cmd := cmd || 'columnar.stripe_row_limit';
    noop := false;
  end if;
  if (compression) then
    if (not noop) then cmd := cmd || ', '; end if;
    cmd := cmd || 'columnar.compression';
    noop := false;
  end if;
  if (compression_level) then
    if (not noop) then cmd := cmd || ', '; end if;
    cmd := cmd || 'columnar.compression_level';
    noop := false;
  end if;
  cmd := cmd || ')';
  if (not noop) then
    execute cmd;
  end if;
  return;
end;
$alter_columnar_table_reset$;

COMMENT ON FUNCTION pg_catalog.alter_columnar_table_reset(
    table_name regclass,
    chunk_group_row_limit bool,
    stripe_row_limit bool,
    compression bool,
    compression_level bool)
IS 'reset on or more options on a columnar table to the system defaults';

-- rename columnar schema to columnar_internal and tighten security

REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA columnar FROM PUBLIC;
ALTER SCHEMA columnar RENAME TO columnar_internal;
REVOKE ALL PRIVILEGES ON SCHEMA columnar_internal FROM PUBLIC;

-- move citus_internal functions to columnar_internal

ALTER FUNCTION citus_internal.upgrade_columnar_storage(regclass) SET SCHEMA columnar_internal;
ALTER FUNCTION citus_internal.downgrade_columnar_storage(regclass) SET SCHEMA columnar_internal;
ALTER FUNCTION citus_internal.columnar_ensure_am_depends_catalog() SET SCHEMA columnar_internal;

-- create columnar schema with public usage privileges

CREATE SCHEMA columnar;
GRANT USAGE ON SCHEMA columnar TO PUBLIC;

-- update UDF to account for columnar_internal schema
CREATE OR REPLACE FUNCTION citus_internal.columnar_ensure_am_depends_catalog()
  RETURNS void
  LANGUAGE plpgsql
  SET search_path = pg_catalog
AS $func$
BEGIN
  INSERT INTO pg_depend
  WITH columnar_schema_members(relid) AS (
    SELECT pg_class.oid AS relid FROM pg_class
      WHERE relnamespace =
            COALESCE(
	       (SELECT pg_namespace.oid FROM pg_namespace WHERE nspname = 'columnar_internal'),
	       (SELECT pg_namespace.oid FROM pg_namespace WHERE nspname = 'columnar')
	    )
        AND relname IN ('chunk',
                        'chunk_group',
                        'chunk_group_pkey',
                        'chunk_pkey',
                        'options',
                        'options_pkey',
                        'storageid_seq',
                        'stripe',
                        'stripe_first_row_number_idx',
                        'stripe_pkey')
  )
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
         columnar_schema_members.relid as refobjid,
         0 as refobjsubid,
         'n' as deptype
  FROM columnar_schema_members
  -- Avoid inserting duplicate entries into pg_depend.
  EXCEPT TABLE pg_depend;
END;
$func$;
COMMENT ON FUNCTION citus_internal.columnar_ensure_am_depends_catalog()
  IS 'internal function responsible for creating dependencies from columnar '
     'table access method to the rel objects in columnar schema';

-- add utility function

CREATE FUNCTION columnar.get_storage_id(regclass) RETURNS bigint
    LANGUAGE C STRICT
    AS 'citus_columnar', $$columnar_relation_storageid$$;

-- create views for columnar table information

CREATE VIEW columnar.storage WITH (security_barrier) AS
  SELECT c.oid::regclass AS relation,
         columnar.get_storage_id(c.oid) AS storage_id
    FROM pg_class c, pg_am am
    WHERE c.relam = am.oid AND am.amname = 'columnar'
      AND pg_has_role(c.relowner, 'USAGE');
COMMENT ON VIEW columnar.storage IS 'Columnar relation ID to storage ID mapping.';
GRANT SELECT ON columnar.storage TO PUBLIC;

CREATE VIEW columnar.options WITH (security_barrier) AS
  SELECT regclass AS relation, chunk_group_row_limit,
         stripe_row_limit, compression, compression_level
    FROM columnar_internal.options o, pg_class c
    WHERE o.regclass = c.oid
      AND pg_has_role(c.relowner, 'USAGE');
COMMENT ON VIEW columnar.options
  IS 'Columnar options for tables on which the current user has ownership privileges.';
GRANT SELECT ON columnar.options TO PUBLIC;

CREATE VIEW columnar.stripe WITH (security_barrier) AS
  SELECT relation, storage.storage_id, stripe_num, file_offset, data_length,
         column_count, chunk_row_count, row_count, chunk_group_count, first_row_number
    FROM columnar_internal.stripe stripe, columnar.storage storage
    WHERE stripe.storage_id = storage.storage_id;
COMMENT ON VIEW columnar.stripe
  IS 'Columnar stripe information for tables on which the current user has ownership privileges.';
GRANT SELECT ON columnar.stripe TO PUBLIC;

CREATE VIEW columnar.chunk_group WITH (security_barrier) AS
  SELECT relation, storage.storage_id, stripe_num, chunk_group_num, row_count
    FROM columnar_internal.chunk_group cg, columnar.storage storage
    WHERE cg.storage_id = storage.storage_id;
COMMENT ON VIEW columnar.chunk_group
  IS 'Columnar chunk group information for tables on which the current user has ownership privileges.';
GRANT SELECT ON columnar.chunk_group TO PUBLIC;

CREATE VIEW columnar.chunk WITH (security_barrier) AS
  SELECT relation, storage.storage_id, stripe_num, attr_num, chunk_group_num,
         minimum_value, maximum_value, value_stream_offset, value_stream_length,
         exists_stream_offset, exists_stream_length, value_compression_type,
         value_compression_level, value_decompressed_length, value_count
    FROM columnar_internal.chunk chunk, columnar.storage storage
    WHERE chunk.storage_id = storage.storage_id;
COMMENT ON VIEW columnar.chunk
  IS 'Columnar chunk information for tables on which the current user has ownership privileges.';
GRANT SELECT ON columnar.chunk TO PUBLIC;

