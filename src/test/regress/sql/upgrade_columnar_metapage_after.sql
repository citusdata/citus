\set upgrade_test_old_citus_version `echo "$upgrade_test_old_citus_version"`
SELECT substring(:'upgrade_test_old_citus_version', 'v(\d+)\.\d+\.\d+')::int >= 10 AND
       substring(:'upgrade_test_old_citus_version', 'v\d+\.(\d+)\.\d+')::int >= 0
AS upgrade_test_old_citus_version_ge_10_0;
\gset
\if :upgrade_test_old_citus_version_ge_10_0
\else
\q
\endif

-- it's not the best practice to define this here, but we don't want to include
-- columnar_test_helpers in upgrade test schedule
CREATE OR REPLACE FUNCTION columnar_storage_info(
  rel regclass,
  version_major OUT int4,
  version_minor OUT int4,
  storage_id OUT int8,
  reserved_stripe_id OUT int8,
  reserved_row_number OUT int8,
  reserved_offset OUT int8)
STRICT
LANGUAGE c AS 'citus', 'columnar_storage_info';

SET search_path TO upgrade_columnar_metapage, public;

-- should work since we upgrade metapages when upgrading schema version
INSERT INTO columnar_table_1 VALUES (3);

-- show that all columnar relation's metapage's are upgraded to "2.0"
SELECT count(*)=0
FROM (SELECT (columnar_storage_info(c.oid)).* t
      FROM pg_class c, pg_am a
      WHERE c.relam = a.oid AND amname = 'columnar') t
WHERE t.version_major != 2 and t.version_minor != 0;

-- print metapage for two of the tables
SELECT columnar_storage_info('columnar_table_1');
SELECT columnar_storage_info('columnar_table_2');

-- table is already upgraded, make sure that upgrade_columnar_metapage is no-op
SELECT citus_internal.upgrade_columnar_storage(c.oid)
FROM pg_class c, pg_am a
WHERE c.relam = a.oid AND amname = 'columnar' and relname = 'columnar_table_2';

SELECT columnar_storage_info('columnar_table_2');
