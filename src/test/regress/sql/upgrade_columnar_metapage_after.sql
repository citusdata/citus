\set upgrade_test_old_citus_version `echo "$CITUS_OLD_VERSION"`
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

CREATE VIEW columnar_table_stripe_info AS
SELECT columnar_table_storageids.relname relname,
       columnar.stripe.stripe_num stripe_num,
       columnar.stripe.row_count row_count,
       columnar.stripe.first_row_number first_row_number
FROM columnar.stripe,
(
  SELECT c.oid relid, c.relname relname, (columnar_storage_info(c.oid)).storage_id relstorageid
  FROM pg_class c, pg_am a
  WHERE c.relam = a.oid AND amname = 'columnar'
) columnar_table_storageids
WHERE relstorageid = columnar.stripe.storage_id;

SET search_path TO upgrade_columnar_metapage, public;

-- show that first_row_number values are equal to MAX(row_count) * stripe_num + COLUMNAR_FIRST_ROW_NUMBER
SELECT * FROM columnar_table_stripe_info ORDER BY relname, stripe_num;

-- should work since we upgrade metapages when upgrading schema version
INSERT INTO columnar_table_1 VALUES (3);

-- state of stripe metadata for columnar_table_1 after post-upgrade insert
SELECT * FROM columnar_table_stripe_info WHERE relname = 'columnar_table_1' ORDER BY stripe_num;

-- show that all columnar relation's metapage's are upgraded to "2.0"
SELECT count(*)=0
FROM (SELECT (columnar_storage_info(c.oid)).* t
      FROM pg_class c, pg_am a
      WHERE c.relam = a.oid AND amname = 'columnar') t
WHERE t.version_major != 2 and t.version_minor != 0;

-- print metapage for two of the tables
SELECT version_major, version_minor, reserved_stripe_id, reserved_row_number
  FROM columnar_storage_info('columnar_table_1');
SELECT version_major, version_minor, reserved_stripe_id, reserved_row_number
  FROM columnar_storage_info('columnar_table_2');

-- show that no_data_columnar_table also has metapage after upgrade
SELECT version_major, version_minor, reserved_stripe_id, reserved_row_number
  FROM columnar_storage_info('no_data_columnar_table');

-- table is already upgraded, make sure that upgrade_columnar_metapage is no-op
SELECT columnar_internal.upgrade_columnar_storage(c.oid)
FROM pg_class c, pg_am a
WHERE c.relam = a.oid AND amname = 'columnar' and relname = 'columnar_table_2';

SELECT version_major, version_minor, reserved_stripe_id, reserved_row_number
  FROM columnar_storage_info('columnar_table_2');

VACUUM FULL columnar_table_2;

-- print metapage and stripe metadata after post-upgrade vacuum full
SELECT version_major, version_minor, reserved_stripe_id, reserved_row_number
  FROM columnar_storage_info('columnar_table_2');
SELECT * FROM columnar_table_stripe_info WHERE relname = 'columnar_table_2' ORDER BY stripe_num;
