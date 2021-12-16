\set upgrade_test_old_citus_version `echo "$CITUS_OLD_VERSION"`
SELECT substring(:'upgrade_test_old_citus_version', 'v(\d+)\.\d+\.\d+')::int >= 10 AND
       substring(:'upgrade_test_old_citus_version', 'v\d+\.(\d+)\.\d+')::int >= 0
AS upgrade_test_old_citus_version_ge_10_0;
\gset
\if :upgrade_test_old_citus_version_ge_10_0
\else
\q
\endif

CREATE SCHEMA upgrade_columnar_metapage;
SET search_path TO upgrade_columnar_metapage, public;

CREATE TABLE columnar_table_1(a INT, b INT) USING columnar;
INSERT INTO columnar_table_1 SELECT i FROM generate_series(160001, 320000) i;

CREATE TABLE columnar_table_2(b INT) USING columnar;
SELECT alter_columnar_table_set('columnar_table_2',
                                chunk_group_row_limit => 1000,
                                stripe_row_limit => 1000);
INSERT INTO columnar_table_2 SELECT i FROM generate_series(1600, 3500) i;

CREATE TABLE columnar_table_3(b INT) USING columnar;
INSERT INTO columnar_table_3 VALUES (1), (2);

CREATE TABLE no_data_columnar_table(a INT, b INT, c TEXT) USING columnar;
