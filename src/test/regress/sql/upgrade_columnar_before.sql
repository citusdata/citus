-- Test if relying on topological sort of the objects, not their names, works
-- fine when re-creating objects during pg_upgrade.

DO
$$
BEGIN
IF EXISTS (SELECT * FROM pg_namespace WHERE nspname = 'upgrade_columnar')
THEN
    -- Drop the the table leftover from the earlier run of
    -- upgrade_columnar_before.sql. Similarly, drop the fake public schema
    -- created before and rename the original one (renamed to citus_schema)
    -- back to public.
    --
    -- This can only happen if upgrade_columnar_before.sql is run multiple
    -- times for flaky test detection.
    DROP TABLE citus_schema.new_columnar_table;
    DROP SCHEMA public CASCADE;
    ALTER SCHEMA citus_schema RENAME TO public;

    SET LOCAL client_min_messages TO WARNING;
    DROP SCHEMA upgrade_columnar CASCADE;
END IF;
END
$$
LANGUAGE plpgsql;

ALTER SCHEMA public RENAME TO citus_schema;
SET search_path TO citus_schema;

-- As mentioned in https://github.com/citusdata/citus/issues/5447, it
-- is essential to already have public schema to be able to use
-- citus_prepare_pg_upgrade. Until fixing that bug, let's create public
-- schema here on all nodes.
CREATE SCHEMA IF NOT EXISTS public;

-- Do "SELECT 1" to hide port numbers
SELECT 1 FROM run_command_on_workers($$CREATE SCHEMA IF NOT EXISTS public$$);

-- create a columnar table within citus_schema
CREATE TABLE new_columnar_table (
  col_1 character varying(100),
  col_2 date,
  col_3 character varying(100),
  col_4 date
) USING columnar;

INSERT INTO new_columnar_table
SELECT ('1', '1999-01-01'::timestamp, '1', '1999-01-01'::timestamp)
FROM generate_series(1, 1000);

CREATE SCHEMA upgrade_columnar;
SET search_path TO upgrade_columnar, public;

CREATE TYPE compfoo AS (f1 int, f2 text);

CREATE TABLE test_retains_data (a int, b text, c compfoo, d int[]) USING columnar;

INSERT INTO test_retains_data VALUES
    (1, 'abc', (1, '4'), ARRAY[1,2,3,4]),
    (2, 'pi', (3, '192'), ARRAY[3,1,4,1,5]),
    (3, 'earth', (4, '22'), ARRAY[1,2,7,5,6]);

--
-- Verify that after upgrade we can read data for tables whose
-- relfilenode has changed before upgrade.
--

-- truncate
CREATE TABLE test_truncated (a int) USING columnar;
INSERT INTO test_truncated SELECT * FROM generate_series(1, 10);
SELECT count(*) FROM test_truncated;

SELECT relfilenode AS relfilenode_pre_truncate
FROM pg_class WHERE oid = 'test_truncated'::regclass::oid \gset

TRUNCATE test_truncated;

SELECT relfilenode AS relfilenode_post_truncate
FROM pg_class WHERE oid = 'test_truncated'::regclass::oid \gset

SELECT :relfilenode_post_truncate <> :relfilenode_pre_truncate AS relfilenode_changed;

INSERT INTO test_truncated SELECT * FROM generate_series(11, 13);
SELECT count(*) FROM test_truncated;

-- vacuum full
CREATE TABLE test_vacuum_full (a int) USING columnar;
INSERT INTO test_vacuum_full SELECT * FROM generate_series(1, 10);
SELECT count(*) FROM test_vacuum_full;

SELECT relfilenode AS relfilenode_pre_vacuum_full
FROM pg_class WHERE oid = 'test_vacuum_full'::regclass::oid \gset

VACUUM FULL test_vacuum_full;

SELECT relfilenode AS relfilenode_post_vacuum_full
FROM pg_class WHERE oid = 'test_vacuum_full'::regclass::oid \gset

SELECT :relfilenode_post_vacuum_full <> :relfilenode_pre_vacuum_full AS relfilenode_changed;

INSERT INTO test_vacuum_full SELECT * FROM generate_series(11, 13);
SELECT count(*) FROM test_vacuum_full;

-- alter column type
CREATE TABLE test_alter_type (a int) USING columnar;
INSERT INTO test_alter_type SELECT * FROM generate_series(1, 10);
SELECT count(*) FROM test_alter_type;

SELECT relfilenode AS relfilenode_pre_alter
FROM pg_class WHERE oid = 'test_alter_type'::regclass::oid \gset

ALTER TABLE test_alter_type ALTER COLUMN a TYPE text;

SELECT relfilenode AS relfilenode_post_alter
FROM pg_class WHERE oid = 'test_alter_type'::regclass::oid \gset

SELECT :relfilenode_pre_alter <> :relfilenode_post_alter AS relfilenode_changed;

INSERT INTO test_alter_type SELECT * FROM generate_series(11, 13);
SELECT count(*) FROM test_alter_type;

-- materialized view
CREATE MATERIALIZED VIEW matview(a, b) USING columnar AS
SELECT floor(a/3), array_agg(b) FROM test_retains_data GROUP BY 1;

SELECT relfilenode AS relfilenode_pre_refresh
FROM pg_class WHERE oid = 'matview'::regclass::oid \gset

REFRESH MATERIALIZED VIEW matview;

SELECT relfilenode AS relfilenode_post_refresh
FROM pg_class WHERE oid = 'matview'::regclass::oid \gset

SELECT :relfilenode_pre_alter <> :relfilenode_post_alter AS relfilenode_changed;

--
-- Test that we retain options
--

SET columnar.stripe_row_limit TO 5000;
SET columnar.chunk_group_row_limit TO 1000;
SET columnar.compression TO 'pglz';

CREATE TABLE test_options_1(a int, b int) USING columnar;
INSERT INTO test_options_1 SELECT i, floor(i/1000) FROM generate_series(1, 10000) i;

CREATE TABLE test_options_2(a int, b int) USING columnar;
INSERT INTO test_options_2 SELECT i, floor(i/1000) FROM generate_series(1, 10000) i;
SELECT alter_columnar_table_set('test_options_2', chunk_group_row_limit => 2000);
SELECT alter_columnar_table_set('test_options_2', stripe_row_limit => 6000);
SELECT alter_columnar_table_set('test_options_2', compression => 'none');
SELECT alter_columnar_table_set('test_options_2', compression_level => 13);
INSERT INTO test_options_2 SELECT i, floor(i/2000) FROM generate_series(1, 10000) i;

Create or replace function test_jsonb() returns jsonb as
$$
begin
	return '{"test_json": "test"}';
end;
$$ language plpgsql;


CREATE TABLE less_common_data_types_table
(
	dist_key bigint,
	col1 int[], col2 int[][], col3 int [][][],
	col4 varchar[], col5 varchar[][], col6 varchar [][][],
	col70 bit, col7 bit[], col8 bit[][], col9 bit [][][],
	col10 bit varying(10),
	col11 bit varying(10)[], col12 bit varying(10)[][], col13 bit varying(10)[][][],
	col14 bytea, col15 bytea[], col16 bytea[][], col17 bytea[][][],
	col18 boolean, col19 boolean[], col20 boolean[][], col21 boolean[][][],
	col22 inet, col23 inet[], col24 inet[][], col25 inet[][][],
	col26 macaddr, col27 macaddr[], col28 macaddr[][], col29 macaddr[][][],
	col30 numeric, col32 numeric[], col33 numeric[][], col34 numeric[][][],
	col35 jsonb, col36 jsonb[], col37 jsonb[][], col38 jsonb[][][]
) USING COLUMNAR;

CREATE UNIQUE INDEX unique_index_on_columnar ON less_common_data_types_table(dist_key, col1);
CREATE INDEX non_unique_index_on_columnar ON less_common_data_types_table(dist_key, col1);

INSERT INTO less_common_data_types_table (dist_key,col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col30, col32, col33, col34, col35, col36, col37, col38)
VALUES (1,ARRAY[1], ARRAY[ARRAY[0,0,0]], ARRAY[ARRAY[ARRAY[0,0,0]]], ARRAY['1'], ARRAY[ARRAY['0','0','0']], ARRAY[ARRAY[ARRAY['0','0','0']]], '1', ARRAY[b'1'], ARRAY[ARRAY[b'0',b'0',b'0']], ARRAY[ARRAY[ARRAY[b'0',b'0',b'0']]], '11101',ARRAY[b'1'], ARRAY[ARRAY[b'01',b'01',b'01']], ARRAY[ARRAY[ARRAY[b'011',b'110',b'0000']]], '\xb4a8e04c0b', ARRAY['\xb4a8e04c0b'::BYTEA], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA, '\xb4a8e04c0b'::BYTEA, '\xb4a8e04c0b'::BYTEA]], ARRAY[ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]]], '1', ARRAY[TRUE], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[ARRAY[1::boolean,TRUE,FALSE]]], INET '192.168.1/24', ARRAY[INET '192.168.1.1'], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']]],MACADDR '08:00:2b:01:02:03', ARRAY[MACADDR '08:00:2b:01:02:03'], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']]], 690, ARRAY[1.1], ARRAY[ARRAY[0,0.111,0.15]], ARRAY[ARRAY[ARRAY[0,0,0]]], test_jsonb(), ARRAY[test_jsonb()], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]]]),
       (2,ARRAY[1,2,3], ARRAY[ARRAY[1,2,3], ARRAY[5,6,7]], ARRAY[ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]], ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]]], ARRAY['1','2','3'], ARRAY[ARRAY['1','2','3'], ARRAY['5','6','7']], ARRAY[ARRAY[ARRAY['1','2','3']], ARRAY[ARRAY['5','6','7']], ARRAY[ARRAY['1','2','3']], ARRAY[ARRAY['5','6','7']]], '0', ARRAY[b'1',b'0',b'0'], ARRAY[ARRAY[b'1',b'1',b'0'], ARRAY[b'0',b'0',b'1']], ARRAY[ARRAY[ARRAY[b'1',b'1',b'1']], ARRAY[ARRAY[b'1','0','0']], ARRAY[ARRAY[b'1','1','1']], ARRAY[ARRAY[b'0','0','0']]], '00010', ARRAY[b'11',b'10',b'01'], ARRAY[ARRAY[b'11',b'010',b'101'], ARRAY[b'101',b'01111',b'1000001']], ARRAY[ARRAY[ARRAY[b'10000',b'111111',b'1101010101']], ARRAY[ARRAY[b'1101010','0','1']], ARRAY[ARRAY[b'1','1','11111111']], ARRAY[ARRAY[b'0000000','0','0']]], '\xb4a8e04c0b', ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA], ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]]], 'true', ARRAY[1::boolean,TRUE,FALSE], ARRAY[ARRAY[1::boolean,TRUE,FALSE], ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]]],'0.0.0.0/32', ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24'], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']]], '0800.2b01.0203', ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203'], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']]], 0.99, ARRAY[1.1,2.22,3.33], ARRAY[ARRAY[1.55,2.66,3.88], ARRAY[11.5,10101.6,7111.1]], ARRAY[ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]], ARRAY[ARRAY[1.1,2.1,3]], ARRAY[ARRAY[5.0,6.0,7.0]]],test_jsonb(), ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()], ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]]]);

-- insert the same data with RETURNING
INSERT INTO less_common_data_types_table (dist_key,col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col30, col32, col33, col34, col35, col36, col37, col38)
VALUES (3,ARRAY[1], ARRAY[ARRAY[0,0,0]], ARRAY[ARRAY[ARRAY[0,0,0]]], ARRAY['1'], ARRAY[ARRAY['0','0','0']], ARRAY[ARRAY[ARRAY['0','0','0']]], '1', ARRAY[b'1'], ARRAY[ARRAY[b'0',b'0',b'0']], ARRAY[ARRAY[ARRAY[b'0',b'0',b'0']]], '11101',ARRAY[b'1'], ARRAY[ARRAY[b'01',b'01',b'01']], ARRAY[ARRAY[ARRAY[b'011',b'110',b'0000']]], '\xb4a8e04c0b', ARRAY['\xb4a8e04c0b'::BYTEA], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA, '\xb4a8e04c0b'::BYTEA, '\xb4a8e04c0b'::BYTEA]], ARRAY[ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]]], '1', ARRAY[TRUE], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[ARRAY[1::boolean,TRUE,FALSE]]], INET '192.168.1/24', ARRAY[INET '192.168.1.1'], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']]],MACADDR '08:00:2b:01:02:03', ARRAY[MACADDR '08:00:2b:01:02:03'], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']]], 690, ARRAY[1.1], ARRAY[ARRAY[0,0.111,0.15]], ARRAY[ARRAY[ARRAY[0,0,0]]], test_jsonb(), ARRAY[test_jsonb()], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]]]),
       (4,ARRAY[1,2,3], ARRAY[ARRAY[1,2,3], ARRAY[5,6,7]], ARRAY[ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]], ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]]], ARRAY['1','2','3'], ARRAY[ARRAY['1','2','3'], ARRAY['5','6','7']], ARRAY[ARRAY[ARRAY['1','2','3']], ARRAY[ARRAY['5','6','7']], ARRAY[ARRAY['1','2','3']], ARRAY[ARRAY['5','6','7']]], '0', ARRAY[b'1',b'0',b'0'], ARRAY[ARRAY[b'1',b'1',b'0'], ARRAY[b'0',b'0',b'1']], ARRAY[ARRAY[ARRAY[b'1',b'1',b'1']], ARRAY[ARRAY[b'1','0','0']], ARRAY[ARRAY[b'1','1','1']], ARRAY[ARRAY[b'0','0','0']]], '00010', ARRAY[b'11',b'10',b'01'], ARRAY[ARRAY[b'11',b'010',b'101'], ARRAY[b'101',b'01111',b'1000001']], ARRAY[ARRAY[ARRAY[b'10000',b'111111',b'1101010101']], ARRAY[ARRAY[b'1101010','0','1']], ARRAY[ARRAY[b'1','1','11111111']], ARRAY[ARRAY[b'0000000','0','0']]], '\xb4a8e04c0b', ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA], ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]]], 'true', ARRAY[1::boolean,TRUE,FALSE], ARRAY[ARRAY[1::boolean,TRUE,FALSE], ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]]],'0.0.0.0/32', ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24'], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']]], '0800.2b01.0203', ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203'], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']]], 0.99, ARRAY[1.1,2.22,3.33], ARRAY[ARRAY[1.55,2.66,3.88], ARRAY[11.5,10101.6,7111.1]], ARRAY[ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]], ARRAY[ARRAY[1.1,2.1,3]], ARRAY[ARRAY[5.0,6.0,7.0]]],test_jsonb(), ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()], ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]]])
       RETURNING *;

-- GROUP BY w/wout the dist key
SELECT
	count(*)
FROM
	less_common_data_types_table
GROUP BY
	col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38;

SELECT
	count(*)
FROM
	less_common_data_types_table
GROUP BY
	dist_key, col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38;


-- window function w/wout distribution key
SELECT
	count(*) OVER (PARTITION BY col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38)
FROM
	less_common_data_types_table;

SELECT
	count(*) OVER (PARTITION BY dist_key, col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38)
FROM
	less_common_data_types_table;

-- DISTINCT w/wout distribution key
-- there seems to be an issue with SELECT DISTINCT ROW with PG14
-- so we add an alternative output that gives an error, this should
-- be removed after the issue is fixed on PG14.
SELECT DISTINCT(col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38)
FROM
	less_common_data_types_table
ORDER BY 1 DESC;

SELECT DISTINCT(dist_key, col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38)
FROM
	less_common_data_types_table
ORDER BY 1 DESC;

-- count DISTINCT w/wout dist key
SELECT count(DISTINCT(col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38))
FROM
	less_common_data_types_table
ORDER BY 1 DESC;

SELECT count(DISTINCT(dist_key, col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38))
FROM
	less_common_data_types_table
ORDER BY 1 DESC;

-- a query that might use index, but doesn't use as chunk group filtering is cheaper
SELECT count(*) FROM less_common_data_types_table WHERE dist_key = 1 AND col1 = ARRAY[1];

-- make sure that we test index scan
set columnar.enable_custom_scan to 'off';
set enable_seqscan to off;
set seq_page_cost TO 10000000;

EXPLAIN (costs off, timing off, summary off, analyze on)
	SELECT count(*) FROM less_common_data_types_table WHERE dist_key = 1 AND col1 = ARRAY[1];

-- make sure that we re-enable columnar scan
RESET columnar.enable_custom_scan;
RESET enable_seqscan;
RESET seq_page_cost;

-- Create a columnar table with
--    (a) PRIMARY KEY on c1
--    (b) UNIQUE KEY on c2
--    (c) EXCLUDE CONSTRAINT on c1
CREATE TABLE columnar_with_constraints (c1 INT PRIMARY KEY,
										c2 INT UNIQUE,
										c3 INT, EXCLUDE USING btree (c3 WITH =))
USING columnar;



-- violate (a) PRIMARY KEY
INSERT INTO columnar_with_constraints (c1) VALUES (1), (1);

-- violate (b) UNIQUE KEY
INSERT INTO columnar_with_constraints (c1, c2) VALUES (1, 1), (2, 1);

-- violate (c)  EXCLUDE CONSTRAINTS
INSERT INTO columnar_with_constraints (c1, c3) VALUES (1, 1), (2, 1);

-- finally, insert a ROW
INSERT INTO columnar_with_constraints (c1, c2, c3) VALUES (1, 2, 3);

-- some data with TOAST column and data
CREATE OR REPLACE FUNCTION generate_random_string(
  length INTEGER,
  characters TEXT default 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
) RETURNS TEXT AS
$$
DECLARE
  result TEXT := '';
BEGIN
  FOR __ IN 1..length LOOP
    result := result || substr(characters, floor(random() * length(characters))::int + 1, 1);
  end loop;
  RETURN result;
END;
$$ LANGUAGE plpgsql;

-- create table and load data
CREATE TABLE text_data (id SERIAL, value TEXT) USING COLUMNAR;
INSERT INTO text_data (value) SELECT generate_random_string(1024 * 10) FROM generate_series(0,10);
select count(DISTINCT value) from text_data;

-- test using a columnar partition
CREATE TABLE foo (d DATE NOT NULL) PARTITION BY RANGE (d);
CREATE TABLE foo3 PARTITION OF foo FOR VALUES FROM ('2009-02-01') TO ('2009-03-01') USING COLUMNAR;
