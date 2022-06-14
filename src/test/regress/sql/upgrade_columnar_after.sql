SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 11 AS server_version_above_eleven
\gset
\if :server_version_above_eleven
\else
\q
\endif

SET search_path TO upgrade_columnar, public;

-- test we retained data
SELECT * FROM test_retains_data ORDER BY a;

SELECT count(*) FROM test_retains_data;

SELECT a,c FROM test_retains_data ORDER BY a;
SELECT b,d FROM test_retains_data ORDER BY a;

SELECT * FROM test_retains_data ORDER BY a;

-- test we retained data with a once truncated table
SELECT * FROM test_truncated ORDER BY a;

-- test we retained data with a once vacuum fulled table
SELECT * FROM test_vacuum_full ORDER BY a;

-- test we retained data with a once alter typed table
SELECT * FROM test_alter_type ORDER BY a;

-- test we retained data with a once refreshed materialized view
SELECT * FROM matview ORDER BY a;

-- test we retained options
SELECT * FROM columnar.options WHERE relation = 'test_options_1'::regclass;
VACUUM VERBOSE test_options_1;
SELECT count(*), sum(a), sum(b) FROM test_options_1;

SELECT * FROM columnar.options WHERE relation = 'test_options_2'::regclass;
VACUUM VERBOSE test_options_2;
SELECT count(*), sum(a), sum(b) FROM test_options_2;

BEGIN;
	INSERT INTO less_common_data_types_table (dist_key,col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col30, col32, col33, col34, col35, col36, col37, col38)
	VALUES (5,ARRAY[1], ARRAY[ARRAY[0,0,0]], ARRAY[ARRAY[ARRAY[0,0,0]]], ARRAY['1'], ARRAY[ARRAY['0','0','0']], ARRAY[ARRAY[ARRAY['0','0','0']]], '1', ARRAY[b'1'], ARRAY[ARRAY[b'0',b'0',b'0']], ARRAY[ARRAY[ARRAY[b'0',b'0',b'0']]], '11101',ARRAY[b'1'], ARRAY[ARRAY[b'01',b'01',b'01']], ARRAY[ARRAY[ARRAY[b'011',b'110',b'0000']]], '\xb4a8e04c0b', ARRAY['\xb4a8e04c0b'::BYTEA], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA, '\xb4a8e04c0b'::BYTEA, '\xb4a8e04c0b'::BYTEA]], ARRAY[ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]]], '1', ARRAY[TRUE], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[ARRAY[1::boolean,TRUE,FALSE]]], INET '192.168.1/24', ARRAY[INET '192.168.1.1'], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']]],MACADDR '08:00:2b:01:02:03', ARRAY[MACADDR '08:00:2b:01:02:03'], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']]], 690, ARRAY[1.1], ARRAY[ARRAY[0,0.111,0.15]], ARRAY[ARRAY[ARRAY[0,0,0]]], test_jsonb(), ARRAY[test_jsonb()], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]]]),
		   (6,ARRAY[1,2,3], ARRAY[ARRAY[1,2,3], ARRAY[5,6,7]], ARRAY[ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]], ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]]], ARRAY['1','2','3'], ARRAY[ARRAY['1','2','3'], ARRAY['5','6','7']], ARRAY[ARRAY[ARRAY['1','2','3']], ARRAY[ARRAY['5','6','7']], ARRAY[ARRAY['1','2','3']], ARRAY[ARRAY['5','6','7']]], '0', ARRAY[b'1',b'0',b'0'], ARRAY[ARRAY[b'1',b'1',b'0'], ARRAY[b'0',b'0',b'1']], ARRAY[ARRAY[ARRAY[b'1',b'1',b'1']], ARRAY[ARRAY[b'1','0','0']], ARRAY[ARRAY[b'1','1','1']], ARRAY[ARRAY[b'0','0','0']]], '00010', ARRAY[b'11',b'10',b'01'], ARRAY[ARRAY[b'11',b'010',b'101'], ARRAY[b'101',b'01111',b'1000001']], ARRAY[ARRAY[ARRAY[b'10000',b'111111',b'1101010101']], ARRAY[ARRAY[b'1101010','0','1']], ARRAY[ARRAY[b'1','1','11111111']], ARRAY[ARRAY[b'0000000','0','0']]], '\xb4a8e04c0b', ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA], ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]]], 'true', ARRAY[1::boolean,TRUE,FALSE], ARRAY[ARRAY[1::boolean,TRUE,FALSE], ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]]],'0.0.0.0/32', ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24'], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']]], '0800.2b01.0203', ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203'], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']]], 0.99, ARRAY[1.1,2.22,3.33], ARRAY[ARRAY[1.55,2.66,3.88], ARRAY[11.5,10101.6,7111.1]], ARRAY[ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]], ARRAY[ARRAY[1.1,2.1,3]], ARRAY[ARRAY[5.0,6.0,7.0]]],test_jsonb(), ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()], ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]]]);

	-- insert the same data with RETURNING
	INSERT INTO less_common_data_types_table (dist_key,col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col30, col32, col33, col34, col35, col36, col37, col38)
	VALUES (7,ARRAY[1], ARRAY[ARRAY[0,0,0]], ARRAY[ARRAY[ARRAY[0,0,0]]], ARRAY['1'], ARRAY[ARRAY['0','0','0']], ARRAY[ARRAY[ARRAY['0','0','0']]], '1', ARRAY[b'1'], ARRAY[ARRAY[b'0',b'0',b'0']], ARRAY[ARRAY[ARRAY[b'0',b'0',b'0']]], '11101',ARRAY[b'1'], ARRAY[ARRAY[b'01',b'01',b'01']], ARRAY[ARRAY[ARRAY[b'011',b'110',b'0000']]], '\xb4a8e04c0b', ARRAY['\xb4a8e04c0b'::BYTEA], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA, '\xb4a8e04c0b'::BYTEA, '\xb4a8e04c0b'::BYTEA]], ARRAY[ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]]], '1', ARRAY[TRUE], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[ARRAY[1::boolean,TRUE,FALSE]]], INET '192.168.1/24', ARRAY[INET '192.168.1.1'], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']]],MACADDR '08:00:2b:01:02:03', ARRAY[MACADDR '08:00:2b:01:02:03'], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']]], 690, ARRAY[1.1], ARRAY[ARRAY[0,0.111,0.15]], ARRAY[ARRAY[ARRAY[0,0,0]]], test_jsonb(), ARRAY[test_jsonb()], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]]]),
		   (8,ARRAY[1,2,3], ARRAY[ARRAY[1,2,3], ARRAY[5,6,7]], ARRAY[ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]], ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]]], ARRAY['1','2','3'], ARRAY[ARRAY['1','2','3'], ARRAY['5','6','7']], ARRAY[ARRAY[ARRAY['1','2','3']], ARRAY[ARRAY['5','6','7']], ARRAY[ARRAY['1','2','3']], ARRAY[ARRAY['5','6','7']]], '0', ARRAY[b'1',b'0',b'0'], ARRAY[ARRAY[b'1',b'1',b'0'], ARRAY[b'0',b'0',b'1']], ARRAY[ARRAY[ARRAY[b'1',b'1',b'1']], ARRAY[ARRAY[b'1','0','0']], ARRAY[ARRAY[b'1','1','1']], ARRAY[ARRAY[b'0','0','0']]], '00010', ARRAY[b'11',b'10',b'01'], ARRAY[ARRAY[b'11',b'010',b'101'], ARRAY[b'101',b'01111',b'1000001']], ARRAY[ARRAY[ARRAY[b'10000',b'111111',b'1101010101']], ARRAY[ARRAY[b'1101010','0','1']], ARRAY[ARRAY[b'1','1','11111111']], ARRAY[ARRAY[b'0000000','0','0']]], '\xb4a8e04c0b', ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA], ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]], ARRAY[ARRAY['\xb4a8e04c0b'::BYTEA,'\x18a232a678'::BYTEA,'\x38b2697632'::BYTEA]]], 'true', ARRAY[1::boolean,TRUE,FALSE], ARRAY[ARRAY[1::boolean,TRUE,FALSE], ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]], ARRAY[ARRAY[1::boolean,TRUE,FALSE]]],'0.0.0.0/32', ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24'], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']], ARRAY[ARRAY[INET '0.0.0.0', '0.0.0.0/32', '::ffff:fff0:1', '192.168.1/24']]], '0800.2b01.0203', ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203'], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']], ARRAY[ARRAY[MACADDR '08002b-010203', MACADDR '08002b-010203', '08002b010203']]], 0.99, ARRAY[1.1,2.22,3.33], ARRAY[ARRAY[1.55,2.66,3.88], ARRAY[11.5,10101.6,7111.1]], ARRAY[ARRAY[ARRAY[1,2,3]], ARRAY[ARRAY[5,6,7]], ARRAY[ARRAY[1.1,2.1,3]], ARRAY[ARRAY[5.0,6.0,7.0]]],test_jsonb(), ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()], ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]], ARRAY[ARRAY[test_jsonb(),test_jsonb(),test_jsonb(),test_jsonb()]]])
		   RETURNING *;
ROLLBACK;

-- count DISTINCT w/wout dist key
SELECT count(DISTINCT(col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38))
FROM
	less_common_data_types_table
ORDER BY 1 DESC;

SELECT count(DISTINCT(dist_key, col1, col2, col3, col4, col5, col6, col70, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col32, col33, col34, col35, col36, col37, col38))
FROM
	less_common_data_types_table
ORDER BY 1 DESC;

-- some batch loads via INSERT .. SELECT
INSERT INTO less_common_data_types_table SELECT * FROM less_common_data_types_table;

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

-- violate (a) PRIMARY KEY
INSERT INTO columnar_with_constraints (c1) VALUES (1), (1);

-- violate (b) UNIQUE KEY
INSERT INTO columnar_with_constraints (c1, c2) VALUES (1, 1), (2, 1);

-- violate (c)  EXCLUDE CONSTRAINTS
INSERT INTO columnar_with_constraints (c1, c3) VALUES (1, 1), (2, 1);

-- finally, insert two ROWs
BEGIN;
  INSERT INTO columnar_with_constraints (c1, c2, c3) VALUES (30, 40, 50), (60, 70, 80);
ROLLBACK;

-- make sure that we can re-create the tables & load some data
BEGIN;
	CREATE TABLE test_retains_data_like (LIKE test_retains_data) USING columnar;
	INSERT INTO test_retains_data_like SELECT * FROM test_retains_data_like;

	CREATE TABLE less_common_data_types_table_like (LIKE less_common_data_types_table INCLUDING INDEXES) USING columnar;
	INSERT INTO less_common_data_types_table_like SELECT * FROM less_common_data_types_table;

	CREATE TABLE columnar_with_constraints_like (LIKE columnar_with_constraints INCLUDING CONSTRAINTS) USING columnar;
	INSERT INTO columnar_with_constraints_like SELECT * FROM columnar_with_constraints_like;

	INSERT INTO text_data (value) SELECT generate_random_string(1024 * 10) FROM generate_series(0,10);
	SELECT count(DISTINCT value) FROM text_data;

	-- make sure that serial is preserved
	-- since we run "after schedule" twice and "rollback" wouldn't undo
	-- sequence changes, it can be 22 or 33, not a different value
	SELECT max(id) in (22, 33) FROM text_data;

	-- since we run "after schedule" twice, rollback the transaction
	-- to avoid getting "table already exists" errors
ROLLBACK;

BEGIN;
  -- Show that we can still drop the extension after upgrading
  SET client_min_messages TO WARNING;

  -- Drop extension migth cascade to columnar.options before dropping a
  -- columnar table. In that case, we were getting below error when opening
  -- columnar.options to delete records for the columnar table that we are
  -- about to drop.: "ERROR:  could not open relation with OID 0".
  --
  -- I somehow reproduced this bug easily when upgrading pg, that is why
  -- adding the test to this file.
  --
  -- TODO: Need to uncomment following line after fixing
  --       https://github.com/citusdata/citus/issues/5483.
    -- DROP EXTENSION citus CASCADE;
ROLLBACK;

-- Make sure that we define dependencies from all rel objects (tables,
-- indexes, sequences ..) to columnar table access method.
--
-- Given that this test file is run both before and after pg upgrade, the
-- first run will test that for columnar--10.2-3--10.2-4.sql script, and the
-- second run will test the same for citus_finish_pg_upgrade(), for the post
-- pg-upgrade scenario.
SELECT pg_class.oid INTO columnar_schema_members
FROM pg_class, pg_namespace
WHERE pg_namespace.oid=pg_class.relnamespace AND
      pg_namespace.nspname='columnar_internal';
SELECT refobjid INTO columnar_schema_members_pg_depend
FROM pg_depend
WHERE classid = 'pg_am'::regclass::oid AND
      objid = (select oid from pg_am where amname = 'columnar') AND
      objsubid = 0 AND
      refclassid = 'pg_class'::regclass::oid AND
      refobjsubid = 0 AND
      deptype = 'n';

-- ... , so this should be empty,
(TABLE columnar_schema_members EXCEPT TABLE columnar_schema_members_pg_depend)
UNION
(TABLE columnar_schema_members_pg_depend EXCEPT TABLE columnar_schema_members);

-- ... , and both columnar_schema_members_pg_depend & columnar_schema_members
-- should have 10 entries.
SELECT COUNT(*)=10 FROM columnar_schema_members_pg_depend;

DROP TABLE columnar_schema_members, columnar_schema_members_pg_depend;

-- Check the same for workers too.

SELECT run_command_on_workers(
$$
SELECT pg_class.oid INTO columnar_schema_members
FROM pg_class, pg_namespace
WHERE pg_namespace.oid=pg_class.relnamespace AND
	  pg_namespace.nspname='columnar_internal';
SELECT refobjid INTO columnar_schema_members_pg_depend
FROM pg_depend
WHERE classid = 'pg_am'::regclass::oid AND
	  objid = (select oid from pg_am where amname = 'columnar') AND
	  objsubid = 0 AND
	  refclassid = 'pg_class'::regclass::oid AND
	  refobjsubid = 0 AND
	  deptype = 'n';
$$
);

SELECT run_command_on_workers(
$$
(TABLE columnar_schema_members EXCEPT TABLE columnar_schema_members_pg_depend)
UNION
(TABLE columnar_schema_members_pg_depend EXCEPT TABLE columnar_schema_members);
$$
);

SELECT run_command_on_workers(
$$
SELECT COUNT(*)=10 FROM columnar_schema_members_pg_depend;
$$
);

SELECT run_command_on_workers(
$$
DROP TABLE columnar_schema_members, columnar_schema_members_pg_depend;
$$
);
