SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 12 AS server_version_above_eleven
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
SELECT * FROM columnar.options WHERE regclass = 'test_options_1'::regclass;
VACUUM VERBOSE test_options_1;
SELECT count(*), sum(a), sum(b) FROM test_options_1;

SELECT * FROM columnar.options WHERE regclass = 'test_options_2'::regclass;
VACUUM VERBOSE test_options_2;
SELECT count(*), sum(a), sum(b) FROM test_options_2;

